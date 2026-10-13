import pg from 'pg';
import { pool, config, transaction } from '../db/connection.js';
import { eventRepository, eventPayload } from '../repositories/eventRepository.js';
import { jobRepository } from '../repositories/jobRepository.js';
import { tradeRepository } from '../repositories/tradeRepository.js';
import { publicJob } from './ingestionService.js';
import { HttpError } from '../middleware/errors.js';

export class EventHub {
  constructor(worker) { this.worker=worker; this.clients=new Set(); this.stopped=false; this.listener=null; this.retryTimer=null; }
  async start() {
    const listener=new pg.Client({ connectionString:config.databaseUrl,ssl:config.ssl,connectionTimeoutMillis:5000,application_name:'tradepulse-events' });
    this.listener=listener;
    listener.on('error', () => this.reconnect(listener));
    listener.on('end', () => { if (!this.stopped && this.listener === listener) this.reconnect(listener); });
    listener.on('notification', message => {
      this.onNotification(message.payload).catch(error => console.error('Event delivery failed:',error.code || error.name));
    });
    try { await listener.connect(); this.schema=(await listener.query('SELECT current_schema() AS name')).rows[0].name; await listener.query('LISTEN tradepulse_events'); }
    catch (error) { await listener?.end().catch(()=>{}); throw error; }
  }
  reconnect(listener) {
    if (this.stopped || this.listener !== listener) return;
    this.listener=null;
    listener?.end().catch(()=>{});
    for (const state of this.clients) state.res.end();
    clearTimeout(this.retryTimer);
    this.retryTimer=setTimeout(async () => {
      try { await this.start(); await this.worker.recover(); }
      catch { this.reconnect(this.listener); }
    },1000);
  }
  async onNotification(payload) {
    const notification=JSON.parse(payload);
    if (notification.schema!==this.schema) return;
    const id=notification.eventId;
    const event=(await pool.query('SELECT * FROM app_events WHERE event_id=$1',[id])).rows[0];
    if (event && ['ingestion_ready','import_started'].includes(event.event_type)) this.worker.wake(event.job_id);
    for (const state of this.clients) this.drain(state);
  }
  frame(state,type,payload,id) {
    if (state.closed) return false;
    const accepted=state.res.write(`${id ? `id: ${id}\n` : ''}event: ${type}\ndata: ${JSON.stringify(payload)}\n\n`);
    if (!accepted) { state.res.end(); return false; }
    return true;
  }
  async subscribe(req,res) {
    const cursor=req.headers['last-event-id'] || req.query.cursor;
    if (cursor !== undefined && (typeof cursor !== 'string' || !/^\d{1,19}$/.test(cursor) || BigInt(cursor)>9223372036854775807n)) throw new HttpError(400,'Invalid event cursor');
    const snapshot=await transaction(async client => ({
      eventId:await eventRepository.latest(client), importJob:publicJob(await jobRepository.latest(client)), stats:await tradeRepository.getStats(client), completionMode:config.completionMode,
    }),'ISOLATION LEVEL REPEATABLE READ READ ONLY');
    res.setHeader('Content-Type','text/event-stream');
    res.setHeader('Cache-Control','no-cache, no-transform');
    res.setHeader('X-Accel-Buffering','no');
    res.flushHeaders();
    const hasCursor=cursor && BigInt(cursor)>0n;
    const state={res,cursor:hasCursor ? cursor : snapshot.eventId,ready:false,draining:false,dirty:false,closed:false};
    this.clients.add(state);
    const heartbeat=setInterval(()=> { if (!state.closed) res.write(': heartbeat\n\n'); },10000);
    const cutoff=setTimeout(()=> { this.frame(state,'reconnect',{}); res.end(); },25000);
    res.once('close',()=> { state.closed=true; clearInterval(heartbeat); clearTimeout(cutoff); this.clients.delete(state); });
    try {
      if (cursor && BigInt(cursor)>BigInt(snapshot.eventId)) state.cursor='0';
      if (hasCursor) {
        while (!state.closed) {
          const events=await eventRepository.after(state.cursor,snapshot.eventId);
          if (!events.length) break;
          for (const event of events) {
            if (!this.frame(state,event.event_type,eventPayload(event),event.event_id)) break;
            state.cursor=event.event_id;
          }
        }
      }
      this.frame(state,'connected',snapshot,snapshot.eventId);
      state.cursor=snapshot.eventId; state.ready=true;
      // Catch events committed between snapshot and subscription, without polling.
      this.drain(state);
    } catch { res.end(); }
  }
  drain(state) {
    state.dirty=true;
    if (!state.ready || state.draining || state.closed) return;
    state.draining=true;
    (async()=> {
      while (state.dirty && !state.closed) {
        state.dirty=false;
        while (!state.closed) {
          const events=await eventRepository.after(state.cursor);
          if (!events.length) break;
          for (const event of events) {
            if (!this.frame(state,event.event_type,eventPayload(event),event.event_id)) break;
            state.cursor=event.event_id;
          }
        }
      }
    })().catch(()=>state.res.end()).finally(()=> { state.draining=false; if (state.dirty && !state.closed) this.drain(state); });
  }
  async stop() {
    this.stopped=true; clearTimeout(this.retryTimer);
    for (const state of this.clients) state.res.end();
    await this.listener?.end().catch(()=>{});
  }
}
