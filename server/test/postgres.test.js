import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import dotenv from 'dotenv';
import express from 'express';
import {loadConfig} from '../src/config/env.js';
dotenv.config({path:new URL('../.env',import.meta.url),quiet:true});
const schema=`tp_test_${randomUUID().replaceAll('-','')}`;
const testUrl=new URL(process.env.TEST_DATABASE_URL || process.env.DATABASE_URL);
const testConfig=loadConfig({...process.env,DATABASE_URL:testUrl.toString(),NODE_ENV:'test'});
const admin=new pg.Pool({connectionString:testConfig.databaseUrl,ssl:testConfig.ssl});
await admin.query(`CREATE SCHEMA "${schema}"`);
testUrl.searchParams.set('options',`-c search_path=${schema}`);
process.env.DATABASE_URL=testUrl.toString();process.env.NODE_ENV='test';
process.env.BSE_COMPLETION_MODE='manual';process.env.BSE_SUPPORTS_IDEMPOTENCY='false';
process.env.BSE_WEBHOOK_SECRET='test-only-secret';
const {pool}=await import('../src/db/connection.js');
const {migrate}=await import('../scripts/migrate.js');
const {jobRepository}=await import('../src/repositories/jobRepository.js');
const {tradeRepository}=await import('../src/repositories/tradeRepository.js');
const {IngestionWorker}=await import('../src/workers/ingestionWorker.js');
const {EventHub}=await import('../src/services/eventHub.js');
const {createApp}=await import('../src/app.js');
const {BseClient}=await import('../src/clients/bseClient.js');
const {config}=await import('../src/db/connection.js');
const {importLegacy}=await import('../scripts/importLegacy.js');

class Source {
  constructor() {this.calls=0;this.pages=[];this.fail=false;this.delayReceipt=0;}
  async initiate() {this.calls++;if(this.receiptGate) await this.receiptGate;if(this.delayReceipt) await new Promise(r=>setTimeout(r,this.delayReceipt));if(this.fail) throw new Error('Source unavailable');return {id:randomUUID(),ready:false};}
  async status() {return {status:'completed'};}
  async results(id,cursor) {return this.pages[Number(cursor || 0)] || {data:[],nextCursor:null,total:0};}
}
const record=index=>({trade_id:`TEST-${String(index).padStart(5,'0')}`,client:'Client',symbol:'TCS',quantity:10,price:'123.45',timestamp:'2026-10-13T12:00:00.123456Z',side:null});
const awaitWorker=async worker=> {await Promise.allSettled([...worker.tasks.values()]); if(worker.tasks.size) await Promise.allSettled([...worker.tasks.values()]);};

test('PostgreSQL migration, ingestion, restart recovery, REST and durable SSE',async t=> {
  await migrate();await migrate();
  const source=new Source();const worker=new IngestionWorker(source);const hub=new EventHub(worker);
  await hub.start();
  const server=await new Promise(resolve=> {const s=createApp(worker,hub).listen(0,'127.0.0.1',()=>resolve(s));});
  const base=`http://127.0.0.1:${server.address().port}`;
  const api=async (path,method='GET',body)=> {const res=await fetch(base+path,{method,headers:{'Content-Type':'application/json'},body:body ? JSON.stringify(body):undefined});return {...await res.json(),httpStatus:res.status};};
  const reset=async ()=> {await awaitWorker(worker);await pool.query('TRUNCATE ingestion_batches,ingestion_staging,app_events,ingestion_jobs,trades,manual_orders CASCADE');};
  try {
    await t.test('migration is versioned and rerunnable; exact schema types',async ()=> {
      assert.equal((await pool.query('SELECT COUNT(*) FROM schema_migrations')).rows[0].count,'2');
      const columns=(await pool.query('SELECT column_name,data_type FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2',[schema,'trades'])).rows;
      assert.ok(columns.some(c=>c.column_name==='timestamp' && c.data_type==='timestamp with time zone'));
    });
    await t.test('acceptance does not wait for slow source; concurrent callers cannot duplicate a worker',async ()=> {
      let releaseReceipt;source.receiptGate=new Promise(resolve=>{releaseReceipt=resolve;});
      try {
      const result=await api('/api/pulls','POST',{delayMs:900000,idempotencyKey:'same'});
      // A withheld source receipt proves acceptance is independent of source
      // latency, including when PostgreSQL itself is accessed over the WAN.
      assert.equal(result.httpStatus,202);assert.equal((await jobRepository.get(result.jobId)).external_job_id,null);
      assert.equal((await api('/api/pulls','POST',{delayMs:900000,idempotencyKey:'same'})).jobId,result.jobId);
      assert.equal((await api('/api/pulls','POST',{delayMs:900000,idempotencyKey:'other'})).httpStatus,409);
      assert.equal((await api('/api/trades')).httpStatus,200);
      } finally {releaseReceipt();source.receiptGate=null;}
      await awaitWorker(worker);assert.equal(source.calls,1);
    });
    await t.test('waiting job survives new worker; no automatic status polling',async ()=> {
      const job=(await jobRepository.active())[0];
      const recovered=new IngestionWorker(source);await recovered.recover();await awaitWorker(recovered);
      assert.equal(source.calls,1);assert.equal((await jobRepository.get(job.job_id)).status,'processing');
      await recovered.stop();
    });
    await t.test('SSE rotates before 30 seconds and reconnects safely',{timeout:30000},async ()=> {
      const start=Date.now();const stream=await fetch(`${base}/api/events`);
      const reader=stream.body.getReader();let content='';
      while(true) {const {value,done}=await reader.read();if(done) break;content+=new TextDecoder().decode(value);}
      assert.match(content,/event: reconnect/);assert.ok(Date.now()-start>=24000 && Date.now()-start<29000);
      const abort=new AbortController();const reopened=await fetch(`${base}/api/events?cursor=0`,{signal:abort.signal});
      assert.equal(reopened.status,200);abort.abort();
      assert.equal((await api('/api/health')).status,'online');
    });
    await t.test('completion notifies multiple SSE clients after commit; dedupe and fixed page sizes',async ()=> {
      const job=(await jobRepository.active())[0];
      source.pages=[{data:Array.from({length:30},(_,i)=>record(i)),nextCursor:'1',total:66},
        {data:Array.from({length:30},(_,i)=>record(i+30)),nextCursor:'2',total:66},
        {data:[...Array.from({length:5},(_,i)=>record(i+60)),record(0)],nextCursor:null,total:66}];
      const streams=[];
      const observe=async ()=> {
        const abort=new AbortController();streams.push(abort);
        const res=await fetch(`${base}/api/events`,{signal:abort.signal});
        let resolveReady;const ready=new Promise(r=>resolveReady=r);
        const completion=(async()=> {
          const reader=res.body.getReader();let buffer='';
          while(true) {
            const {value,done}=await reader.read();if(done) throw new Error('Stream ended before completion');buffer+=new TextDecoder().decode(value);
            let boundary;
            while((boundary=buffer.indexOf('\n\n'))>=0) {
              const frame=buffer.slice(0,boundary);buffer=buffer.slice(boundary+2);
              if(frame.includes('event: connected')) resolveReady();
              if(frame.includes('event: import_completed')) {
                const payload=JSON.parse(frame.split('data: ')[1]);
                assert.equal(payload.affectedRecords,65);assert.ok(!payload.data);
                assert.equal((await tradeRepository.getStats()).totalTrades,65);return payload;
              }
            }
          }
        })();
        completion.catch(()=>{});await ready;return {completion,abort};
      };
      const clients=await Promise.all([observe(),observe()]);
      assert.equal((await api('/api/bse/notifications','POST',{externalJobId:job.external_job_id,status:'completed'})).httpStatus,403);
      const notified=await fetch(`${base}/api/bse/notifications`,{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer test-only-secret'},body:JSON.stringify({externalJobId:job.external_job_id,clientJobId:job.job_id,status:'completed'})});
      assert.equal(notified.status,202);
      const events=await Promise.all(clients.map(c=>c.completion));clients.forEach(c=>c.abort.abort());
      await awaitWorker(worker);
      assert.equal(events[0].eventId,events[1].eventId);
      assert.equal((await api('/api/trades?limit=10000')).data.length,30);
      assert.equal((await api('/api/trades?page=2')).data.length,30);
      assert.equal((await api('/api/trades?page=3')).data.length,5);
      assert.equal((await jobRepository.get(job.job_id)).duplicates_skipped,1);
      assert.equal((await api('/api/trades/TEST-00001')).data.trade_id,'TEST-00001');
      await jobRepository.notify(job.external_job_id,job.job_id);worker.wake(job.job_id);await awaitWorker(worker);
      assert.equal((await tradeRepository.getStats()).totalTrades,65);
      const replayAbort=new AbortController();
      const replay=await fetch(`${base}/api/events?cursor=${BigInt(events[0].eventId)-1n}`,{signal:replayAbort.signal});
      const chunk=await replay.body.getReader().read();assert.match(new TextDecoder().decode(chunk.value),/event: import_completed/);replayAbort.abort();
    });
    await t.test('failure leaves existing ledger readable; checkpoint resumes after restart',async ()=> {
      const checkpointId=randomUUID();
      await pool.query("INSERT INTO ingestion_jobs(job_id,external_job_id,status,idempotency_key,request,phase,notification_received,external_attempted) VALUES ($1,$2,'processing','checkpoint',$3,'fetching',true,true)",[checkpointId,randomUUID(),{delayMs:0,mode:'append',filters:{}}]);
      const job=await jobRepository.get(checkpointId);
      await jobRepository.stageBatch(await jobRepository.get(job.job_id),{data:[record(100)],nextCursor:'1',total:2});
      source.pages=[null,{data:[record(101)],nextCursor:null,total:2}];
      const recovered=new IngestionWorker(source);await recovered.recover();await awaitWorker(recovered);await awaitWorker(worker);await recovered.stop();
      assert.equal((await jobRepository.get(job.job_id)).status,'completed');
      assert.equal((await tradeRepository.getStats()).totalTrades,67);
      source.fail=true;
      const failed=await api('/api/pulls','POST',{delayMs:0,idempotencyKey:'failure'});assert.equal(failed.httpStatus,202);
      await awaitWorker(worker);assert.equal((await api(`/api/pulls/${failed.jobId}`)).data.status,'failed');
      assert.equal((await api('/api/trades')).data.length,30);
      assert.equal((await api(`/api/pulls/${failed.jobId}/retry`,'POST')).httpStatus,409);source.fail=false;
      const reconciled=await api(`/api/pulls/${failed.jobId}/reconcile`,'POST',{externalJobId:randomUUID()});
      assert.equal(reconciled.httpStatus,202);await awaitWorker(worker);
      assert.equal((await jobRepository.get(failed.jobId)).status,'completed');
    });
    await t.test('webhooks reject mismatched correlation and acknowledge duplicate source failures',async ()=> {
      // This subtest exercises receiver state transitions, not extraction.
      // Prevent LISTEN notifications from racing its deliberately assigned IDs.
      await awaitWorker(worker);worker.stopping=true;
      try {
      const existing=await jobRepository.latest();
      // An unknown client ID must not match an existing external job via OR.
      await assert.rejects(jobRepository.notify(existing.external_job_id,randomUUID()),error=>error.status===404);
      const {job}=await jobRepository.create({delayMs:0,mode:'append',filters:{}},'webhook-failure');
      const externalId=randomUUID();
      await jobRepository.attachExternal(job.job_id,externalId,false);
      await assert.rejects(jobRepository.notify(existing.external_job_id,job.job_id),error=>error.status===409);
      const send=body=>fetch(`${base}/api/bse/notifications`,{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer test-only-secret'},body:JSON.stringify(body)});
      const payload={externalJobId:externalId,clientJobId:job.job_id,status:'failed'};
      assert.equal((await send(payload)).status,202);
      const failed=await jobRepository.get(job.job_id);
      assert.equal(failed.status,'failed');assert.equal(failed.phase,'source_failed');assert.ok(failed.completed_at);
      const count=(await pool.query("SELECT COUNT(*) FROM app_events WHERE job_id=$1 AND event_type='import_failed'",[job.job_id])).rows[0].count;
      assert.equal((await send(payload)).status,202);
      assert.equal((await pool.query("SELECT COUNT(*) FROM app_events WHERE job_id=$1 AND event_type='import_failed'",[job.job_id])).rows[0].count,count);
      assert.equal((await send({...payload,clientJobId:''})).status,400);
      assert.equal((await send({...payload,status:'processing'})).status,400);
      const {job:early}=await jobRepository.create({delayMs:0,mode:'append',filters:{}},'early-callback');
      const earlyExternal=randomUUID();
      await jobRepository.notify(earlyExternal,early.job_id);
      const attached=await jobRepository.attachExternal(early.job_id,earlyExternal,false);
      assert.equal(attached.notification_received,true);
      await assert.rejects(jobRepository.attachExternal(early.job_id,randomUUID(),false),/does not match/);
      await jobRepository.fail(early.job_id,new Error('Isolated callback correlation fixture finished'));
      } finally {worker.stopping=false;}
    });
    await t.test('conflicting duplicate fails atomically; manual order never enters external ledger',async ()=> {
      const {job}=await jobRepository.create({delayMs:0,mode:'append',filters:{}},'conflict');
      await jobRepository.attachExternal(job.job_id,randomUUID(),true);
      source.pages=[{data:[{...record(0),price:'999.00'}],nextCursor:null,total:1}];
      worker.wake(job.job_id);await awaitWorker(worker);
      assert.equal((await jobRepository.get(job.job_id)).status,'failed');
      assert.equal((await tradeRepository.getByTradeId(record(0).trade_id)).price,'123.45');
      const saved=await api('/api/orders','POST',{client:'Manual',symbol:'TCS',quantity:1,price:'100.00',type:'BUY'});
      assert.equal(saved.httpStatus,201);assert.equal((await tradeRepository.getStats()).totalTrades,67);
    });
    await t.test('legacy export imports losslessly and is idempotent',{skip:!existsSync(new URL('../backups/legacy-export.json',import.meta.url))},async ()=> {
      await reset();
      const report=await importLegacy(new URL('../backups/legacy-export.json',import.meta.url),{writeReport:false});
      assert.equal(report.exportedTrades,10000);assert.equal(report.inserted,10000);
      const repeated=await importLegacy(new URL('../backups/legacy-export.json',import.meta.url),{writeReport:false});assert.equal(repeated.inserted,0);
      assert.equal((await pool.query('SELECT COUNT(*) FROM legacy_archive')).rows[0].count,'10031');
      assert.equal((await api('/api/worker/logs')).data.length,20);
      assert.equal((await pool.query("SELECT COUNT(*) FROM trades WHERE source='legacy_import'")).rows[0].count,'10000');
    });
    await t.test('HTTP source callback drives bounded ingestion without manual refresh or status polling',async ()=> {
      const originalSource=worker.source;
      const before=(await tradeRepository.getStats()).totalTrades;
      const externalId=randomUUID();let registration,statusCalls=0,resultCalls=0,delivery;
      const exchange=await new Promise(resolve=> {
        // Test-only transport fixture; never part of the application runtime.
        const app=express();app.use(express.json());
        app.post('/extractions',(req,res)=> {registration=req.body;res.status(202).json({jobId:externalId,status:'pending'});});
        app.get('/extractions/:id',(req,res)=> {statusCalls++;res.json({data:{status:'completed',total_records:2}});});
        app.get('/extractions/:id/results',(req,res)=> {resultCalls++;const second=req.query.cursor==='next';res.json({data:[record(second ? 201 : 200)],nextCursor:second ? null : 'next'});});
        const s=app.listen(0,'127.0.0.1',()=>resolve(s));
      });
      try {
        worker.source=new BseClient({...config,bseBaseUrl:`http://127.0.0.1:${exchange.address().port}`,completionMode:'webhook',callbackUrl:`${base}/api/bse/notifications`,batchSize:1});
        const accepted=await api('/api/pulls','POST',{delayMs:31000,idempotencyKey:'http-webhook'});
        assert.equal(accepted.httpStatus,202);await awaitWorker(worker);
        assert.equal(statusCalls,0);assert.equal(resultCalls,0);
        assert.equal(registration.clientJobId,accepted.jobId);assert.equal(registration.delay,31000);
        delivery=await fetch(registration.callbackUrl,{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer test-only-secret'},body:JSON.stringify({externalJobId:externalId,clientJobId:registration.clientJobId,status:'completed'})});
        assert.equal(delivery.status,202);await delivery.body.cancel();await awaitWorker(worker);
        const completed=await jobRepository.get(accepted.jobId);
        assert.equal(completed.status,'completed',completed.error_message);
        assert.equal(completed.records_received,2);assert.equal(completed.records_inserted,2);
        assert.equal(statusCalls,1);assert.equal(resultCalls,2);
        assert.equal((await tradeRepository.getStats()).totalTrades,before+2);
        const duplicate=await fetch(registration.callbackUrl,{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer test-only-secret'},body:JSON.stringify({externalJobId:externalId,clientJobId:registration.clientJobId,status:'completed'})});
        assert.equal(duplicate.status,202);await duplicate.body.cancel();await awaitWorker(worker);
        assert.equal(statusCalls,1);assert.equal(resultCalls,2);
        assert.equal((await pool.query("SELECT COUNT(*) FROM app_events WHERE job_id=$1 AND event_type='import_completed'",[accepted.jobId])).rows[0].count,'1');
      } finally {worker.source=originalSource;exchange.closeAllConnections();await new Promise(r=>exchange.close(r));}
    });
    if (process.env.TEST_LIVE_BSE==='true') await t.test('real independently hosted BSE extraction and bounded retrieval',async ()=> {
      await reset();
      const originalSource=worker.source;worker.source=new BseClient(config);const live=worker;
      const {job}=await jobRepository.create({delayMs:0,mode:'append',filters:{}},'live-source');
      live.wake(job.job_id);await awaitWorker(live);
      const accepted=await jobRepository.get(job.job_id);assert.ok(accepted.external_job_id);
      // One explicit readiness request, never an automatic polling loop.
      await new Promise(resolve=>setTimeout(resolve,100));
      const status=await live.source.status(accepted.external_job_id);assert.equal(status.status,'completed');
      await jobRepository.notify(accepted.external_job_id,job.job_id);live.wake(job.job_id);await awaitWorker(live);worker.source=originalSource;
      const finished=await jobRepository.get(job.job_id);assert.equal(finished.status,'completed',finished.error_message);
      assert.equal((await tradeRepository.getStats()).totalTrades,status.total_records);
      assert.ok(status.total_records>=1000);
    });
  } finally {
    await hub.stop();await worker.stop();server.closeAllConnections();await new Promise(r=>server.close(r));
    await pool.end();
    assert.match(schema,/^tp_test_[a-f0-9]{32}$/);
    await admin.query(`DROP SCHEMA "${schema}" CASCADE`);await admin.end();
  }
});
