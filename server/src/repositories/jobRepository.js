import { randomUUID } from 'node:crypto';
import { pool, transaction } from '../db/connection.js';
import { recordEvent } from './eventRepository.js';
import { HttpError } from '../middleware/errors.js';

const fields = 'trade_id varchar(40),client varchar(100),symbol varchar(20),quantity integer,price numeric(14,2),timestamp timestamptz,side varchar(4)';
const equality = (a,b) => `(${a}.client,${a}.symbol,${a}.quantity,${a}.price,${a}.timestamp,${a}.side) IS DISTINCT FROM (${b}.client,${b}.symbol,${b}.quantity,${b}.price,${b}.timestamp,${b}.side)`;
export const jobRepository = {
  async create(request, key) {
    return transaction(async client => {
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended('tradepulse-ingestion-control',0))");
      const previous = (await client.query('SELECT * FROM ingestion_jobs WHERE idempotency_key=$1',[key])).rows[0];
      if (previous) {
        if (JSON.stringify(previous.request) !== JSON.stringify(request)) {
          // Compare JSON structurally; PostgreSQL JSONB normalizes key ordering.
          const same = (await client.query('SELECT request=$2::jsonb AS same FROM ingestion_jobs WHERE job_id=$1',[previous.job_id,request])).rows[0].same;
          if (!same) throw new HttpError(409,'Idempotency key was already used for a different request');
        }
        return { job:previous,created:false };
      }
      const active = (await client.query("SELECT job_id FROM ingestion_jobs WHERE status IN ('pending','processing') LIMIT 1")).rows[0];
      if (active) throw new HttpError(409,`Ingestion already active: ${active.job_id}`);
      const { rows: [job] } = await client.query('INSERT INTO ingestion_jobs(job_id,idempotency_key,request) VALUES ($1,$2,$3) RETURNING *',[randomUUID(),key,request]);
      await recordEvent(client,'import_started',job.job_id,0,{ status:'pending' });
      return { job,created:true };
    });
  },
  async get(id, client = pool) { return (await client.query('SELECT * FROM ingestion_jobs WHERE job_id=$1',[id])).rows[0]; },
  async latest(client = pool) { return (await client.query('SELECT * FROM ingestion_jobs ORDER BY started_at DESC,job_id DESC LIMIT 1')).rows[0] || null; },
  async recent() { return (await pool.query('SELECT * FROM ingestion_jobs ORDER BY started_at DESC LIMIT 10')).rows; },
  async active() { return (await pool.query("SELECT * FROM ingestion_jobs WHERE status IN ('pending','processing') ORDER BY started_at")).rows; },
  async markRequesting(id) {
    await pool.query("UPDATE ingestion_jobs SET status='processing',phase='requesting',external_attempted=true,attempts=attempts+1,updated_at=NOW() WHERE job_id=$1",[id]);
  },
  async attachExternal(id, externalId, ready) {
    return transaction(async client => {
      const existing=(await client.query('SELECT * FROM ingestion_jobs WHERE job_id=$1 FOR UPDATE',[id])).rows[0];
      if (existing.external_job_id && existing.external_job_id!==externalId) throw new Error('Source receipt does not match completion notification');
      if (['failed','completed'].includes(existing.status)) return existing;
      const { rows:[job] } = await client.query("UPDATE ingestion_jobs SET external_job_id=$2,status='processing',phase='waiting',notification_received=notification_received OR $3,updated_at=NOW() WHERE job_id=$1 RETURNING *",[id,externalId,ready]);
      await recordEvent(client,'import_progress',id,0,{ status:job.status,phase:job.phase });
      return job;
    });
  },
  async setExpected(id,total) {
    if (!Number.isSafeInteger(total) || total<0 || total>2147483647) throw new Error('Invalid external total_records');
    const { rows:[job] }=await pool.query('UPDATE ingestion_jobs SET expected_records=$2 WHERE job_id=$1 AND (expected_records IS NULL OR expected_records=$2) RETURNING *',[id,total]);
    if (!job) throw new Error('External result total changed');
    return job;
  },
  async notify(externalId, internalId, status='completed') {
    return transaction(async client => {
      const job = (await client.query('SELECT * FROM ingestion_jobs WHERE external_job_id=$1 OR ($2::uuid IS NOT NULL AND job_id=$2) FOR UPDATE',[externalId,internalId || null])).rows[0];
      if (!job) throw new HttpError(404,'Unknown extraction job');
      if (job.external_job_id && job.external_job_id !== externalId) throw new HttpError(409,'External job ID does not match');
      if (job.status === 'completed') return job;
      if (job.status === 'failed') throw new HttpError(409,'Retry the failed ingestion explicitly before redelivery');
      if (job.notification_received && status==='completed') return job;
      const failed=status==='failed';
      const { rows:[updated] } = await client.query("UPDATE ingestion_jobs SET external_job_id=COALESCE(external_job_id,$2),notification_received=NOT $3,status=CASE WHEN $3 THEN 'failed' ELSE status END,error_message=CASE WHEN $3 THEN 'External extraction failed' ELSE error_message END,updated_at=NOW() WHERE job_id=$1 RETURNING *",[job.job_id,externalId,failed]);
      await recordEvent(client,failed ? 'import_failed' : 'ingestion_ready',job.job_id,0,failed ? {status:'failed',error:'External extraction failed'} : {});
      return updated;
    });
  },
  async stageBatch(job, batch) {
    return transaction(async client => {
      const current = (await client.query('SELECT * FROM ingestion_jobs WHERE job_id=$1 FOR UPDATE',[job.job_id])).rows[0];
      if (!['pending','processing'].includes(current.status)) throw new Error('Job is no longer active');
      if (current.next_batch !== job.next_batch) return current;
      if (batch.nextCursor !== null && (await client.query('SELECT 1 FROM ingestion_batches WHERE job_id=$1 AND cursor_in=$2',[job.job_id,batch.nextCursor])).rowCount) throw new Error('External result cursor cycle detected');
      if (batch.total !== null && current.expected_records !== null && current.expected_records !== batch.total) throw new Error('External result total changed');
      const encoded = JSON.stringify(batch.data);
      const conflict = await client.query(`WITH incoming AS (SELECT * FROM jsonb_to_recordset($2::jsonb) AS x(${fields})) SELECT 1 FROM ingestion_staging s JOIN incoming i USING(trade_id) WHERE s.job_id=$1 AND ${equality('s','i')} LIMIT 1`,[job.job_id,encoded]);
      if (conflict.rowCount) throw new Error('Conflicting payload for duplicate trade_id');
      // Detect conflicting duplicates within this batch before INSERT .. ON CONFLICT.
      const seen = new Map();
      for (const trade of batch.data) {
        const value = JSON.stringify({ ...trade,price:Number(trade.price).toFixed(2) });
        if (seen.has(trade.trade_id) && seen.get(trade.trade_id) !== value) throw new Error('Conflicting duplicate in BSE batch');
        seen.set(trade.trade_id,value);
      }
      await client.query(`INSERT INTO ingestion_staging(job_id,trade_id,client,symbol,quantity,price,timestamp,side) SELECT $1,x.* FROM jsonb_to_recordset($2::jsonb) AS x(${fields}) ON CONFLICT(job_id,trade_id) DO NOTHING`,[job.job_id,encoded]);
      await client.query('INSERT INTO ingestion_batches(job_id,batch_number,cursor_in,cursor_out,records_received) VALUES ($1,$2,$3,$4,$5)',[job.job_id,job.next_batch,job.next_cursor,batch.nextCursor,batch.data.length]);
      const { rows:[updated] } = await client.query("UPDATE ingestion_jobs SET phase=$2,next_cursor=$3,next_batch=next_batch+1,records_received=records_received+$4,expected_records=COALESCE(expected_records,$5),updated_at=NOW() WHERE job_id=$1 RETURNING *",[job.job_id,batch.nextCursor === null ? 'publishing' : 'fetching',batch.nextCursor,batch.data.length,batch.total]);
      await recordEvent(client,'import_progress',job.job_id,batch.data.length,{ status:'processing',phase:updated.phase,recordsReceived:updated.records_received });
      return updated;
    });
  },
  async publish(id) {
    return transaction(async client => {
      const job = (await client.query('SELECT * FROM ingestion_jobs WHERE job_id=$1 FOR UPDATE',[id])).rows[0];
      if (job.status === 'completed') return job;
      if (job.status === 'failed') throw new Error('Cannot publish a failed job');
      if (job.expected_records !== null && job.records_received !== job.expected_records) throw new Error('Incomplete BSE extraction result');
      const conflicts = await client.query(`SELECT 1 FROM trades t JOIN ingestion_staging s USING(trade_id) WHERE s.job_id=$1 AND ${equality('t','s')} LIMIT 1`,[id]);
      if (conflicts.rowCount) throw new Error('Existing trade_id has conflicting external data');
      if (job.request.mode === 'replace') await client.query('DELETE FROM trades');
      const inserted = await client.query('INSERT INTO trades(trade_id,client,symbol,quantity,price,timestamp,side) SELECT trade_id,client,symbol,quantity,price,timestamp,side FROM ingestion_staging WHERE job_id=$1 ON CONFLICT(trade_id) DO NOTHING',[id]);
      const { rows:[updated] } = await client.query("UPDATE ingestion_jobs SET status='completed',phase='completed',records_inserted=$2,duplicates_skipped=records_received-$2,error_message=NULL,completed_at=NOW(),updated_at=NOW() WHERE job_id=$1 RETURNING *",[id,inserted.rowCount]);
      await client.query('DELETE FROM ingestion_staging WHERE job_id=$1',[id]);
      await recordEvent(client,'import_completed',id,inserted.rowCount,{ status:'completed',insertedCount:inserted.rowCount,duplicatesSkipped:updated.duplicates_skipped });
      return updated;
    });
  },
  async fail(id,error) {
    return transaction(async client => {
      const { rows:[job] } = await client.query("UPDATE ingestion_jobs SET status='failed',error_message=$2,updated_at=NOW() WHERE job_id=$1 AND status NOT IN ('completed','failed') RETURNING *",[id,String(error.message).slice(0,1000)]);
      if (job) await recordEvent(client,'import_failed',id,0,{ status:'failed',error:job.error_message });
      return job;
    });
  },
  async retry(id, allowInitiation) {
    return transaction(async client => {
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended('tradepulse-ingestion-control',0))");
      const job = await this.get(id,client);
      if (!job) throw new HttpError(404,'Job not found');
      if (job.status !== 'failed') throw new HttpError(409,'Only failed jobs can be retried');
      if (!job.external_job_id && job.external_attempted && !allowInitiation) throw new HttpError(409,'Source acceptance is uncertain; external idempotency or operator reconciliation is required');
      if ((await client.query("SELECT 1 FROM ingestion_jobs WHERE status IN ('pending','processing')")).rowCount) throw new HttpError(409,'Another ingestion is active');
      const { rows:[updated] } = await client.query("UPDATE ingestion_jobs SET status='pending',error_message=NULL,updated_at=NOW() WHERE job_id=$1 RETURNING *",[id]);
      await recordEvent(client,'import_started',id,0,{ status:'pending' });
      return updated;
    });
  },
  async reconcile(id, externalId, metadata) {
    return transaction(async client => {
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended('tradepulse-ingestion-control',0))");
      const job=await this.get(id,client);
      if (!job) throw new HttpError(404,'Job not found');
      if (job.status!=='failed' || job.external_job_id || !job.external_attempted) throw new HttpError(409,'Only an uncertain failed initiation can be reconciled');
      if ((await client.query("SELECT 1 FROM ingestion_jobs WHERE status IN ('pending','processing')")).rowCount) throw new HttpError(409,'Another ingestion is active');
      if (metadata.filters) {
        const same=(await client.query('SELECT $1::jsonb=$2::jsonb AS same',[metadata.filters,job.request.filters || {}])).rows[0].same;
        if (!same) throw new HttpError(409,'External extraction filters do not match this pull');
      }
      if (metadata.delay_ms!==undefined && metadata.delay_ms!==job.request.delayMs) throw new HttpError(409,'External extraction delay does not match this pull');
      const ready=['completed','ready'].includes(String(metadata.status).toLowerCase());
      const {rows:[updated]}=await client.query("UPDATE ingestion_jobs SET external_job_id=$2,status='processing',phase='waiting',notification_received=$3,error_message=NULL,updated_at=NOW() WHERE job_id=$1 RETURNING *",[id,externalId,ready]);
      await recordEvent(client,ready ? 'ingestion_ready' : 'import_started',id,0,{status:'processing',phase:'waiting'});
      return updated;
    });
  },
};
