import { pool, config } from '../db/connection.js';
import { jobRepository } from '../repositories/jobRepository.js';
import { BseClient } from '../clients/bseClient.js';

export class IngestionWorker {
  constructor(source = new BseClient(config)) { this.source=source; this.tasks=new Map(); this.stopping=false; this.rerun=new Set(); }
  wake(id) {
    if (this.stopping) return;
    if (this.tasks.has(id)) { this.rerun.add(id); return; }
    const task = this.run(id).catch(error => console.error('Ingestion worker error:',error.code || error.message)).finally(() => { this.tasks.delete(id); if (this.rerun.delete(id)) this.wake(id); });
    this.tasks.set(id,task);
  }
  async run(id) {
    const lock = await pool.connect();
    let locked=false,lockError;
    const onLockError=error=> { lockError=error; };
    lock.on('error',onLockError);
    try {
      locked=(await lock.query('SELECT pg_try_advisory_lock(hashtextextended($1,0)) AS acquired',[`ingestion:${id}`])).rows[0].acquired;
      if (!locked) return;
      let job=await jobRepository.get(id);
      if (!job || ['completed','failed'].includes(job.status)) return;
      if (!job.external_job_id) {
        if (job.external_attempted && !config.externalIdempotency) throw new Error('Interrupted source initiation: reconcile external job before retry; source idempotency is not confirmed');
        await jobRepository.markRequesting(id);
        const receipt=await this.source.initiate(job);
        if (lockError) throw lockError;
        job=await jobRepository.attachExternal(id,receipt.id,receipt.ready);
      }
      job=await jobRepository.get(id);
      if (['completed','failed'].includes(job.status) || !job.notification_received) return;
      if (job.expected_records===null) {
        const metadata=await this.source.status(job.external_job_id);
        if (!['completed','ready'].includes(String(metadata.status).toLowerCase())) throw new Error('Source notification arrived before results were ready');
        if (metadata.total_records !== undefined) job=await jobRepository.setExpected(id,metadata.total_records);
      }
      while (!this.stopping && job.phase !== 'publishing') {
        const batch=await this.source.results(job.external_job_id,job.next_cursor);
        if (lockError) throw lockError;
        job=await jobRepository.stageBatch(job,batch);
      }
      if (!this.stopping && !lockError) await jobRepository.publish(id);
    } catch (error) {
      if (!lockError) await jobRepository.fail(id,error).catch(failure => console.error('Could not persist ingestion failure:',failure.code || failure.name));
    } finally {
      if (locked) await lock.query('SELECT pg_advisory_unlock(hashtextextended($1,0))',[`ingestion:${id}`]).catch(()=>{});
      lock.removeListener('error',onLockError);
      lock.release(Boolean(lockError));
    }
  }
  async recover() { for (const job of await jobRepository.active()) this.wake(job.job_id); }
  async stop() { this.stopping=true; await Promise.allSettled([...this.tasks.values()]); }
}
