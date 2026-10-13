import { randomUUID, timingSafeEqual } from 'node:crypto';
import { config } from '../db/connection.js';
import { jobRepository } from '../repositories/jobRepository.js';
import { validateTimestamp,timestampMicros } from '../clients/bseClient.js';
import { HttpError } from '../middleware/errors.js';

export function publicJob(job) {
  if (!job) return null;
  return { jobId:job.job_id,externalJobId:job.external_job_id,status:job.status,phase:job.phase,
    startedAt:job.started_at,updatedAt:job.updated_at,completedAt:job.completed_at,recordsReceived:job.records_received,
    insertedCount:job.records_inserted,duplicatesSkipped:job.duplicates_skipped,error:job.error_message,
    delayMs:job.request.delayMs || 0,completionMode:config.completionMode };
}
export function validJobId(id) {
  if (typeof id !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) throw new HttpError(400,'Invalid job ID');
  return id;
}
export class IngestionService {
  constructor(worker) { this.worker=worker; }
  async initiate(body, headerKey) {
    const delayMs=Number(body.delayMs ?? 900000);
    const mode=body.mode || 'append';
    if (!Number.isInteger(delayMs) || delayMs<0 || delayMs>900000) throw new HttpError(400,'delayMs must be an integer from 0 to 900000');
    if (!['append','replace'].includes(mode)) throw new HttpError(400,'Invalid import mode');
    if (body.count !== undefined || body.seed !== undefined) throw new HttpError(400,'The external extraction contract does not accept count or seed');
    const filters={};
    if (body.symbol) {
      if (typeof body.symbol !== 'string' || !/^[A-Z0-9.-]{1,20}$/.test(body.symbol)) throw new HttpError(400,'Invalid symbol');
      filters.symbol=body.symbol;
    }
    for (const key of ['from','to']) if (body[key] !== undefined) {
      try {validateTimestamp(body[key]);} catch {throw new HttpError(400,`Invalid ${key} timestamp`);}
      filters[key]=body[key];
    }
    if (filters.from && filters.to && timestampMicros(filters.from)>timestampMicros(filters.to)) throw new HttpError(400,'from must precede to');
    const key=headerKey || body.idempotencyKey || randomUUID();
    if (typeof key !== 'string' || !key || key.length>128) throw new HttpError(400,'Invalid idempotency key');
    const result=await jobRepository.create({delayMs,mode,filters},key);
    // Do not await external network work in the accepting HTTP request.
    this.worker.wake(result.job.job_id);
    return { success:true,jobId:result.job.job_id,idempotencyKey:key,status:result.job.status,
      job:publicJob(result.job),message:config.completionMode === 'webhook' ? 'Pull accepted; awaiting external completion notification' : 'Pull accepted. External automatic completion is not configured; check source status explicitly.' };
  }
  async refresh(id) {
    const job=await jobRepository.get(validJobId(id));
    if (!job) throw new HttpError(404,'Job not found');
    if (!job.external_job_id) throw new HttpError(409,'External extraction ID is not available yet');
    const source=await this.worker.source.status(job.external_job_id);
    const status=String(source.status).toLowerCase();
    if (['completed','ready'].includes(status) && !['completed','failed'].includes(job.status)) {
      await jobRepository.notify(job.external_job_id,job.job_id); this.worker.wake(job.job_id);
    } else if (status==='failed') await jobRepository.fail(job.job_id,new Error(typeof source.error === 'string' ? source.error : 'External extraction failed'));
    return { success:true,sourceStatus:status,job:publicJob(await jobRepository.get(id)) };
  }
  async notification(req) {
    const expected=Buffer.from(`Bearer ${config.webhookSecret}`),actual=Buffer.from(req.headers.authorization || '');
    if (!config.webhookSecret || actual.length!==expected.length || !timingSafeEqual(actual,expected)) throw new HttpError(403,'Invalid webhook credentials');
    if (!req.body || typeof req.body !== 'object' || Array.isArray(req.body)) throw new HttpError(400,'Notification must be a JSON object');
    const { externalJobId, clientJobId, status }=req.body;
    if (typeof externalJobId!=='string' || !externalJobId || externalJobId.length>100) throw new HttpError(400,'externalJobId is required');
    if (clientJobId !== undefined && clientJobId !== null) validJobId(clientJobId);
    if (!['completed','failed'].includes(status)) throw new HttpError(400,'Unsupported extraction notification status');
    const job=await jobRepository.notify(externalJobId,clientJobId,status);
    if (!['completed','failed'].includes(job.status)) this.worker.wake(job.job_id);
    return { success:true,jobId:job.job_id };
  }
  async reconcile(id,externalId) {
    validJobId(id);
    if (typeof externalId!=='string' || !externalId || externalId.length>100) throw new HttpError(400,'externalJobId is required');
    const metadata=await this.worker.source.status(externalId);
    if (!['pending','processing','completed','ready'].includes(String(metadata.status).toLowerCase())) throw new HttpError(409,'External job cannot be resumed');
    const job=await jobRepository.reconcile(id,externalId,metadata);
    this.worker.wake(id);
    return {success:true,data:publicJob(job)};
  }
}
