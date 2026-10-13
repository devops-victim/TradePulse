import express from 'express';
import { pool, config } from '../db/connection.js';
import { tradeRepository } from '../repositories/tradeRepository.js';
import { jobRepository } from '../repositories/jobRepository.js';
import { validateTrade } from '../clients/bseClient.js';
import { IngestionService, publicJob, validJobId } from '../services/ingestionService.js';
import { asyncRoute, HttpError } from '../middleware/errors.js';
import { createSSEController } from '../controllers/sseController.js';

export function createApiRoutes(worker, hub) {
  const router=express.Router();
  const service=new IngestionService(worker);
  const sse=createSSEController(hub);
  router.get('/events',asyncRoute(sse.subscribe));
  router.get('/health',asyncRoute(async (req,res)=> {
    try { await pool.query('SELECT 1'); }
    catch { return res.status(503).json({status:'unavailable',database:'PostgreSQL',error:'Database unavailable'}); }
    res.json({status:'online',database:'PostgreSQL',completionMode:config.completionMode,
      automaticCompletionConfigured:config.completionMode==='webhook',sseConnections:hub.clients.size,
      activeImportJob:publicJob((await jobRepository.active())[0] || null)});
  }));
  router.get('/trades',asyncRoute(async (req,res)=>res.json({success:true,...await tradeRepository.getPaginated({page:Number(req.query.page || 1),symbol:req.query.symbol})})));
  router.get('/trades/stats',asyncRoute(async (req,res)=>res.json({success:true,data:await tradeRepository.getStats()})));
  router.get('/trades/:tradeId',asyncRoute(async (req,res)=> {
    const trade=await tradeRepository.getByTradeId(req.params.tradeId);
    if (!trade) throw new HttpError(404,'Trade not found');
    res.json({success:true,data:trade});
  }));
  const initiate=asyncRoute(async (req,res)=>res.status(202).json(await service.initiate(req.body || {},req.headers['idempotency-key'])));
  router.post('/pulls',initiate);
  router.post('/bse/import',initiate);
  router.get('/pulls',asyncRoute(async (req,res)=>res.json({success:true,data:(await jobRepository.recent()).map(publicJob)})));
  router.get('/pulls/:jobId',asyncRoute(async (req,res)=> {
    const job=await jobRepository.get(validJobId(req.params.jobId));
    if (!job) throw new HttpError(404,'Job not found');
    res.json({success:true,data:publicJob(job)});
  }));
  router.post('/pulls/:jobId/refresh',asyncRoute(async (req,res)=>res.json(await service.refresh(req.params.jobId))));
  router.post('/pulls/:jobId/reconcile',asyncRoute(async (req,res)=>res.status(202).json(await service.reconcile(req.params.jobId,req.body?.externalJobId))));
  router.post('/pulls/:jobId/retry',asyncRoute(async (req,res)=> {
    const job=await jobRepository.retry(validJobId(req.params.jobId),config.externalIdempotency);
    worker.wake(job.job_id);res.status(202).json({success:true,data:publicJob(job)});
  }));
  router.post('/bse/notifications',asyncRoute(async (req,res)=>res.status(202).json(await service.notification(req))));
  router.get('/bse/jobs',asyncRoute(async (req,res)=>res.json({success:true,data:(await jobRepository.recent()).map(publicJob)})));
  router.get('/bse/jobs/active',asyncRoute(async (req,res)=>res.json({activeJob:publicJob((await jobRepository.active())[0] || null)})));
  router.get('/bse/getTrades',asyncRoute(async (req,res)=>res.json(await worker.source.getTrades(req.query))));
  router.get('/market',asyncRoute(async (req,res)=>res.json({success:true,data:await tradeRepository.getMarket()})));
  router.post('/trades',(req,res)=>res.status(410).json({success:false,error:'The trade ledger accepts external BSE data only. Use POST /api/orders for manual orders.'}));
  router.post('/orders',asyncRoute(async (req,res)=> {
    let order;
    try { order=validateTrade({...req.body,trade_id:'manual-order',timestamp:new Date().toISOString(),side:req.body.type || req.body.side}); }
    catch (error) { throw new HttpError(400,error.message); }
    if (!order.side) throw new HttpError(400,'Order side is required');
    res.status(201).json({success:true,data:await tradeRepository.createOrder(order)});
  }));
  router.get('/orders',asyncRoute(async (req,res)=>res.json({success:true,data:await tradeRepository.orders()})));
  router.post('/reset',asyncRoute(async (req,res)=> {
    if (config.nodeEnv==='production') throw new HttpError(403,'Ledger reset is disabled in production');
    await tradeRepository.clearAll();res.json({success:true,message:'Ledger cleared'});
  }));
  router.get('/users',asyncRoute(async (req,res)=>res.json({success:true,data:(await pool.query('SELECT id,name,email,balance,created_at FROM users ORDER BY id')).rows})));
  router.get('/worker/logs',asyncRoute(async (req,res)=>res.json({success:true,data:(await pool.query("SELECT * FROM (SELECT id,task_name,details,created_at::text AS created_at FROM worker_logs UNION ALL SELECT source_key::bigint AS id,record->>'task_name' AS task_name,record->'details' AS details,record->>'created_at' AS created_at FROM legacy_archive WHERE source_table='worker_logs') AS history ORDER BY created_at DESC,id DESC LIMIT 30")).rows})));
  router.post(['/worker/toggle','/worker/burst'],(req,res)=>res.status(410).json({success:false,error:'The simulated market worker was removed. Ingestion is driven by external completion events.'}));
  return router;
}
