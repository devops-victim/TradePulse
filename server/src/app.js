import express from 'express';
import cors from 'cors';
import { config } from './db/connection.js';
import { createApiRoutes } from './routes/apiRoutes.js';
import { requestDeadline } from './requestDeadline.js';
import { errorHandler } from './middleware/errors.js';
export function createApp(worker,hub) {
  const app=express();
  app.use(cors({origin:config.corsOrigin,methods:['GET','POST','OPTIONS'],allowedHeaders:['Content-Type','Authorization','Idempotency-Key','Last-Event-ID']}));
  app.use(requestDeadline());
  app.use(express.json({limit:'64kb'}));
  app.use('/api',createApiRoutes(worker,hub));
  app.get('/',(req,res)=>res.json({name:'TradePulse',database:'PostgreSQL',trades:'/api/trades',pulls:'/api/pulls',events:'/api/events'}));
  app.use((req,res)=>res.status(404).json({success:false,error:'Route not found'}));
  app.use(errorHandler);
  return app;
}
