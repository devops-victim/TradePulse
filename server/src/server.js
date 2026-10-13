import { pool, config } from './db/connection.js';
import { IngestionWorker } from './workers/ingestionWorker.js';
import { EventHub } from './services/eventHub.js';
import { createApp } from './app.js';

const worker=new IngestionWorker();
const hub=new EventHub(worker);
let server;
let shuttingDown=false;
async function shutdown() {
  if (shuttingDown) return; shuttingDown=true;
  server?.close();
  await hub.stop();
  await worker.stop();
  server?.closeAllConnections();
  await pool.end();
}
try {
  await pool.query('SELECT job_id FROM ingestion_jobs LIMIT 0');
  await hub.start();
  server=createApp(worker,hub).listen(config.port,()=>console.log(`TradePulse backend: http://localhost:${config.port}`));
  server.on('error',async error=> { console.error('HTTP server failed:',error.code);await shutdown();process.exitCode=1; });
  await worker.recover();
  if (config.completionMode==='manual') console.log('External completion notifications are not configured. Source status checks require an explicit user request.');
  process.on('SIGINT',shutdown);
  process.on('SIGTERM',shutdown);
} catch (error) {
  console.error('Backend startup failed:',error.message);
  console.error('Check server/.env and run npm --prefix server run migrate.');
  await shutdown();process.exitCode=1;
}
