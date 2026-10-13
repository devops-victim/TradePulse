import test from 'node:test';
import assert from 'node:assert/strict';
import { BseClient,validateTrade } from '../src/clients/bseClient.js';
import { loadConfig } from '../src/config/env.js';
const sample={trade_id:'BSE-001',client:'Client',symbol:'TCS',quantity:5,price:'1234.56',timestamp:'2026-10-13T00:00:00.123456Z'};
test('trade validation preserves exact price and microsecond timestamp',()=> {
  assert.equal(validateTrade(sample).price,'1234.56');
  assert.equal(validateTrade(sample).timestamp,sample.timestamp);
  for (const change of [{quantity:1.5},{quantity:0},{price:'1.001'},{price:'1000000000000'},{timestamp:'2026-10-13'},{trade_id:'x'.repeat(41)}]) assert.throws(()=>validateTrade({...sample,...change}));
});
test('configuration enforces dedicated database and verified cloud TLS',()=> {
  const local=loadConfig({DATABASE_URL:'postgresql://u:p@localhost/tradepulse'});
  assert.equal(local.port,4000);assert.equal(local.ssl,false);
  const cloud=loadConfig({DATABASE_URL:'postgresql://u:p@db.example/tradepulse'});
  assert.equal(cloud.ssl.rejectUnauthorized,true);
  const override=loadConfig({DATABASE_URL:'postgresql://u:p@db.example/tradepulse?ssl=no-verify'});
  assert.equal(override.ssl.rejectUnauthorized,true);assert.equal(new URL(override.databaseUrl).searchParams.has('ssl'),false);
  assert.throws(()=>loadConfig({DATABASE_URL:'postgresql://u:p@db.example/tradepulse?sslmode=require'}));
  assert.throws(()=>loadConfig({DATABASE_URL:'postgresql://u:p@localhost/other'}));
  assert.throws(()=>loadConfig({DATABASE_URL:'postgresql://u:p@localhost/tradepulse',BSE_COMPLETION_MODE:'webhook'}));
});
test('external adapter follows the independently hosted extraction contract',async ()=> {
  const client=new BseClient({bseBaseUrl:'http://localhost:4001',completionMode:'manual',batchSize:500});
  let request;
  client.request=async (path,options)=> {request={path,options};return {jobId:'external-1',status:'pending'};};
  const receipt=await client.initiate({job_id:'internal',idempotency_key:'key',request:{delayMs:900000,mode:'append',filters:{symbol:'TCS'}}});
  assert.equal(receipt.id,'external-1');assert.deepEqual(request.options.body,{delay:900000,symbol:'TCS'});
  client.request=async ()=>({success:true,data:{status:'completed',total_records:1}});
  assert.equal((await client.status('external-1')).status,'completed');
  client.request=async ()=>({success:true,data:[sample],nextCursor:null});
  assert.equal((await client.results('external-1',null)).data.length,1);
  client.request=async ()=>({data:[sample],nextCursor:'same'});
  await assert.rejects(client.results('external-1','same'),/did not advance/);
});
test('unsafe extraction creation is never automatically retried',async ()=> {
  const original=globalThis.fetch;let calls=0;
  globalThis.fetch=async ()=> {calls++;throw new TypeError('network failure');};
  try {
    const client=new BseClient({bseBaseUrl:'http://localhost:4001',bseTimeoutMs:100,externalIdempotency:false});
    await assert.rejects(client.request('/extractions',{method:'POST',body:{delay:0}}));assert.equal(calls,1);
  } finally {globalThis.fetch=original;}
});
