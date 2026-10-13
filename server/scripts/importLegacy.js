import { readFile, writeFile, mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { pool, transaction } from '../src/db/connection.js';
import { validateTrade } from '../src/clients/bseClient.js';
import { recordEvent } from '../src/repositories/eventRepository.js';

const timestamp = value => {
  if (typeof value !== 'string') throw new Error('Missing legacy timestamp');
  return /(Z|[+-]\d{2}:\d{2})$/.test(value) ? value : `${value.replace(' ','T')}Z`;
};
export async function importLegacy(filename,{writeReport=true}={}) {
  const exported=JSON.parse(await readFile(filename,'utf8'));
  if (!Array.isArray(exported.trades)) throw new Error('Export must include a trades array');
  // Validate everything before opening the import transaction. Never round quantities.
  const trades=exported.trades.map(row=>validateTrade({ ...row,timestamp:timestamp(row.created_at),side:row.type }));
  const result=await transaction(async client=> {
    await client.query("SELECT pg_advisory_xact_lock(hashtextextended('tradepulse-ingestion-control',0))");
    if ((await client.query("SELECT 1 FROM ingestion_jobs WHERE status IN ('pending','processing')")).rowCount) throw new Error('Cannot migrate legacy data during ingestion');
    let archived=0,inserted=0;
    for (const [table,records] of Object.entries(exported)) {
      if (!Array.isArray(records)) throw new Error('Invalid legacy table export');
      for (let start=0;start<records.length;start+=500) {
        const batch=records.slice(start,start+500).map((record,index)=>({key:String(record.trade_id ?? record.job_id ?? record.id ?? record.symbol ?? start+index),record}));
        const saved=await client.query('INSERT INTO legacy_archive(source_table,source_key,record) SELECT $1,x.key,x.record FROM jsonb_to_recordset($2::jsonb) AS x(key text,record jsonb) ON CONFLICT(source_table,source_key) DO NOTHING',[table,JSON.stringify(batch)]);
        archived+=saved.rowCount;
      }
    }
    for (let start=0;start<trades.length;start+=500) {
      const batch=JSON.stringify(trades.slice(start,start+500));
      const conflict=await client.query(`WITH incoming AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(trade_id text,client text,symbol text,quantity integer,price numeric,timestamp timestamptz,side text))
        SELECT 1 FROM trades t JOIN incoming i USING(trade_id) WHERE (t.client,t.symbol,t.quantity,t.price,t.timestamp,t.side) IS DISTINCT FROM (i.client,i.symbol,i.quantity,i.price,i.timestamp,i.side) LIMIT 1`,[batch]);
      if (conflict.rowCount) throw new Error('Legacy trade_id conflicts with existing PostgreSQL data');
      const saved=await client.query(`INSERT INTO trades(trade_id,client,symbol,quantity,price,timestamp,side,source)
        SELECT x.*,'legacy_import' FROM jsonb_to_recordset($1::jsonb) AS x(trade_id varchar(40),client varchar(100),symbol varchar(20),quantity integer,price numeric(14,2),timestamp timestamptz,side varchar(4)) ON CONFLICT(trade_id) DO NOTHING`,[batch]);
      inserted+=saved.rowCount;
    }
    for (const row of exported.market_ticks || []) await client.query('INSERT INTO market_ticks(symbol,price,change_percent,high_24h,low_24h,volume,updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(symbol) DO NOTHING',[row.symbol,row.price,row.change_percent,row.high_24h,row.low_24h,row.volume,timestamp(row.updated_at)]);
    for (const row of exported.users || []) await client.query('INSERT INTO users(id,name,email,balance,created_at) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(id) DO NOTHING',[row.id,row.name,row.email,row.balance,timestamp(row.created_at)]);
    // Historical jobs and logs remain losslessly preserved in legacy_archive,
    // rather than falsely treating old local mock jobs as external extractions.
    await recordEvent(client,'legacy_imported',null,inserted);
    return { exportedTrades:trades.length,inserted,archived };
  });
  if (writeReport) {
    await mkdir(new URL('../backups/',import.meta.url),{recursive:true});
    await writeFile(new URL('../backups/postgresql-import-report.json',import.meta.url),JSON.stringify(result,null,2));
  }
  return result;
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try { console.log(await importLegacy(process.argv[2] || fileURLToPath(new URL('../backups/legacy-export.json',import.meta.url)))); }
  catch (error) { console.error('Legacy import failed; source data is unchanged:',error.message);process.exitCode=1; }
  finally { await pool.end(); }
}
