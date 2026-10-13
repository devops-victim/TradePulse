import pg from 'pg';
import { loadConfig } from '../src/config/env.js';
const config=loadConfig();
const url=new URL(config.databaseUrl);url.pathname='/postgres';
const admin=new pg.Pool({connectionString:url.toString(),ssl:config.ssl,connectionTimeoutMillis:5000});
try {
  const exists=(await admin.query('SELECT 1 FROM pg_database WHERE datname=$1',['tradepulse'])).rowCount;
  if (!exists) await admin.query('CREATE DATABASE tradepulse');
  console.log(exists ? 'tradepulse database already exists' : 'Created tradepulse database');
} catch(error) {console.error('Database creation failed:',error.code || error.name);process.exitCode=1;}
finally {await admin.end();}
