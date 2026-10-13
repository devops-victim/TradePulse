import pg from 'pg';
import { loadConfig } from '../config/env.js';
export const config = loadConfig();
export const pool = new pg.Pool({ connectionString: config.databaseUrl, ssl: config.ssl, max: config.poolMax,
  connectionTimeoutMillis: 5000, idleTimeoutMillis: 30000, statement_timeout: 15000, application_name: 'tradepulse' });
pool.on('error', error => console.error('PostgreSQL connection error:', error.code || error.name));
export async function transaction(work, options = '') {
  const client = await pool.connect();
  try {
    await client.query(`BEGIN ${options}`);
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (error) { await client.query('ROLLBACK').catch(() => {}); throw error; }
  finally { client.release(); }
}
export default pool;
