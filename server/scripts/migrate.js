import { readdir, readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { pool } from '../src/db/connection.js';
export async function migrate() {
  const client = await pool.connect();
  try {
    await client.query("SELECT pg_advisory_lock(hashtextextended('tradepulse-migrations',0))");
    await client.query('CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW())');
    const directory = fileURLToPath(new URL('../migrations/', import.meta.url));
    for (const filename of (await readdir(directory)).filter(name => /^\d+.*\.sql$/.test(name)).sort()) {
      if ((await client.query('SELECT 1 FROM schema_migrations WHERE version=$1', [filename])).rowCount) continue;
      await client.query('BEGIN');
      try {
        await client.query(await readFile(`${directory}/${filename}`, 'utf8'));
        await client.query('INSERT INTO schema_migrations(version) VALUES ($1)', [filename]);
        await client.query('COMMIT'); console.log(`Applied ${filename}`);
      } catch (error) { await client.query('ROLLBACK'); throw error; }
    }
  } finally {
    await client.query("SELECT pg_advisory_unlock(hashtextextended('tradepulse-migrations',0))").catch(() => {}); client.release();
  }
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try { await migrate(); } catch (error) { console.error('Migration failed:', error.message); process.exitCode = 1; }
  finally { await pool.end(); }
}
