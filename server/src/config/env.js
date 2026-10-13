import dotenv from 'dotenv';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
dotenv.config({ path: fileURLToPath(new URL('../../.env', import.meta.url)), quiet: true });

export function loadConfig(env = process.env) {
  const integer = (key, fallback, min, max) => {
    const value = Number(env[key] ?? fallback);
    if (!Number.isInteger(value) || value < min || value > max) throw new Error(`Invalid ${key}`);
    return value;
  };
  const url = (key, fallback, protocols) => {
    let value;
    try { value = new URL(env[key] || fallback); } catch { throw new Error(`${key} must be a valid URL`); }
    if (!protocols.includes(value.protocol)) throw new Error(`Invalid ${key} protocol`);
    return value;
  };
  const database = url('DATABASE_URL', '', ['postgres:', 'postgresql:']);
  if (decodeURIComponent(database.pathname.slice(1)) !== 'tradepulse' && env.NODE_ENV !== 'test') throw new Error('DATABASE_URL must select the dedicated tradepulse database');
  const bse = url('BSE_API_BASE_URL', 'http://localhost:4001', ['http:', 'https:']);
  const cloud = !['localhost', '127.0.0.1', '[::1]'].includes(database.hostname);
  const sslMode = env.PG_SSL_MODE || database.searchParams.get('sslmode') || (cloud ? 'verify-full' : 'disable');
  if (!['disable', 'verify-full'].includes(sslMode)) throw new Error('PG_SSL_MODE/sslmode must be disable or verify-full');
  if (cloud && sslMode === 'disable') throw new Error('Cloud PostgreSQL requires verified TLS');
  for (const key of ['ssl', 'sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'sslnegotiation', 'uselibpqcompat']) database.searchParams.delete(key);
  const completionMode = env.BSE_COMPLETION_MODE || 'manual';
  if (!['manual', 'webhook'].includes(completionMode)) throw new Error('Invalid BSE_COMPLETION_MODE');
  if (completionMode === 'webhook' && (!env.BSE_WEBHOOK_SECRET || !env.BSE_CALLBACK_URL)) throw new Error('Webhook mode requires BSE_WEBHOOK_SECRET and BSE_CALLBACK_URL');
  if (completionMode === 'webhook' && env.BSE_WEBHOOK_SECRET.length < 32) throw new Error('BSE_WEBHOOK_SECRET must contain at least 32 characters');
  if (env.BSE_CALLBACK_URL) {
    const callback=url('BSE_CALLBACK_URL', '', ['http:', 'https:']);
    if (callback.username || callback.password || callback.hash) throw new Error('BSE_CALLBACK_URL must not contain credentials or a fragment');
    if (env.NODE_ENV === 'production' && callback.protocol !== 'https:') throw new Error('Production BSE_CALLBACK_URL requires HTTPS');
  }
  return {
    port: integer('PORT', 4000, 1, 65535), nodeEnv: env.NODE_ENV || 'development', databaseUrl: database.toString(),
    ssl: sslMode === 'disable' ? false : { rejectUnauthorized: true, ...(env.PG_SSL_CA_FILE ? { ca: readFileSync(env.PG_SSL_CA_FILE, 'utf8') } : {}) },
    poolMax: integer('PG_POOL_MAX', 12, 5, 50), bseBaseUrl: bse.toString(), bseToken: env.BSE_API_TOKEN || '',
    completionMode, callbackUrl: env.BSE_CALLBACK_URL || '', webhookSecret: env.BSE_WEBHOOK_SECRET || '',
    externalIdempotency: env.BSE_SUPPORTS_IDEMPOTENCY === 'true', batchSize: integer('INGESTION_BATCH_SIZE', 500, 1, 1000),
    bseTimeoutMs: integer('BSE_REQUEST_TIMEOUT_MS', 20000, 100, 20000), corsOrigin: env.CORS_ORIGIN || 'http://localhost:5173',
  };
}
