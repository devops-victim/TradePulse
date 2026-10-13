import { setTimeout as sleep } from 'node:timers/promises';
import { HttpError } from '../middleware/errors.js';
export function validateTimestamp(value) {
  const match=typeof value==='string' && /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$/.exec(value);
  if (!match || value.startsWith('0000') || !Number.isFinite(Date.parse(value)) ||
      new Date(`${match[1]}T00:00:00Z`).toISOString().slice(0,10)!==match[1] ||
      Number(match[2])>23 || Number(match[3])>59 || Number(match[4])>59 ||
      (match[5]!=='Z' && (Number(match[5].slice(1,3))>15 || Number(match[5].slice(4))>59))) {
    throw new Error('Timestamp must be a valid ISO timestamp with timezone and at most six fractional digits');
  }
  return value;
}
export function timestampMicros(value) {
  return BigInt(Date.parse(value))*1000n+BigInt((/\.(\d+)/.exec(value)?.[1] || '').padEnd(6,'0').slice(3));
}
export function validateTrade(input) {
  if (!input || typeof input !== 'object') throw new Error('Trade must be an object');
  const text = (name, max) => {
    if (typeof input[name] !== 'string' || !input[name].trim() || input[name].length > max) throw new Error(`Invalid trade ${name}`);
    return input[name];
  };
  if (!['number','string'].includes(typeof input.quantity) || !/^[1-9]\d*$/.test(String(input.quantity))) throw new Error('Invalid trade quantity');
  const quantity = Number(input.quantity);
  if (!Number.isInteger(quantity) || quantity < 1 || quantity > 2147483647) throw new Error('Trade quantity must be a positive PostgreSQL integer');
  const price = String(input.price);
  if (!/^(?:0|[1-9]\d{0,11})(?:\.\d{1,2})?$/.test(price) || Number(price) <= 0) throw new Error('Invalid trade price: positive NUMERIC(14,2) required');
  validateTimestamp(input.timestamp);
  const side = input.side ?? input.type ?? null;
  if (side !== null && !['BUY','SELL'].includes(side)) throw new Error('Invalid optional trade side');
  return { trade_id: text('trade_id',40), client: text('client',100), symbol: text('symbol',20), quantity, price, timestamp: input.timestamp, side };
}
export class BseClient {
  constructor(config) { this.config = config; }
  async request(path, { method = 'GET', body, idempotencyKey } = {}) {
    const attempts = method === 'GET' || this.config.externalIdempotency ? 3 : 1;
    let lastError;
    const signal=AbortSignal.timeout(this.config.bseTimeoutMs);
    for (let attempt = 0; attempt < attempts; attempt++) {
      try {
        const response = await fetch(new URL(path, this.config.bseBaseUrl), {
          method, signal,
          headers: { Accept: 'application/json', ...(body ? { 'Content-Type': 'application/json' } : {}),
            ...(this.config.bseToken ? { Authorization: `Bearer ${this.config.bseToken}` } : {}),
            ...(idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : {}) },
          body: body ? JSON.stringify(body) : undefined,
        });
        if (!response.ok) {
          // Surface only a bounded machine-readable code, never upstream
          // messages that could contain credentials or reflected request data.
          let code='';
          try {
            const reader=response.body.getReader();let length=0;const chunks=[];
            while (true) {
              const {value,done}=await reader.read();if (done) break;
              length+=value.length;if (length>16384) {await reader.cancel();break;}
              chunks.push(Buffer.from(value));
            }
            const payload=JSON.parse(Buffer.concat(chunks).toString('utf8'));
            const candidate=payload.error?.code ?? payload.code;
            if (typeof candidate==='string' && /^[A-Z][A-Z0-9_]{0,79}$/.test(candidate)) code=candidate;
          } catch { await response.body?.cancel().catch(()=>{}); }
          const hint=code==='INVALID_CALLBACK_URL' ? ': approve BSE_CALLBACK_URL in the exchange BSE_CALLBACK_ALLOWLIST' : '';
          const error = new HttpError(502, `BSE API returned HTTP ${response.status}${code ? ` (${code})` : ''}${hint}`);
          error.retryable = response.status === 429 || response.status >= 500; throw error;
        }
        const reader = response.body.getReader(); const chunks = []; let length = 0;
        while (true) {
          const { value, done } = await reader.read(); if (done) break;
          length += value.length;
          if (length > 2 * 1024 * 1024) { await reader.cancel(); throw new Error('BSE response exceeds 2 MiB batch limit'); }
          chunks.push(Buffer.from(value));
        }
        return JSON.parse(Buffer.concat(chunks).toString('utf8'));
      } catch (error) {
        lastError = error;
        if (signal.aborted || error.retryable === false || attempt + 1 === attempts) break;
        try { await sleep(250 * (2 ** attempt),undefined,{signal}); } catch { break; }
      }
    }
    throw lastError;
  }
  async initiate(job) {
    const payload = await this.request('/extractions', { method: 'POST', idempotencyKey: job.idempotency_key,
      body: { delay:job.request.delayMs, ...job.request.filters, ...(this.config.completionMode === 'webhook' ? { clientJobId:job.job_id, callbackUrl: this.config.callbackUrl } : {}) } });
    const id = payload.jobId ?? payload.job_id ?? payload.id;
    if (typeof id !== 'string' || !id || id.length > 100) throw new Error('BSE extraction receipt must include jobId (string, max 100 characters)');
    return { id, ready: ['completed','ready'].includes(String(payload.status).toLowerCase()) };
  }
  async status(id) { const payload=await this.request(`/extractions/${encodeURIComponent(id)}`); return payload.data ?? payload; }
  async results(id, cursor) {
    const params = new URLSearchParams({ limit: String(this.config.batchSize) });
    if (cursor !== null && cursor !== undefined) params.set('cursor',cursor);
    const payload = await this.request(`/extractions/${encodeURIComponent(id)}/results?${params}`);
    const data = payload.data ?? payload.trades;
    if (!Array.isArray(data) || data.length > this.config.batchSize) throw new Error('BSE results must return a bounded data array');
    const nextCursor = payload.nextCursor ?? payload.pagination?.nextCursor ?? null;
    if (nextCursor !== null && (typeof nextCursor !== 'string' || !nextCursor || nextCursor.length > 2048)) throw new Error('Invalid BSE result cursor');
    if (nextCursor !== null && (data.length === 0 || nextCursor === cursor)) throw new Error('BSE pagination did not advance');
    const total = payload.total ?? payload.pagination?.total ?? null;
    if (total !== null && (!Number.isSafeInteger(total) || total < 0)) throw new Error('Invalid BSE total');
    return { data: data.map(validateTrade), nextCursor, total };
  }
  getTrades(params = {}) { return this.request(`/getTrades?${new URLSearchParams(params)}`); }
}
