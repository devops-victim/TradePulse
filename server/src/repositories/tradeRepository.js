import { randomUUID } from 'node:crypto';
import { pool, transaction } from '../db/connection.js';
import { recordEvent } from './eventRepository.js';
import { HttpError } from '../middleware/errors.js';

export const tradeColumns = 'trade_id,client,symbol,quantity,price,timestamp,ingested_at,side AS type, timestamp AS created_at,(quantity*price)::numeric(24,2) AS total';
export const tradeRepository = {
  async getPaginated({ page = 1, symbol } = {}) {
    if (!Number.isSafeInteger(page) || page < 1 || page > 10000000) throw new HttpError(400,'Invalid page');
    if (symbol !== undefined && (typeof symbol !== 'string' || symbol.length > 20)) throw new HttpError(400,'Invalid symbol');
    return transaction(async client => {
      const total = Number((await client.query('SELECT COUNT(*) FROM trades WHERE ($1::text IS NULL OR symbol=$1)', [symbol || null])).rows[0].count);
      const { rows } = await client.query(`SELECT ${tradeColumns} FROM trades WHERE ($1::text IS NULL OR symbol=$1) ORDER BY timestamp DESC,trade_id DESC LIMIT 30 OFFSET $2`, [symbol || null, (page-1)*30]);
      const totalPages = Math.max(1,Math.ceil(total/30));
      return { data: rows, pagination: { page,limit:30,total,totalPages,hasPrevPage:page>1,hasNextPage:page<totalPages } };
    }, 'ISOLATION LEVEL REPEATABLE READ READ ONLY');
  },
  async getByTradeId(id) { return (await pool.query(`SELECT ${tradeColumns} FROM trades WHERE trade_id=$1`,[id])).rows[0]; },
  async getStats(client = pool) {
    const { rows: [row] } = await client.query("SELECT COUNT(*)::integer AS total,COALESCE(SUM(quantity*price),0)::text AS volume,COUNT(*) FILTER (WHERE side='BUY')::integer AS buys,COUNT(*) FILTER (WHERE side='SELL')::integer AS sells,MAX(timestamp) AS last FROM trades");
    return { totalTrades:row.total,totalVolume:row.volume,buyTrades:row.buys,sellTrades:row.sells,lastTradeTime:row.last };
  },
  async getMarket() {
    const { rows } = await pool.query('SELECT DISTINCT ON (symbol) symbol,price,timestamp AS updated_at FROM trades ORDER BY symbol,timestamp DESC,trade_id DESC');
    // Original snapshots remain available only as a preserved-data fallback.
    return rows.length ? rows : (await pool.query('SELECT * FROM market_ticks ORDER BY symbol')).rows;
  },
  async clearAll() {
    return transaction(async client => {
      await client.query("SELECT pg_advisory_xact_lock(hashtextextended('tradepulse-ingestion-control',0))");
      if ((await client.query("SELECT 1 FROM ingestion_jobs WHERE status IN ('pending','processing')")).rowCount) throw new HttpError(409,'An ingestion job is active');
      await client.query('DELETE FROM trades');
      await recordEvent(client,'database_reset');
    });
  },
  async createOrder(order) {
    return transaction(async client => {
      const { rows: [row] } = await client.query('INSERT INTO manual_orders(order_id,client,symbol,side,quantity,price) VALUES ($1,$2,$3,$4,$5,$6) RETURNING *', [randomUUID(),order.client,order.symbol,order.side,order.quantity,order.price]);
      await recordEvent(client,'order_saved',null,1);
      return row;
    });
  },
  async orders() { return (await pool.query('SELECT *, (quantity*price)::numeric(24,2) AS total FROM manual_orders ORDER BY created_at DESC,order_id DESC LIMIT 30')).rows; },
};
