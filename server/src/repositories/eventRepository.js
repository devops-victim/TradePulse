import { pool } from '../db/connection.js';
export async function recordEvent(client, eventType, jobId = null, affectedRecords = 0, payload = {}) {
  // Serialize event allocation through commit so cursors cannot skip late commits.
  await client.query("SELECT pg_advisory_xact_lock(hashtextextended('tradepulse-event-order',0))");
  const { rows: [event] } = await client.query(
    'INSERT INTO app_events(event_type,job_id,affected_records,payload) VALUES ($1,$2,$3,$4) RETURNING *',
    [eventType,jobId,affectedRecords,payload]);
  await client.query("SELECT pg_notify('tradepulse_events',json_build_object('schema',current_schema(),'eventId',$1::text)::text)", [event.event_id]);
  return event;
}
export const eventRepository = {
  async after(cursor, until = null) {
    const { rows } = await pool.query('SELECT * FROM app_events WHERE event_id > $1 AND ($2::bigint IS NULL OR event_id <= $2) ORDER BY event_id LIMIT 200', [cursor,until]);
    return rows;
  },
  async latest(client = pool) { return (await client.query('SELECT COALESCE(MAX(event_id),0)::text AS id FROM app_events')).rows[0].id; },
};
export function eventPayload(event) {
  return { ...event.payload, createdAt:event.created_at, eventId: event.event_id, eventType: event.event_type, jobId: event.job_id, affectedRecords: event.affected_records };
}
