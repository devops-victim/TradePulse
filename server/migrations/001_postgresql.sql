CREATE TABLE trades (
  trade_id VARCHAR(40) PRIMARY KEY,
  client VARCHAR(100) NOT NULL,
  symbol VARCHAR(20) NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0),
  price NUMERIC(14,2) NOT NULL CHECK (price > 0),
  timestamp TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  side VARCHAR(4) CHECK (side IN ('BUY', 'SELL'))
);
CREATE INDEX trades_timestamp_idx ON trades (timestamp DESC, trade_id DESC);
CREATE INDEX trades_symbol_timestamp_idx ON trades (symbol, timestamp DESC, trade_id DESC);
CREATE TABLE ingestion_jobs (
  job_id UUID PRIMARY KEY,
  external_job_id VARCHAR(100),
  status VARCHAR(12) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','completed','failed')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), completed_at TIMESTAMPTZ,
  records_received INTEGER NOT NULL DEFAULT 0, error_message TEXT,
  idempotency_key VARCHAR(128) NOT NULL UNIQUE, request JSONB NOT NULL DEFAULT '{}',
  phase VARCHAR(30) NOT NULL DEFAULT 'pending', notification_received BOOLEAN NOT NULL DEFAULT FALSE,
  external_attempted BOOLEAN NOT NULL DEFAULT FALSE, next_cursor TEXT, next_batch INTEGER NOT NULL DEFAULT 0,
  expected_records INTEGER, records_inserted INTEGER NOT NULL DEFAULT 0, duplicates_skipped INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX one_active_ingestion ON ingestion_jobs ((1)) WHERE status IN ('pending','processing');
CREATE UNIQUE INDEX ingestion_external_job_idx ON ingestion_jobs (external_job_id) WHERE external_job_id IS NOT NULL;
CREATE TABLE ingestion_staging (
  job_id UUID NOT NULL REFERENCES ingestion_jobs(job_id), trade_id VARCHAR(40) NOT NULL,
  client VARCHAR(100) NOT NULL, symbol VARCHAR(20) NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0), price NUMERIC(14,2) NOT NULL CHECK (price > 0),
  timestamp TIMESTAMPTZ NOT NULL, side VARCHAR(4), PRIMARY KEY (job_id, trade_id)
);
CREATE TABLE ingestion_batches (
  job_id UUID NOT NULL REFERENCES ingestion_jobs(job_id), batch_number INTEGER NOT NULL,
  cursor_in TEXT, cursor_out TEXT, records_received INTEGER NOT NULL, PRIMARY KEY (job_id, batch_number)
);
CREATE TABLE app_events (
  event_id BIGSERIAL PRIMARY KEY, event_type VARCHAR(40) NOT NULL,
  job_id UUID REFERENCES ingestion_jobs(job_id), affected_records INTEGER NOT NULL DEFAULT 0,
  payload JSONB NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE manual_orders (
  order_id UUID PRIMARY KEY, client VARCHAR(100) NOT NULL, symbol VARCHAR(20) NOT NULL,
  side VARCHAR(4) NOT NULL CHECK (side IN ('BUY','SELL')), quantity INTEGER NOT NULL CHECK (quantity > 0),
  price NUMERIC(14,2) NOT NULL CHECK (price > 0), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE legacy_archive (
  source_table TEXT NOT NULL, source_key TEXT NOT NULL, record JSONB NOT NULL,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (source_table, source_key)
);
CREATE TABLE market_ticks (
  symbol VARCHAR(20) PRIMARY KEY, price NUMERIC(14,2) NOT NULL, change_percent NUMERIC,
  high_24h NUMERIC, low_24h NUMERIC, volume NUMERIC, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE NOT NULL, balance NUMERIC, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE TABLE worker_logs (id BIGSERIAL PRIMARY KEY, task_name TEXT NOT NULL, details JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
