# PostgreSQL migration record

## Before migration

The application had synchronous better-sqlite3 models, PRAGMA/WAL setup, SQLite placeholders and INSERT OR IGNORE queries. Trade seeding and an internally hosted mock exchange supplied local data. A random market worker wrote simulated ticks; imports used in-memory job state and SSE replay used an in-memory ring buffer.

Runtime code inspected: every server model, route, controller, database initializer/seeder, worker and mock; the React App, hook, styles, startup and Vite configuration; package manifests/locks, environment examples, old tests, API collection and documentation. The frontend layout was retained.

## Exact legacy inventory

| Table | Records | Treatment |
| --- | ---: | --- |
| trades | 10,000 | Imported into PostgreSQL trades, preserving optional side; source=legacy_import |
| users | 1 | Copied into users and archived |
| market_ticks | 5 | Copied as historical fallback snapshots and archived |
| worker_logs | 20 | Preserved losslessly in legacy_archive |
| import_jobs | 5 | Preserved losslessly in legacy_archive, not restarted as external jobs |
| staging_trades | 0 | Exported as empty table |

All 10,031 source records are archived as JSONB. Trade timestamps range from 2026-01-01 to 2026-01-07. No trade exceeded PostgreSQL field limits or had a nonpositive/fractional quantity or nonpositive price during inspection; full import validation also passed.

## Backup and mapping

Original files under server/data remain untouched, including SQLite WAL/SHM files. Python's standard sqlite3 read-only connection and backup API created server/backups/legacy-sqlite-backup.db, a consistent snapshot including committed WAL data. Every source table was exported to legacy-export.json. inspection.json records counts/schema and postgresql-import-report.json records destination import results. These private artifacts are ignored by Git.

The backup predates runtime removal. Do not use a raw file copy of a live database with uncheckpointed WAL as the sole backup. For a later export, use the original SQLite backup API through an offline migration tool; the PostgreSQL application itself does not load SQLite.

Mapping: trade_id is retained as primary key; client/symbol/quantity/price are validated against the requested schema; created_at becomes timestamp; type becomes optional side; total is derived from quantity*price; original id/status/total remain in the JSONB archive. SQLite timestamps without a timezone are interpreted as UTC. Import never rounds quantities or overwrites conflicting trades.

The original SQLite database was not deleted. Before any future deletion, retain both the consistent backup and export, verify source/destination IDs and values, verify archive counts, and make an explicit retention decision. Rerunning the importer safely skips matching IDs.

## Changed files

- server/src/db/connection.js: centralized pg.Pool and transaction helper.
- server/src/config/env.js and server/.env.example: validated environment and verified cloud TLS.
- server/migrations/001_postgresql.sql and 002_legacy_provenance.sql: versioned schema, indexes, checkpoints, events and legacy provenance.
- server/scripts/migrate.js, createDatabase.js, importLegacy.js: setup and non-destructive migration tools.
- server/src/repositories/*: parameterized PostgreSQL access for trades/jobs/events.
- server/src/clients/bseClient.js: actual external extraction/status/results adapter.
- server/src/workers/ingestionWorker.js: bounded background ingestion and restart recovery.
- server/src/services/*, server/src/controllers/sseController.js, server/src/routes/apiRoutes.js: ingestion coordination, committed SSE replay and compatible APIs.
- server/src/app.js and server/src/server.js: composition, health/readiness and graceful lifecycle.
- client/src/hooks/useTradePulseSSE.js and App.jsx: PostgreSQL-compatible responses, durable job status, automatic page updates, explicit source-status action and separately recorded manual orders.
- client/vite.config.js: proxy target moved to port 4000; existing styling retained with small order/status additions.
- package manifests/lockfile, tests, README, API examples and architecture/delivery documents updated.

Removed after PostgreSQL replacements passed: all old SQLite models, initializer/seeder, internal mock API/generator, random market worker/manager and obsolete SQLite import test. No SQLite package or SQL remains in active runtime code. Source database files are retained only as migration artifacts.
