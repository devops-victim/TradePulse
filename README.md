# TradePulse

React dashboard and Express backend for trades stored in PostgreSQL and pulled from an independently hosted BSE API. The existing dashboard layout is retained. No local exchange, synthetic trade generator, polling loop, cron job, or periodic ingestion scheduler runs in this application.

## Local setup

Requires Node.js 22+ and PostgreSQL 14+ (verified here with Node.js 24 and PostgreSQL 18).

```powershell
npm --prefix server install
npm --prefix client install
Copy-Item server/.env.example server/.env
# Edit server/.env; do not overwrite an existing configured .env.
npm run db:create
npm run migrate
npm run server
```

In a second terminal:

```powershell
npm run client
```

Backend: http://localhost:4000. Dashboard: http://localhost:5173. Start the independent BSE service separately on port 4001. The TradePulse backend never starts an exchange server.

`db:create` creates the dedicated `tradepulse` database through the `postgres` maintenance database, if absent. The account needs CREATEDB permission. Alternatively execute `CREATE DATABASE tradepulse;` with an administrator. For cloud-hosted PostgreSQL, provision the database through the provider and skip `db:create`.

Migrations are numbered SQL files in `server/migrations`. They run transactionally under an advisory lock and are recorded in `schema_migrations`. Rerunning skips applied versions. Startup checks the schema; it does not seed or silently recreate tables. Add a new migration rather than editing an applied one.

## Configuration

The backend loads `server/.env` independently of the working directory. Existing process environment values take precedence. Never commit credentials. URL-encode special password characters.

```env
PORT=4000
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/tradepulse
BSE_API_BASE_URL=http://localhost:4001
NODE_ENV=development
```

Additional settings are documented in `server/.env.example`:

| Variable | Default | Purpose |
| --- | --- | --- |
| CORS_ORIGIN | http://localhost:5173 | Allowed dashboard origin |
| PG_POOL_MAX | 12 | PostgreSQL connection-pool capacity |
| PG_SSL_MODE | local: disable; cloud: verify-full | Verified TLS mode |
| PG_SSL_CA_FILE | unset | Optional provider CA PEM file |
| BSE_API_TOKEN | unset | Optional external API bearer token |
| BSE_REQUEST_TIMEOUT_MS | 20000 | Overall deadline across a request's retries, at most 20 seconds |
| INGESTION_BATCH_SIZE | 500 | Result page limit, at most 1000 |
| BSE_COMPLETION_MODE | manual | manual or webhook |
| BSE_CALLBACK_URL | unset | Public URL ending in /api/bse/notifications, required for webhook mode |
| BSE_WEBHOOK_SECRET | unset | Shared notification bearer secret, required for webhook mode |
| BSE_SUPPORTS_IDEMPOTENCY | false | Enable only if the source implements Idempotency-Key on extraction POSTs |

For remote PostgreSQL use `PG_SSL_MODE=verify-full`. Certificate verification stays enabled. Supply a provider CA with `PG_SSL_CA_FILE` when necessary. The application removes conflicting URL TLS settings before passing its explicit verified TLS configuration to `pg`. It never changes global TLS verification.

Use a persistent Node process and a direct PostgreSQL connection or session-mode pooler. PostgreSQL LISTEN and session advisory locks do not work through transaction-mode poolers. Pool sizing must account for one LISTEN connection plus pooled queries and an active worker lock connection.

## External BSE contract

The running external service's documentation was inspected at `Assignment/tradepulse-database/README.md`; its files were not changed. Its API accepts:

- `GET /getTrades?limit=...&cursor=...&symbol=...&from=...&to=...&delay=...` for explicitly requested short retrievals.
- `POST /extractions` with optional `delay` (milliseconds), `symbol`, `from`, and `to`. A short 202 receipt contains `jobId`.
- `GET /extractions/:jobId` returns `{success:true,data:{job_id,status,total_records,...}}`.
- `GET /extractions/:jobId/results?limit=500&cursor=...` returns `{success:true,data:[...],nextCursor:null|token}`. Each record contains `trade_id`, `client`, `symbol`, integer `quantity`, decimal-string `price`, and timezone-qualified `timestamp`.

`server/src/clients/bseClient.js` is the adapter boundary. Dashboard `delayMs` is translated to the external `delay` field. Local append/replace mode is not sent upstream. The source does not accept count or seed on extraction requests, so those old mock controls were replaced with a symbol selector. Long pulls always use the extraction API; the application never holds a synchronous GET open for 15 minutes.

Prices remain NUMERIC decimal strings and timestamps retain source microseconds during validation and insertion. The source does not supply BUY/SELL, so side is nullable and displayed as unknown. Legacy sides remain preserved; missing source sides are not fabricated.

### Missing automatic completion dependency

**The currently running BSE service has no webhook, SSE completion feed, or idempotent extraction POST. TradePulse cannot automatically learn that a delayed extraction finished from the four existing endpoints without polling.** Automatic downstream dashboard updates are implemented; an event-driven source completion mechanism remains an external integration dependency.

In `manual` mode the dashboard explicitly shows this limitation. Clicking **Check source status** issues one status request; if complete, ingestion starts and all open dashboards update automatically through SSE. There is no repeated status loop or scheduled readiness check. On a completion trigger, the worker may make one metadata request to verify readiness and expected record count before fetching bounded result pages.

For automatic completion, the source must implement the documented notification contract (or another event transport adapted to the same service interface):

```http
POST /api/bse/notifications
Authorization: Bearer <BSE_WEBHOOK_SECRET>
Content-Type: application/json

{"externalJobId":"SOURCE_JOB_ID","clientJobId":"OPTIONAL_INTERNAL_UUID","status":"completed"}
```

`status` may be completed or failed. Notifications are acknowledged after their state is persisted, before ingestion finishes. Duplicate notifications for a completed job are safe. The source should retry notification delivery after disconnects. Configure the callback secret on the source separately; TradePulse never embeds it in extraction metadata.

Webhook mode sends `callbackUrl` and `clientJobId` in the extraction request as registration metadata. The current BSE service rejects those fields, so **do not enable webhook mode until the external service supports both registration and authenticated delivery**. No source-side code was added to this repository.

## Ingestion and recovery

1. `POST /api/pulls` durably creates a job and returns 202 without awaiting a source HTTP request. Supply an Idempotency-Key header (or idempotencyKey body field) for safe retry of the accepting request.
2. A background task owns a PostgreSQL session advisory lock for its job, calls POST /extractions, and stores the external job ID. A unique partial index permits one active pull across all dashboard users and backend instances.
3. The job waits durably for a completion notification, or an explicit user-requested status check in manual mode. No database connection is held throughout the source's 15-minute wait.
4. The worker reads bounded result pages and validates every record. A transaction stages each batch together with its cursor checkpoint and progress event. Interrupted retrieval resumes at the next committed cursor.
5. One final transaction publishes the staged dataset, deduplicates by trade_id, updates job counters/status, inserts a lightweight completion event, and issues PostgreSQL NOTIFY. Events are delivered only after commit. Replace mode deletes the previous dataset inside the same publication transaction; failed retrieval leaves existing trades intact.
6. Startup performs one recovery scan of active jobs. Known external IDs and saved checkpoints are reused. Completed jobs are not started again.

Identical duplicate trades are skipped; a reused trade_id with different values fails visibly rather than silently overwriting the ledger. Retrieval requests retry transient failures within one bounded deadline. Extraction creation is not automatically retried unless upstream idempotency is explicitly confirmed. A crash/timeout after source acceptance but before storing its ID leaves an uncertain initiation; automatic recreation is blocked. Use POST /api/pulls/:jobId/reconcile with {"externalJobId":"KNOWN_SOURCE_JOB_ID"} to attach the original extraction explicitly. The adapter checks its status/filters before resuming; it never recreates an uncertain source job. This limitation cannot be removed safely without source idempotency or correlation lookup.

## SSE and dashboard

`app_events` is a durable event log. Each backend instance listens to PostgreSQL NOTIFY and fans out committed events to its connected clients. SSE payloads contain event ID/type, job ID and record counts, not thousands of trades. Event allocation is serialized until commit so replay cursors cannot skip late commits.

SSE connections close cleanly at 25 seconds with a reconnect instruction. The browser reconnects after one second with its last event cursor. Missed events are replayed from PostgreSQL, followed by a consistent current job/statistics snapshot. Database-listener disconnects trigger reconnection and close streams so clients can resynchronize. Heartbeats are transport keepalive messages, not data polling.

The dashboard reads persisted trades immediately, independently of ingestion state. The table uses server-side 30-row pagination with timestamp DESC/trade_id DESC ordering. SSE invalidations refresh the user's current page without a browser reload. It moves to the last valid page only when the selected page no longer exists after replacement. Failed reads retain previously displayed rows. Empty/loading/error/disconnected states are visible.

Manual entries remain available as **orders**, stored separately in manual_orders and displayed in the existing entry panel. They never create externally ingested trades. Displayed market prices come from the latest persisted trades; preserved historical snapshots are a fallback, not a random price worker.

## Backend routes

| Method | Endpoint | Purpose |
| --- | --- | --- |
| GET | /api/trades?page=1&symbol=TCS | Persisted trade page, fixed 30 rows |
| GET | /api/trades/:tradeId | Single persisted trade |
| GET | /api/trades/stats | Ledger totals |
| POST | /api/pulls | Accept background pull |
| GET | /api/pulls | Recent jobs |
| GET | /api/pulls/:jobId | Durable job status, no source request |
| POST | /api/pulls/:jobId/refresh | Explicit one-shot source status check |
| POST | /api/pulls/:jobId/retry | Retry a safe failed ingestion |
| POST | /api/pulls/:jobId/reconcile | Explicitly attach a known source ID after uncertain initiation |
| POST | /api/bse/notifications | Authenticated completion adapter |
| GET | /api/events | SSE updates and durable replay |
| GET | /api/health | Application/database readiness and completion configuration |
| GET | /api/market | Latest stored prices |
| GET, POST | /api/orders | Recent/manual orders |

Compatibility aliases: POST /api/bse/import, GET /api/bse/jobs, GET /api/bse/jobs/active. GET /api/bse/getTrades forwards a short explicit source GET; it generates nothing locally. GET /api/users and /api/worker/logs preserve access to historical support data. POST /api/reset clears the ledger only in non-production mode and refuses while ingestion is active. POST /api/trades and simulated worker controls return 410 with a migration explanation.

Example accepting request:

```powershell
Invoke-RestMethod http://localhost:4000/api/pulls -Method Post -ContentType application/json -Body '{"delayMs":900000,"mode":"append","idempotencyKey":"demo-pull-1"}'
```

## Preserved legacy data

The original SQLite database and WAL files were inspected and retained in server/data. A consistent backup, full JSON export, and inspection report are in the ignored server/backups directory. No SQLite runtime package remains. See [migration notes](docs/MIGRATION.md) for counts, field mapping, validation, and recovery strategy.

For this workspace, the dedicated PostgreSQL database has been created/migrated and all 10,000 existing trades imported. All 10,031 original records are archived losslessly; historical local mock jobs are not misrepresented as new external jobs. Preserved trades have source=legacy_import; newly ingested records have source=external_bse.

To import the already prepared export into another configured database:

```powershell
npm run import:legacy
# Or: node server/scripts/importLegacy.js path/to/legacy-export.json
```

The import validates everything first and commits atomically. Rerunning deduplicates existing records. Invalid quantities, prices, timestamps or conflicting IDs fail the import without changing the source database or silently rounding values. Keep backups until the destination is verified and a separate retention decision is made.

## Tests

```powershell
npm test
npm --prefix client run lint
npm --prefix client run build
```

Integration tests create/drop only a unique temporary PostgreSQL schema, never truncate the application schema. The database role needs CREATE schema privileges. Set TEST_DATABASE_URL to another PostgreSQL database if preferred. The full legacy-data test is skipped explicitly on fresh clones without the private export.

To additionally test the separately running BSE service:

```powershell
$env:TEST_LIVE_BSE='true'
npm test
```

This creates one short external extraction with delay=0 and retrieves its actual paginated results; no second mock exchange is started. Tests cover migrations, typed validation, exact decimals/microseconds, immediate acceptance, concurrency/idempotency, PostgreSQL persistence, duplicate/conflict handling, checkpoint recovery, failures, manual-order separation, multiple SSE clients, durable replay and 25-second stream rotation. See [verification notes](DELIVERY-AND-VERIFICATION.md).

## Deployment dependencies

Provision PostgreSQL, apply migrations as a release step, configure the independent BSE URL, and run npm --prefix server start in a persistent process. Serve the client build with /api forwarded to the backend; disable SSE proxy buffering and permit connections to live for at least 25 seconds. Public deployments need the deployment's access control around user-facing routes; webhook authentication is already enforced. Event/job retention is explicit administration, not an automatic cleanup scheduler.

When your external API is cloud-hosted, set BSE_API_BASE_URL to its HTTPS origin; the same adapter is used without rebuilding the dashboard. Set BSE_CALLBACK_URL to the publicly reachable TradePulse notification URL only after the source implements webhook support.

The remaining source-side work is completion notification support and, ideally, idempotent extraction creation. Neither is falsely emulated by timers or polling in TradePulse.
#   t r a d e p u l s e  
 