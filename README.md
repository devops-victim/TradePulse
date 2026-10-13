# TradePulse

> Real-time asynchronous trade ingestion, deduplication engine, and live monitoring dashboard built with React, Node.js/Express, PostgreSQL, and Server-Sent Events (SSE).

[![Node.js Version](https://img.shields.io/badge/node.js-v22%2B-blue)](https://nodejs.org/)
[![PostgreSQL](https://img.shields.io/badge/postgresql-v14%2B-336791)](https://www.postgresql.org/)
[![React](https://img.shields.io/badge/react-v19-61dafb)](https://react.dev/)
[![Vite](https://img.shields.io/badge/vite-v8-646cff)](https://vitejs.dev/)
[![Tailwind CSS](https://img.shields.io/badge/tailwind-v4-38bdf8)](https://tailwindcss.com/)

---

## 1. Project Overview & Problem Statement

In modern financial data pipelines, retrieving large batches of historical and simulated trade execution records from external exchange systems (such as the Bombay Stock Exchange / BSE) frequently involves heavy queries or asynchronous extractions that can take upwards of **15 minutes** (900 seconds) to compile.

### The Problem
Traditional web architectures often fail in this environment due to fundamental design limitations:
- **Proxy & Gateway Timeouts:** Reverse proxies, load balancers, and cloud gateways (such as Vercel, Render, Cloudflare, AWS ALB) enforce strict request deadlines (typically 20 to 30 seconds). Holding an HTTP `GET` connection open for 15 minutes causes universal `504 Gateway Timeout` or connection reset errors.
- **Client & Server Blocking:** When an extraction is awaited synchronously, server worker threads and database connections are held idle, exhausting pool capacity and preventing users from querying existing records or placing manual orders.
- **Client Polling Fatigue:** Querying an exchange endpoint in tight polling loops wastes network bandwidth, risks hitting rate limits (HTTP 429), and creates distributed thundering herd problems.
- **Duplicate & Corrupt Data Ingestion:** Network failures during multi-page batch retrieval often cause duplicate deliveries or partial writes, resulting in dirty ledgers and corrupted transaction statistics.

### The TradePulse Solution
TradePulse solves these challenges by implementing a decoupled, asynchronous, event-driven ingestion architecture:
1. **Immediate Acceptance (HTTP 202):** Extractions are queued immediately with durable UUID jobs and idempotency keys; the HTTP connection resolves in under 100 milliseconds.
2. **Zero Blocked Database or HTTP Connections:** The background worker triggers the exchange extraction and releases all locks while the external system processes the job over its 15-minute window.
3. **Flexible Completion Discovery:** Supports both an authenticated event-driven **Webhook** adapter (`POST /api/bse/notifications`) and an explicit single-shot **Manual** status check (`POST /api/pulls/:jobId/refresh`). No automated polling loops run in either the client or server.
4. **Staged Cursor Checkpointing:** Extractions are fetched in bounded 500-record batches, verified against strict numeric/timestamp schemas, and staged in temporary tables with persistent cursor tracking before atomic publication.
5. **Conflict-Detecting Deduplication:** Records are published to the primary ledger inside a single database transaction. Identical records are deduplicated (`ON CONFLICT (trade_id) DO NOTHING`), while altered records with colliding IDs trigger hard failures to preserve financial data integrity.
6. **Durable SSE Live Feed with Connection Rotation:** Open dashboards receive real-time updates via Server-Sent Events backed by PostgreSQL `LISTEN`/`NOTIFY`. Connections rotate cleanly every 25 seconds to preempt proxy drops, and reconnected clients replay missed events seamlessly via durable database cursors.

---

## 2. Live Deployments & Repository Links

| Component | Target / Environment | URL |
|---|---|---|
| **TradePulse Frontend** | Vercel (Production SPA) | [https://tradepulse-flax-five.vercel.app/](https://tradepulse-flax-five.vercel.app/) |
| **TradePulse Backend** | Render (Production Web Service) | [https://tradepulse-gok9.onrender.com](https://tradepulse-gok9.onrender.com) |
| **Main Repository** | GitHub | [https://github.com/devops-victim/tradepulse](https://github.com/devops-victim/tradepulse) |
| **BSE Mock API Repository** | GitHub (External Service) | [https://github.com/devops-victim/tradepulse-database](https://github.com/devops-victim/tradepulse-database) |
| **BSE Mock API Service** | Render (External Service) | [https://tradepulse-bse-api.onrender.com](https://tradepulse-bse-api.onrender.com) |

---

## 3. Key Implemented Features

Every feature listed below is verified directly against the production source code:

- **Asynchronous Extraction Lifecycle:** `POST /api/pulls` accepts extraction tasks with configurable delays up to 900,000 ms (15 minutes), returning an immediate `202 Accepted` response with a tracking `jobId`.
- **Concurrency Serialization:** A PostgreSQL partial unique index (`one_active_ingestion`) combined with session advisory locks (`pg_try_advisory_lock`) guarantees that only one extraction job can be active at a time across all backend instances.
- **Dual Completion Modes:**
  - `manual`: Discloses source limitations on the dashboard; users trigger a one-shot status check without any background polling loops.
  - `webhook`: Authenticated webhook endpoint (`POST /api/bse/notifications`) protected with cryptographic constant-time token comparison (`timingSafeEqual`) and job correlation checks.
- **Resilient Cursor-Based Retrieval:** Ingestion retrieves records from the external service in chunks of up to 500 records, storing incremental progress and cursor checkpoints (`ingestion_batches`). Interrupted retrievals resume from the last committed cursor.
- **Atomic Deduplication & Conflict Detection:** Ingested trades are written to PostgreSQL in a single transaction. Identical `trade_id` values are cleanly skipped; conflicting field values for an existing `trade_id` cause an intentional rollback and visible failure rather than silently altering financial ledger history.
- **Append vs. Replace Modes:** Users can append new trades to the ledger or replace existing records. In replace mode, the existing ledger is truncated within the same atomic transaction that publishes the new dataset.
- **Durable Server-Sent Events (SSE):** `EventHub` broadcasts lightweight event notifications (`import_started`, `import_progress`, `import_completed`, etc.) across PostgreSQL `LISTEN`/`NOTIFY`. Trades themselves are never dumped over the SSE stream; clients fetch only their current paginated view.
- **Intentional 25-Second Stream Rotation:** SSE streams are proactively closed at 25 seconds with an `event: reconnect` frame, allowing clients to reconnect before upstream cloud proxies (Render/Vercel) kill idle sockets at 30 seconds. Missed events are restored using PostgreSQL event cursors (`Last-Event-ID`).
- **30-Row Server-Side Pagination:** React dashboard displays trades sorted by `timestamp DESC, trade_id DESC`, supporting fast navigation across tens of thousands of trades with zero UI lag.
- **Segregated Manual Orders Ledger:** Manual orders entered via the UI are stored in a dedicated `manual_orders` table, completely isolated from externally ingested exchange trades (`trades`).
- **Lossless Legacy Archive:** Previous SQLite ledger records (10,031 total items) are losslessly imported into PostgreSQL, tagged with `source='legacy_import'`, and backed up in `legacy_archive`.

---

## 4. Technology Stack

### Frontend
- **Framework:** React 19 (`react`, `react-dom`)
- **Build Tool:** Vite 8 (`vite`, `@vitejs/plugin-react`)
- **Styling:** Tailwind CSS v4 (`tailwindcss`, `@tailwindcss/vite`) with custom dark dashboard theme
- **Icons:** Lucide React (`lucide-react`)
- **Linter:** Oxlint (`oxlint`)
- **Streaming Client:** Native browser `EventSource` managed via custom `useTradePulseSSE` hook

### Backend
- **Runtime:** Node.js 22+ (verified on Node.js 24 ESM)
- **Framework:** Express 4.21 (`express`)
- **Database Driver:** `pg` (Node-Postgres with `pg.Pool` connection pooling)
- **Security & Headers:** `cors`, constant-time webhook verification (`node:crypto`)
- **Environment Management:** `dotenv` with path resolution independent of process CWD
- **Process Manager / Dev:** `nodemon`
- **Request Deadline Middleware:** Custom 20-second timeout middleware with client disconnect cleanup

### Database & Persistence
- **RDBMS:** PostgreSQL 14+ (tested on PostgreSQL 18 and cloud Render PostgreSQL)
- **Messaging / IPC:** PostgreSQL `LISTEN` / `NOTIFY` (`tradepulse_events`)
- **Concurrency Control:** PostgreSQL Session & Transaction Advisory Locks (`pg_advisory_lock`, `pg_advisory_xact_lock`)
- **Schema Management:** Numbered transactional SQL migrations (`server/migrations/*.sql`) tracked in `schema_migrations`

### Integration & Infrastructure
- **External Mock Service:** Independent Express + PostgreSQL service simulating BSE extractions
- **Cloud Hosting:** Vercel (Frontend SPA rewrite proxy) & Render (Backend Web Service + Managed PostgreSQL)

---

## 5. System Architecture

```mermaid
flowchart TB
    subgraph ClientLayer["Frontend Client (Vercel / Local :5173)"]
        UI["React 19 Dashboard"]
        SSEHook["useTradePulseSSE Hook"]
        UI -->|HTTP GET/POST /api/*| SSEHook
    end

    subgraph BackendLayer["TradePulse Backend (Render / Local :4000)"]
        Router["Express Router /api"]
        IngestionSvc["IngestionService"]
        Worker["IngestionWorker (Advisory Locks)"]
        EventHubSvc["EventHub (SSE Manager)"]
        BseClientAdapter["BseClient HTTP Adapter"]

        Router --> IngestionSvc
        Router --> EventHubSvc
        IngestionSvc --> Worker
        Worker --> BseClientAdapter
    end

    subgraph DatabaseLayer["TradePulse PostgreSQL Database"]
        PGPool[("pg.Pool Connection Pool")]
        TableTrades[("trades (Ledger)")]
        TableJobs[("ingestion_jobs")]
        TableStaging[("ingestion_staging & batches")]
        TableEvents[("app_events")]
        TableOrders[("manual_orders")]
        ListenNotify["PostgreSQL LISTEN / NOTIFY"]

        IngestionSvc -.-> PGPool
        Worker --> TableJobs
        Worker --> TableStaging
        Worker --> TableTrades
        Worker --> TableEvents
        TableEvents --> ListenNotify
    end

    subgraph ExternalBSE["Independent BSE Service (Port 4001)"]
        BseAPI["BSE Mock API (:4001)"]
        BseDB[("bse_trades PostgreSQL")]

        BseAPI --> BseDB
    end

    %% Interactions
    SSEHook -->|EventSource GET /api/events| EventHubSvc
    ListenNotify -.->|NOTIFY tradepulse_events| EventHubSvc
    EventHubSvc -.->|SSE stream: reconnect at 25s| SSEHook

    BseClientAdapter -->|1. POST /extractions delay<=15m| BseAPI
    BseAPI -.->|2. Async Processing up to 15 mins| BseAPI
    BseAPI -->|3a. Webhook POST /api/bse/notifications| Router
    UI -.->|3b. Manual Click POST /api/pulls/:id/refresh| Router

    Worker -->|4. GET /extractions/:id/results batches of 500| BseAPI
    Worker -->|5. Atomic Stage & Commit Deduplication| TableTrades
```

---

## 6. Asynchronous Extraction Workflow

The sequence below illustrates how TradePulse manages a 15-minute extraction without hitting request deadlines or dropping client connections:

```mermaid
sequenceDiagram
    autonumber
    actor User as User / Browser
    participant UI as React Dashboard
    participant API as TradePulse Backend (:4000)
    participant DB as TradePulse PostgreSQL
    participant Worker as IngestionWorker
    participant BSE as External BSE API (:4001)

    User->>UI: Select 15-Minute Delay & Click "Start Import"
    UI->>API: POST /api/pulls { delayMs: 900000, mode: "append", idempotencyKey }
    API->>DB: INSERT INTO ingestion_jobs (status='pending')
    API->>DB: INSERT INTO app_events ('import_started') & NOTIFY
    API-->>UI: HTTP 202 Accepted { jobId, status: "pending" } (Elapsed: <50ms)

    Note over UI,API: Dashboard shows "Status: preparing" timer; user continues browsing trades.

    API->>Worker: wake(jobId)
    Worker->>DB: Acquire pg_try_advisory_lock('ingestion:<jobId>')
    Worker->>BSE: POST /extractions { delay: 900000, callbackUrl, clientJobId }
    BSE-->>Worker: HTTP 202 { jobId: "bse-ext-123", status: "pending" }
    Worker->>DB: UPDATE ingestion_jobs (external_job_id='bse-ext-123', phase='waiting')
    Worker->>DB: Release session lock; worker goes to sleep

    Note over Worker,BSE: 15 MINUTES ELAPSE. Zero HTTP requests or DB connections held open.

    alt Mode A: Webhook Delivery (Automated)
        BSE->>API: POST /api/bse/notifications { externalJobId, status: "completed" } (Bearer secret)
        API->>DB: Validate secret; UPDATE ingestion_jobs (phase='ready')
        API->>DB: INSERT INTO app_events ('ingestion_ready') & NOTIFY
        API-->>BSE: HTTP 202 Accepted
        API->>Worker: wake(jobId)
    else Mode B: Manual Verification (Single-Shot)
        User->>UI: Clicks "Check source status"
        UI->>API: POST /api/pulls/:jobId/refresh
        API->>BSE: GET /extractions/bse-ext-123
        BSE-->>API: { status: "completed", total_records: 10000 }
        API->>DB: UPDATE ingestion_jobs (phase='ready')
        API->>Worker: wake(jobId)
        API-->>UI: HTTP 200 { sourceStatus: "completed" }
    end

    Worker->>DB: Acquire advisory lock & verify expected records
    loop Paginated Retrieval (Batches of 500)
        Worker->>BSE: GET /extractions/bse-ext-123/results?limit=500&cursor=...
        BSE-->>Worker: HTTP 200 { data: [...], nextCursor: "..." }
        Worker->>DB: Transaction: Stage batch in ingestion_staging & checkpoint cursor
    end

    Worker->>DB: Transaction: Deduplicate, commit to trades, record completion, cleanup staging
    DB-->>API: NOTIFY tradepulse_events ('import_completed')
    API-->>UI: SSE: event: import_completed { insertedCount, duplicatesSkipped }
    UI->>API: GET /api/trades?page=1&limit=30
    API-->>UI: Updated trade ledger (refreshed in place)
```

---

## 7. Repository Structure

```
TradePulse/
├── client/                               # React + Vite frontend SPA
│   ├── public/                           # Static assets
│   ├── src/
│   │   ├── hooks/
│   │   │   └── useTradePulseSSE.js       # Resilient SSE subscription hook & status state
│   │   ├── App.css                       # Dashboard styles & animations
│   │   ├── App.jsx                       # Main dashboard: ledger table, pagination, import, manual orders
│   │   ├── index.css                     # Base CSS tokens
│   │   └── main.jsx                      # Application mount point
│   ├── package.json                      # Client dependencies (React 19, Vite, Tailwind v4)
│   ├── vercel.json                       # Production rewrite proxy to Render backend
│   └── vite.config.js                    # Dev server config & /api proxy to localhost:4000
├── docs/
│   ├── BSE-SERVICE-FIX-PROMPT.md         # Reference implementation guide for BSE webhook publisher
│   └── MIGRATION.md                      # Detailed notes on legacy SQLite to PostgreSQL migration
├── server/                               # Node.js + Express backend service
│   ├── backups/                          # Ignored archive directory for database export reports
│   ├── data/                             # Preserved legacy SQLite files (losslessly archived)
│   ├── migrations/                       # Numbered PostgreSQL SQL migration files
│   │   ├── 001_postgresql.sql            # Core tables, indexes, constraints, and app_events
│   │   └── 002_legacy_provenance.sql     # Trade source provenance column & constraint
│   ├── scripts/
│   │   ├── createDatabase.js             # Utility to create dedicated database if missing
│   │   ├── importLegacy.js               # Lossless legacy export import runner
│   │   └── migrate.js                    # Transactional migration runner with advisory locks
│   ├── src/
│   │   ├── clients/
│   │   │   └── bseClient.js              # HTTP client adapter for external BSE mock API
│   │   ├── config/
│   │   │   └── env.js                    # Strict environment validation & SSL mode parser
│   │   ├── controllers/
│   │   │   └── sseController.js          # Controller handling SSE subscription endpoints
│   │   ├── db/
│   │   │   └── connection.js             # pg.Pool setup, verified TLS, and transaction helper
│   │   ├── middleware/
│   │   │   └── errors.js                 # Centralized error handler & PostgreSQL error mapper
│   │   ├── repositories/
│   │   │   ├── eventRepository.js        # Durable app_events log and LISTEN/NOTIFY publisher
│   │   │   ├── jobRepository.js          # Ingestion job state machine, staging, and deduplication
│   │   │   └── tradeRepository.js        # Parameterized trade queries, stats, and manual orders
│   │   ├── routes/
│   │   │   └── apiRoutes.js              # REST endpoints for trades, pulls, orders, and health
│   │   ├── services/
│   │   │   ├── eventHub.js               # SSE broker, connection rotation, and client replay
│   │   │   └── ingestionService.js       # Pull initiation, refresh, and webhook validation
│   │   ├── workers/
│   │   │   └── ingestionWorker.js        # Background task worker with PostgreSQL advisory locks
│   │   ├── app.js                        # Express application factory & middleware pipeline
│   │   ├── requestDeadline.js            # 20-second request timeout enforcement
│   │   └── server.js                     # Server entrypoint with graceful shutdown handlers
│   ├── test/
│   │   ├── postgres.test.js              # Integration test suite (disposable temporary schemas)
│   │   └── unit.test.js                  # Unit tests for validation, config, and adapters
│   ├── .env.example                      # Documented environment template
│   └── package.json                      # Server dependencies (Express, pg, dotenv, cors)
├── DELIVERY-AND-VERIFICATION.md          # Formal delivery checklist and verification log
├── INTEGRATION-QA-2026-10-13.md          # Comprehensive integration QA audit report
├── package.json                          # Workspace orchestration root scripts
├── PRD.md                                # Functional requirements specification
├── README.md                             # Repository technical documentation (this file)
└── TECHNICAL-ARCHITECTURE.md             # Architecture overview and sequence flows
```

---

## 8. Local Setup & Execution Guide

Follow these instructions to run the complete TradePulse stack locally on macOS, Linux, or Windows.

### Prerequisites
- **Node.js:** v22.0.0 or higher (tested on Node.js v24.12.0)
- **PostgreSQL:** v14 or higher (tested on PostgreSQL v18)
- **Git**

---

### Step 1: Clone Repositories

You need both the main **TradePulse** repository and the independent **BSE Mock API** repository:

```bash
# Clone TradePulse main repository
git clone https://github.com/devops-victim/tradepulse.git
cd tradepulse

# (In a separate folder) Clone the external BSE Mock service
git clone https://github.com/devops-victim/tradepulse-database.git
```

---

### Step 2: Database Setup

TradePulse requires its own dedicated PostgreSQL database named `tradepulse` (or matching `tradepulse_db_*`). The external BSE service uses its own database (typically `bse_trades`).

Using `psql`:
```sql
CREATE DATABASE tradepulse;
CREATE DATABASE bse_trades;
```

---

### Step 3: Configure Environment Variables

Navigate to the `tradepulse/server` directory and copy the environment template:

#### On Linux / macOS / Bash:
```bash
cp server/.env.example server/.env
```

#### On Windows PowerShell:
```powershell
Copy-Item server/.env.example server/.env
```

Open `server/.env` and configure your credentials:

```env
PORT=4000
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/tradepulse
BSE_API_BASE_URL=http://localhost:4001
NODE_ENV=development
CORS_ORIGIN=http://localhost:5173
PG_POOL_MAX=12
PG_SSL_MODE=disable
BSE_REQUEST_TIMEOUT_MS=20000
INGESTION_BATCH_SIZE=500
BSE_COMPLETION_MODE=manual
```

> **Note on Webhook Mode:** If you have updated the BSE service with webhook publisher support, set:
> ```env
> BSE_COMPLETION_MODE=webhook
> BSE_CALLBACK_URL=http://localhost:4000/api/bse/notifications
> BSE_WEBHOOK_SECRET=your_minimum_32_character_random_secret_here
> ```

---

### Step 4: Install Dependencies & Run Migrations

From the root of the `tradepulse` directory:

#### On Linux / macOS / Bash:
```bash
# Install server & client dependencies
npm --prefix server install
npm --prefix client install

# Run database migrations
npm run migrate

# (Optional) Import preserved legacy dataset (10,000 trades)
npm run import:legacy
```

#### On Windows PowerShell:
```powershell
npm --prefix server install
npm --prefix client install
npm run migrate
npm run import:legacy
```

---

### Step 5: Start the Services

Open **three separate terminal windows** to run the complete system:

#### Terminal 1: External BSE Mock API (Port 4001)
Navigate to your cloned `tradepulse-database` directory:
```bash
cd path/to/tradepulse-database
npm install
npm run dev # or npm start
# Verified running on http://localhost:4001
```

#### Terminal 2: TradePulse Backend (Port 4000)
In the `tradepulse` repository root:
```bash
npm run server
# Verified running on http://localhost:4000
```

#### Terminal 3: TradePulse Frontend (Port 5173)
In the `tradepulse` repository root:
```bash
npm run client
# Verified running on http://localhost:5173
```

---

### Step 6: Verify Service Health

Test that the services are online:

```bash
# Check TradePulse Backend Health
curl -s http://localhost:4000/api/health
```

Expected JSON response:
```json
{
  "status": "online",
  "database": "PostgreSQL",
  "completionMode": "manual",
  "automaticCompletionConfigured": false,
  "sseConnections": 0,
  "activeImportJob": null
}
```

Now open **http://localhost:5173** in your browser to access the TradePulse live dashboard.

---

## 9. Environment Variable Reference

### Backend (`server/.env`)

| Variable | Type | Default | Required | Purpose & Constraints |
|---|---|---|---|---|
| `PORT` | Integer | `4000` | No | Port for the Express server (1 - 65535). |
| `DATABASE_URL` | String (URL) | *None* | **Yes** | PostgreSQL connection URL. Must target `tradepulse` or match `^tradepulse_db(?:_[a-z0-9]+)?$`. Special password characters must be URL-encoded. |
| `BSE_API_BASE_URL` | String (URL) | `http://localhost:4001` | **Yes** | Base URL of the external BSE Mock API service. Protocols: `http:` or `https:`. |
| `NODE_ENV` | String | `development` | No | Runtime environment (`development`, `production`, `test`). |
| `CORS_ORIGIN` | String (URL) | `http://localhost:5173` | No | Origin permitted for browser cross-origin requests. |
| `PG_POOL_MAX` | Integer | `12` | No | Maximum number of pooled database connections (min: 5, max: 50). |
| `PG_SSL_MODE` | String | `disable` (local) / `verify-full` (cloud) | No | PostgreSQL TLS mode. Must be either `disable` or `verify-full`. Cloud hosts reject `disable`. |
| `PG_SSL_CA_FILE` | String (Path) | *None* | No | Optional absolute filesystem path to a trusted root CA certificate (PEM format). |
| `BSE_API_TOKEN` | String | *Empty* | No | Optional Bearer authentication token sent to the BSE API. |
| `BSE_REQUEST_TIMEOUT_MS`| Integer | `20000` | No | Overall timeout deadline for external BSE HTTP requests (maximum 20,000 ms). |
| `INGESTION_BATCH_SIZE` | Integer | `500` | No | Maximum number of records fetched per paginated extraction page (min: 1, max: 1000). |
| `BSE_COMPLETION_MODE` | String | `manual` | No | Completion discovery mode: `manual` or `webhook`. |
| `BSE_CALLBACK_URL` | String (URL) | *None* | Only if `webhook` | Public callback URL ending in `/api/bse/notifications`. In production, protocol must be `https:`. |
| `BSE_WEBHOOK_SECRET` | String | *None* | Only if `webhook` | Shared secret for webhook authorization header. Must be at least 32 characters long. |
| `BSE_SUPPORTS_IDEMPOTENCY`| Boolean | `false` | No | Enable only if the external BSE service honors `Idempotency-Key` headers on `POST /extractions`. |

### Frontend (`client/`)
The React application communicates with the backend using relative `/api/*` routes.
- **In Development:** Proxied by Vite (`client/vite.config.js`) to `http://127.0.0.1:4000`.
- **In Production (Vercel):** Routed via `client/vercel.json` rewrites to the deployed Render backend URL (`https://tradepulse-gok9.onrender.com/api/:path*`).

---

## 10. REST API Specification

### Health & Monitoring
| Method | Path | Description | Response Status |
|---|---|---|---|
| `GET` | `/` | Service identification and links to primary API collections. | `200 OK` |
| `GET` | `/api/health` | Comprehensive health check: checks PostgreSQL connectivity, active jobs, SSE client count, and completion mode. | `200 OK` (or `503 Service Unavailable`) |
| `GET` | `/api/events` | Server-Sent Events (SSE) feed for real-time dashboard invalidations and snapshots. Rotates cleanly at 25 seconds. Accepts `Last-Event-ID` header or `?cursor=` query parameter. | `200 OK` (text/event-stream) |

### Trades & Ledger
| Method | Path | Description | Parameters / Body | Response Status |
|---|---|---|---|---|
| `GET` | `/api/trades` | Retrieves server-paginated trades sorted by `timestamp DESC, trade_id DESC`. Page size is fixed at 30 rows. | `?page=1&symbol=TCS` | `200 OK` |
| `GET` | `/api/trades/:tradeId` | Retrieves a single trade by unique trade identifier. | Path parameter `:tradeId` | `200 OK` (or `404 Not Found`) |
| `GET` | `/api/trades/stats` | Returns aggregated metrics across the entire ledger: total trade count, cumulative INR volume, total buy count, total sell count, and timestamp of the most recent trade. | None | `200 OK` |
| `POST` | `/api/trades` | **Deprecated / Retired endpoint.** Returns `410 Gone`. The ledger accepts external BSE data only; manual entries must use `/api/orders`. | None | `410 Gone` |
| `POST` | `/api/reset` | Clears all records from the trade ledger. Disabled in production (`NODE_ENV=production`) and refused if an ingestion is active. | None | `200 OK` (or `403 Forbidden`) |

### Ingestion & Asynchronous Pulls
| Method | Path | Description | Body / Headers | Response Status |
|---|---|---|---|---|
| `POST` | `/api/pulls` | Initiates an asynchronous BSE extraction pull. Durably persists a job and returns immediately without blocking. *(Alias: `POST /api/bse/import`)* | `Header: Idempotency-Key`<br>`Body: { "delayMs": 900000, "mode": "append", "symbol": "TCS" }` | `202 Accepted` |
| `GET` | `/api/pulls` | Lists the 10 most recent ingestion jobs with status, inserted counts, and timestamps. *(Alias: `GET /api/bse/jobs`)* | None | `200 OK` |
| `GET` | `/api/pulls/:jobId` | Retrieves detailed status, progress counters, and error diagnostics for a specific ingestion job. | Path parameter `:jobId` | `200 OK` (or `404 Not Found`) |
| `POST` | `/api/pulls/:jobId/refresh`| Performs a single-shot status check against the external BSE extraction API. If complete, triggers background batch ingestion. | Path parameter `:jobId` | `200 OK` |
| `POST` | `/api/pulls/:jobId/retry` | Retries a safe failed ingestion job. Blocked if initiation was uncertain without upstream idempotency. | Path parameter `:jobId` | `202 Accepted` |
| `POST` | `/api/pulls/:jobId/reconcile`| Explicitly attaches an existing external BSE job ID to an uncertain initiation failure without re-triggering extraction. | `Body: { "externalJobId": "source-job-uuid" }` | `202 Accepted` |
| `POST` | `/api/bse/notifications` | Authenticated webhook callback receiver invoked by the BSE service upon extraction completion. | `Header: Authorization: Bearer <BSE_WEBHOOK_SECRET>`<br>`Body: { "externalJobId": "...", "status": "completed" }` | `202 Accepted` |
| `GET` | `/api/bse/jobs/active` | Returns the currently active ingestion job, if any. | None | `200 OK` |
| `GET` | `/api/bse/getTrades` | Direct proxy endpoint for short, immediate BSE trade queries. | Query parameters forwarded to BSE | `200 OK` |

### Market Ticks, Orders & Historical Data
| Method | Path | Description | Body | Response Status |
|---|---|---|---|---|
| `GET` | `/api/market` | Returns the latest recorded prices per symbol derived from persisted trades (with fallback to `market_ticks`). | None | `200 OK` |
| `POST` | `/api/orders` | Records a manual order in the segregated `manual_orders` table. | `Body: { "client": "Desk", "symbol": "INFY", "type": "BUY", "quantity": 100, "price": "1450.00" }` | `201 Created` |
| `GET` | `/api/orders` | Retrieves the 30 most recent manual orders. | None | `200 OK` |
| `GET` | `/api/users` | Preserved legacy support endpoint returning user accounts. | None | `200 OK` |
| `GET` | `/api/worker/logs` | Preserved log inspection endpoint returning task execution logs. | None | `200 OK` |
| `POST` | `/api/worker/toggle`<br>`/api/worker/burst` | **Deprecated / Retired endpoints.** Returns `410 Gone`. Internal synthetic workers were removed in favor of real external BSE ingestion. | None | `410 Gone` |

---

## 11. Demonstrating a 15-Minute Extraction Locally

TradePulse is designed to demonstrate that a 15-minute extraction (900,000 ms delay) can run without holding client connections open or crashing proxies.

### Method A: Via the Web Dashboard
1. Open the TradePulse dashboard at **http://localhost:5173**.
2. Scroll to the **Import trades** section (`#import`).
3. Set **Delay** to `15 minutes` (900 seconds) in the dropdown.
4. Select **Mode** (`Append trades` or `Replace all trades`) and optional **Symbol** filter.
5. Click **Start import**.
6. **Observe Immediate Non-Blocking Behavior:**
   - The button transitions immediately to *Import in progress...*.
   - A job link (`Job: <uuid>`) and upstream extraction identifier appear within milliseconds.
   - The elapsed timer begins counting up: `0m 12s elapsed / 15m 0s source delay`.
   - **Verify Zero Blocking:** Click **First**, **Next**, or **Refresh trades** in the trade table; the existing 30-row ledger remains completely responsive. Navigate between pages or submit manual orders in the **New order** panel.
7. **Completing the Job:**
   - When using `webhook` mode: Once the 15-minute timer completes on the BSE service, the webhook fires automatically, the worker stages the records, publishes them, and the dashboard automatically updates via SSE.
   - When using `manual` mode (or for accelerated testing with a shorter delay such as 15s or 30s): Once the delay expires, click **Check source status**. The backend queries the source once, fetches all pages, and commits the records.

### Method B: Via PowerShell / cURL
To initiate the extraction via CLI:

```powershell
# Initiate a 15-minute pull via PowerShell
Invoke-RestMethod -Uri "http://localhost:4000/api/pulls" `
  -Method POST `
  -ContentType "application/json" `
  -Headers @{ "Idempotency-Key" = "demo-15m-pull-$(Get-Random)" } `
  -Body '{"delayMs": 900000, "mode": "append", "symbol": "TCS"}'
```

```bash
# Initiate a 15-minute pull via cURL
curl -X POST http://localhost:4000/api/pulls \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: demo-15m-pull-001" \
  -d '{"delayMs": 900000, "mode": "append", "symbol": "TCS"}'
```

Output received in **<50ms**:
```json
{
  "success": true,
  "jobId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "idempotencyKey": "demo-15m-pull-001",
  "status": "pending",
  "job": {
    "jobId": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "status": "pending",
    "phase": "pending",
    "delayMs": 900000,
    "recordsReceived": 0,
    "insertedCount": 0,
    "duplicatesSkipped": 0
  },
  "message": "Pull accepted..."
}
```

---

## 12. Concurrency, Idempotency & Deduplication

### Concurrency Protection
To prevent duplicate workers or concurrent write races:
1. **Single Active Ingestion Rule:** Enforced at the database engine level via a partial unique index:
   ```sql
   CREATE UNIQUE INDEX one_active_ingestion ON ingestion_jobs ((1)) WHERE status IN ('pending', 'processing');
   ```
   Any concurrent attempt to initiate an extraction while another is active immediately returns `409 Conflict: Ingestion already active`.
2. **PostgreSQL Session Advisory Locks:** When executing extraction retrieval and staging, the background worker acquires an advisory lock (`SELECT pg_try_advisory_lock(hashtextextended('ingestion:<jobId>', 0))`). If another process is servicing the job, the second worker yields safely.

### Request Idempotency
- Every `POST /api/pulls` request accepts an `Idempotency-Key` header (or body property).
- Requests acquire a transactional lock (`tradepulse-ingestion-control`) and check `ingestion_jobs`.
- If a job with the identical idempotency key exists and the payload matches, the existing job is returned without re-executing. If the payload differs, `409 Conflict` is returned.

### Upstream Idempotency & Safe Initiation Recovery
- Initiating an external extraction (`POST /extractions`) over an unreliable network can fail before an external ID is recorded.
- If `BSE_SUPPORTS_IDEMPOTENCY` is `false`, TradePulse **never automatically retries** extraction creation to prevent orphan duplicate extractions at the exchange.
- Instead, the job marks `external_attempted=true` and transitions to `failed`. An operator can inspect the external exchange and call `POST /api/pulls/:jobId/reconcile` with `{ "externalJobId": "known-bse-id" }` to bind the original extraction and resume retrieval safely.

### Checkpoint Staging & Resilient Pagination
- Results are retrieved in cursor-driven batches of 500 records.
- Each batch is written to `ingestion_staging` and committed alongside cursor state in `ingestion_batches`.
- If the worker process is interrupted or restarted mid-stream, `worker.recover()` runs on startup, reads `next_cursor` from `ingestion_jobs`, and resumes ingestion without re-reading previously processed batches.

### Ledger Deduplication vs. Conflict Hard Errors
When all batches are staged, the publication transaction executes:
```sql
INSERT INTO trades (trade_id, client, symbol, quantity, price, timestamp, side)
SELECT trade_id, client, symbol, quantity, price, timestamp, side
FROM ingestion_staging
WHERE job_id = $1
ON CONFLICT (trade_id) DO NOTHING;
```
- **Identical Duplicates:** If an incoming trade has the exact same `trade_id`, `client`, `symbol`, `quantity`, `price`, `timestamp`, and `side`, it is safely ignored and counted in `duplicates_skipped`.
- **Conflicting Records:** Before publication, the worker executes a conflict detection query. If an incoming trade shares an existing `trade_id` but contains different financial attributes, the transaction aborts with:
  ```
  Existing trade_id has conflicting external data
  ```
  This guarantees that corrupt exchange data cannot silently overwrite immutable audit ledger entries.

---

## 13. Server-Sent Events (SSE) & Durable Sync

### Stream Architecture
TradePulse uses Server-Sent Events (`GET /api/events`) for server-to-client notifications rather than heavy polling loops.

```
PostgreSQL Transaction COMMIT
       │
       ▼
pg_notify('tradepulse_events', { eventId })
       │
       ▼
EventHub pg.Client listener
       │
       ▼
EventHub.drain(clientState)
       │
       ▼
HTTP chunk: "event: import_completed\ndata: { ... }\n\n"
```

### Key SSE Characteristics
1. **Lightweight Metadata Payloads:** SSE events never deliver arrays of trade rows. Payloads contain only event types, job IDs, and record count differentials. Clients react by querying their specific active view.
2. **25-Second Intentional Connection Rotation:**
   - Standard cloud proxies and browsers close inactive streaming sockets after 30 seconds.
   - TradePulse proactively closes the stream after **25 seconds** by emitting an `event: reconnect` frame.
   - The browser client's `useTradePulseSSE` hook detects this cleanly, waits 1,000 ms, and reconnects passing its last known event cursor (`Last-Event-ID`).
3. **Durable Event Replay:**
   - All system events are permanently written to the `app_events` table before `NOTIFY` is emitted.
   - When a reconnected client supplies a cursor (e.g. `?cursor=142`), `EventHub` queries `SELECT * FROM app_events WHERE event_id > 142` and replays all missed events in strict order before switching to live mode.
4. **Transport Keepalives:** Comment heartbeats (`: heartbeat\n\n`) are transmitted every 10 seconds to maintain connection health across proxies that disable buffering.

---

## 14. Automated Testing & Verification

The repository includes a comprehensive test suite built on the native Node.js test runner (`node:test`). Tests create and drop unique, isolated PostgreSQL schemas, guaranteeing that application data is never touched or truncated.

### Running Unit Tests
```bash
node --test server/test/unit.test.js
```
Verified passing test cases:
- Trade validation preserves exact decimal prices and microsecond timestamps.
- Configuration enforces dedicated database naming and verified TLS for cloud environments.
- External adapter strictly follows the BSE extraction contract.
- Webhook registration correlates jobs without leaking shared secrets in metadata.
- Webhook configuration validates secret length (>=32 characters) and production HTTPS.
- Unsafe extraction creation is never retried automatically without idempotency confirmation.

### Running Integration Tests
```bash
npm test
```
*Note: Requires a running PostgreSQL instance with permissions to create temporary schemas.*

To test against a live, running BSE mock service:
```bash
# Bash
TEST_LIVE_BSE=true npm test

# PowerShell
$env:TEST_LIVE_BSE="true"; npm test
```

### Running Frontend Linter & Build
```bash
npm --prefix client run lint
npm --prefix client run build
```

---

## 15. Troubleshooting & FAQ

### 1. `DATABASE_URL must select a dedicated TradePulse database`
- **Cause:** `server/src/config/env.js` validates that your database name matches `tradepulse` or `^tradepulse_db(?:_[a-z0-9]+)?$`.
- **Fix:** Ensure the database portion of your connection string is named `tradepulse` or starts with `tradepulse_db` (e.g., `postgresql://postgres:password@localhost:5432/tradepulse`).

### 2. `Cloud PostgreSQL requires verified TLS`
- **Cause:** Non-localhost hosts (such as Render, Supabase, Neon, AWS RDS) require verified TLS. In `server/.env`, `PG_SSL_MODE` is set to `disable`.
- **Fix:** Set `PG_SSL_MODE=verify-full` in `server/.env`. If your provider uses a self-signed root certificate, provide its path in `PG_SSL_CA_FILE`.

### 3. `BSE API returned HTTP 502` / `Request deadline exceeded (HTTP 504)`
- **Cause:** The TradePulse backend cannot reach the external BSE service at `BSE_API_BASE_URL`, the service is sleeping (e.g., Render free tier cold start), or a query exceeded the 20-second request deadline.
- **Fix:** Verify the BSE service is online by running `curl http://localhost:4001/` or your remote BSE URL. For cloud instances on Render, trigger a manual GET request to wake the instance before running extractions.

### 4. `Webhook mode requires BSE_WEBHOOK_SECRET and BSE_CALLBACK_URL`
- **Cause:** `BSE_COMPLETION_MODE=webhook` is enabled, but the required secret or callback URL is missing, or the secret is shorter than 32 characters.
- **Fix:** Supply a 32+ character random string for `BSE_WEBHOOK_SECRET` and set `BSE_CALLBACK_URL` to your publicly reachable endpoint (e.g., `https://tradepulse-gok9.onrender.com/api/bse/notifications`).

### 5. SSE Disconnections or Missing Live Updates
- **Cause:** An intermediate reverse proxy (Nginx, Cloudflare, AWS ALB) is buffering response chunks or cutting long-lived HTTP streams.
- **Fix:** TradePulse sets `X-Accel-Buffering: no` and rotates connections every 25 seconds. Ensure upstream proxies have proxy buffering disabled for `/api/events` and timeout limits configured above 30 seconds.

---

## 16. Current Limitations & Future Roadmap

To maintain strict technical accuracy, implemented capabilities are separated from current design boundaries:

### Current Architecture Limitations
- **Single Active Pull:** The system allows only one extraction job in `pending` or `processing` state at any given moment across the entire system.
- **Fixed Page Size:** The REST API and frontend table operate on a fixed page size of 30 records.
- **Segregated Manual Orders:** Manual orders entered in the UI are recorded to `manual_orders` for testing; they do not route to an external exchange order book or matching engine.
- **Single Operator Model:** The current dashboard does not implement multi-tenant authentication or Role-Based Access Control (RBAC).

### Planned Roadmap
- **Partitioned Ingestion:** Support parallel concurrent extractions partitioned by market symbol or asset class.
- **WebSocket Gateway:** Provide an optional bidirectional WebSocket transport alongside SSE for high-frequency order placement.
- **Kafka / RabbitMQ Outbox Integration:** Introduce an external distributed queue for multi-node ingestion workers.
- **Multi-Tenant RBAC:** Add authentication (OAuth2 / JWT) with role segregation between desk traders, risk managers, and system auditors.

---

## 17. License & Academic Attribution

This project was developed as a technical portfolio project and academic evaluation assignment demonstrating production-grade distributed systems design, financial ledger ingestion, and real-time streaming architectures.

All rights reserved. Proprietary and confidential.
