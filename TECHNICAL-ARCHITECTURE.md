# TradePulse architecture

```mermaid
sequenceDiagram
  participant UI as React dashboard
  participant API as TradePulse API :4000
  participant PG as PostgreSQL tradepulse
  participant Worker as Ingestion worker
  participant BSE as Independent BSE API :4001
  UI->>API: GET /api/trades?page=N
  API->>PG: Read committed page (30 rows)
  API-->>UI: Existing trades immediately
  UI->>API: POST /api/pulls + idempotency key
  API->>PG: Create durable job + event
  API-->>UI: 202 Accepted
  Worker->>PG: Acquire job lock
  Worker->>BSE: POST /extractions {delay:900000}
  BSE-->>Worker: Short receipt + external job ID
  Worker->>PG: Persist ID, release lock while waiting
  Note over BSE: Processing may take 15 minutes; no TradePulse HTTP connection waits
  alt Future source notification adapter
    BSE->>API: Authenticated completion webhook
    API->>PG: Persist ready state + event
    API-->>BSE: 202
  else Current source has no completion feed
    UI->>API: Explicit Check source status action
    API->>BSE: One GET /extractions/:id
    BSE-->>API: Completed metadata
    API->>PG: Persist ready state + event
  end
  Worker->>BSE: GET result pages with bounded limit/cursor
  Worker->>PG: Stage batches + checkpoints
  Worker->>PG: Atomically publish trades, job and durable event
  PG-->>API: NOTIFY after commit
  API-->>UI: Lightweight SSE completion
  UI->>API: Read current page again
  API-->>UI: Updated rows, same page
```

The API, external source and database are independently configured. Runtime uses pg.Pool and parameterized repositories. There is no in-app mock exchange or source data generation. Pending work survives restarts through ingestion_jobs, ingestion_batches and ingestion_staging. Worker execution is asynchronous I/O in the existing backend process, protected across processes by database advisory locks; CPU simulation threads were removed.

A unique partial index serializes active pulls. Batch checkpoints commit with staging data. Publication commits trades, counters and app_events together. A conflicting duplicate fails rather than overwriting financial data. Extraction initiation cannot be safely retried after an uncertain response unless the source honors idempotency; current-source recovery requires reconciling its original ID.

PostgreSQL LISTEN/NOTIFY signals each API instance to read its durable event log. It is not used as the event store. Replay is ordered by committed event IDs. SSE closes at 25 seconds and reconnects; a snapshot and cursor restore state after a gap. Neither frontend nor backend runs a periodic source-status query.

The independent API currently has no completion transport. Explicit source-status checks are a disclosed interim mode, not automatic completion. A webhook interface exists behind IngestionService; enabling it requires the source to implement registration, authentication and notification delivery. An unchanged 15-minute synchronous GET cannot survive the universal 30-second cutoff.

Manual orders are separate from the externally ingested ledger. Legacy data is preserved with provenance and a lossless archive. Cloud deployment requires verified TLS and direct/session-mode PostgreSQL connections, not transaction pooling.
