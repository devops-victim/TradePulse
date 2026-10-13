# TradePulse current requirements

This repository now implements the PostgreSQL migration and external BSE integration requested on 13 October 2026. The earlier SQLite/internal-mock design is superseded.

- Dedicated PostgreSQL tradepulse database, pooled pg access, versioned migrations and parameterized SQL.
- Preserve inspected legacy data and document backups; never delete source database files automatically.
- Use the independently hosted BSE extraction API for every new ingested trade.
- Accept pulls promptly, persist external IDs/checkpoints, prevent concurrent duplicate workers and recover interrupted retrieval.
- Publish committed lightweight SSE events, replay missed events from PostgreSQL and rotate connections before the 30-second cutoff.
- Preserve the React dashboard and its 30-row server pagination, existing rows during ingestion and automatic current-page updates after committed events.
- No internally hosted BSE API, synthetic production trade seeding, frontend polling loop, cron or periodic ingestion scheduler.
- Expose missing external completion notifications clearly. Current manual checks are explicitly user-driven; automatic arrival requires the source's webhook/event contract.
- Preserve manual-entry functionality as separate orders so local entries cannot masquerade as external exchange trades.

Actual configuration, endpoint contract, verification and remaining dependencies are described in README.md, TECHNICAL-ARCHITECTURE.md, docs/MIGRATION.md and DELIVERY-AND-VERIFICATION.md.
