# Verification record

Environment: Windows, Node.js 24, PostgreSQL 18. Dedicated local database tradepulse; independent BSE service at localhost:4001. An isolated PostgreSQL instance on loopback port 54329 was used for disposable integration schemas.

Verified:

- PostgreSQL connects; both numbered migrations apply and rerun safely.
- The original SQLite database remains untouched, with a consistent backup/export. All 10,000 trades and 10,031 source records were migrated/archived in the dedicated PostgreSQL database.
- PostgreSQL constraints, fixed 30-row pages (30/30/5 in the fixture), trade detail lookup and symbol ordering.
- External extraction requests use the actual source contract; a live extraction retrieves the independently hosted service's full dataset through bounded result pages.
- Acceptance is returned before a delayed source receipt; concurrent callers and repeated idempotency keys do not start duplicate workers.
- Existing trades remain readable after source failures. Conflicting IDs fail without overwriting stored values. Manual orders remain outside the trade ledger.
- Waiting jobs and committed retrieval checkpoints recover through a new worker instance.
- Two SSE clients observe the same committed completion event; trades are readable before notification delivery. Durable replay recovers missed events.
- SSE closes at 25 seconds and can reconnect. The frontend reacts to events; its only recurring UI timer updates elapsed display, never fetches data.
- All 14 tests pass, including the optional live BSE case and the private legacy export case. Frontend production build and lint run successfully.
- A full field-by-field comparison matched all 10,000 migrated trades and all 10,031 archived records. Backend startup/health and a 30-row REST page were verified on port 4000.

Run npm test for the built-in Node test suite. Set TEST_LIVE_BSE=true to include the external-service test. Tests use unique temporary schemas and do not truncate application tables. The private legacy-export test is skipped on fresh clones without that export.

Limits: no full 15-minute timed run was performed. The actual BSE service does not yet publish completion notifications or support idempotent extraction creation. Automatic source-driven completion cannot be tested against that service until it provides those capabilities. Event-driven downstream ingestion/SSE is tested with injected source adapters; a user-triggered one-shot status check verifies the live source path. No production mock service or polling fallback was added.
