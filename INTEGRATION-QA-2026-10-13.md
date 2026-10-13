# TradePulse local integration audit — 2026-10-13

**Verdict: does not yet meet the automatic completion requirement.** The exchange completes extraction, but TradePulse remains waiting until an explicit manual status refresh. PostgreSQL persistence, validation, cursor ingestion, deduplication and backend SSE worked after that refresh. Browser checks are blocked by a saved browser access denial.

## Architecture discovered

Inspected both repositories and their READMEs before integration tests:

- TradePulse: `C:\Users\Manav Sonawane\Desktop\TradePulse`.
- Exchange: `C:\Users\Manav Sonawane\Desktop\Assignment\tradepulse-database`.
- Exchange is running on port 4001 with PostgreSQL database **bse_trades**. TradePulse runs on port 4000 with database **tradepulse**, and Vite on 5173. The requested names `bse_exchange_db` and `tradepulse_db` do not exist locally.
- TradePulse uses its own `pg.Pool`, durable jobs, ingestion staging/checkpoints, persistent events and PostgreSQL LISTEN/NOTIFY. Its BseClient accesses the exchange over HTTP. Observed TradePulse application database connections all used `tradepulse`.
- Exchange accepts `POST /extractions` with `{delay, symbol?, from?, to?}` and returns a job ID promptly. Status is at `GET /extractions/:id`; results use `GET /extractions/:id/results?limit=&cursor=`. It snapshots source rows at submission and uses a one-shot timer to commit completion after the configured delay.
- TradePulse creates a durable job through `POST /api/pulls`, then submits extraction asynchronously. The worker ingests cursor pages of 500 into staging, validates rows, and publishes to the ledger transactionally with trade-ID deduplication.
- Current completion mode is **manual**. `POST /api/pulls/:jobId/refresh` explicitly checks source status. An authenticated `/api/bse/notifications` receiver exists, but its secret/callback configuration is absent, and the source has no outward completion publisher.
- Backend SSE uses lightweight durable event IDs and record counts, with connections rotated before 30 seconds. React source refreshes data on events and retains the current page; no fetch polling loop or cron was found in the inspected code. Runtime browser behavior remains unverified.

## Executed tests

PASS applies only to the scope explicitly described below. Planned one-off status assertions were test observations, not an application polling mechanism.

| Test | Result | Evidence |
|---|---|---|
| Service/database health | PASS | Both backend health endpoints responded; independent read-only SQL connections succeeded. |
| Exchange lifecycle and delay | PASS | 3-second extraction accepted with 202 in 216 ms; receipt pending, immediate status processing, completed after 3,016 ms. |
| Premature results | PASS | Results returned 409 `EXTRACTION_NOT_READY` while extraction was processing. |
| Exchange cursor pagination | PASS | Ten 1,000-row pages yielded 10,000 valid unique trades, zero duplicates. |
| Long extraction without blocked HTTP | PASS | TradePulse accepted a 31-second pull with 202 in 75 ms; exchange completed after 31,046 ms. A concurrent ledger read returned 30 rows in 7 ms. |
| Automatic source completion | **FAIL** | Exchange completed, but TradePulse stayed `processing / waiting`, received zero records and ledger remained at 10,000 until manual refresh. |
| Explicit refresh and complete ingestion | PASS | Refresh returned 200 in 11 ms. Twenty 500-row checkpoint batches ingested 10,000 trades and completed the job. |
| Field integrity and original data preservation | PASS | All 10,000 source trades matched ID, client, symbol, quantity, price and timestamp at microsecond precision. Original legacy core-row hash remained unchanged. |
| Repeated request and concurrent pull | PASS | Same idempotency key returned the same job; a competing key while active returned 409. |
| Re-ingestion deduplication | PASS | Second completed pull received 10,000, inserted zero, skipped 10,000 duplicates; ledger hash unchanged. |
| Backend SSE fanout | PASS | Two HTTP SSE consumers received completion event ID 25, each with affected count 10,000 and no full trade array. |
| Backend SSE reconnect | PASS | Streams rotated after 25,086 / 25,070 ms; consumers reconnected with their durable cursors and received completion. Browser reconnection was not tested. |
| REST pagination and ordering | PASS | Pages 1 / 2 / 667 returned 30 / 30 / 20 rows, total 20,000; rereading page 1 returned identical ordered data. |
| Safe recovery path | PASS, limited | A fresh worker process invoked existing recovery, retained the same job/extraction IDs and waiting state. The running service was not restarted. |
| Request deadline and responsiveness | PASS | Synchronous proxy GET with delay 31,000 returned 504 after 20,047 ms; concurrent ledger read completed in 155 ms and backend stayed healthy. |
| Validation/authentication | PASS | Invalid delays returned 400; unconfigured/unauthenticated webhook returned 403; unknown symbol produced an empty successful REST page. |
| Isolated client failures | PASS, limited | Real refused loopback connection rejected in 19 ms; malformed negative-quantity response was rejected in an isolated client invocation. No live failed ingestion job was injected. |
| Existing automated unit tests | PASS | Four TradePulse unit tests and four exchange validation/error tests executed successfully. |
| React rendering, navigation, console/network and automatic updates | BLOCKED | Browser tool denied localhost:5173 because of a saved access preference, including after chat approval. No alternate browser bypass attempted. |
| Full server restart, live upstream outage/invalid response, PostgreSQL outage and failed-job state | BLOCKED | Not executed: would require controlled service/config/data intervention beyond this audit's safe scope. |
| New automated integration fixtures | BLOCKED | No new/extended test files created; schema-creating suites were not run without approval. |

## Database verification

| Database | Before | Expected after append | Actual after tests | Unique IDs |
|---|---:|---:|---:|---:|
| bse_trades | 10,000 | 10,000 | 10,000 | 10,000 |
| tradepulse | 10,000 | 20,000 | 20,000 | 20,000 |

The initial TradePulse records were `legacy_import`, with IDs distinct from the current exchange dataset. There were zero overlapping IDs before ingestion, so 20,000 is the correct append-mode result. Final provenance: 10,000 legacy and 10,000 external BSE records. Both audit ingestion jobs are completed; zero unfinished jobs remained at the final SQL check. Original records were neither deleted nor overwritten.

## Network evidence and job identifiers

- Exchange standalone lifecycle job: `a53d1104-a015-4419-9ea0-d1dd60ed3191`.
- First TradePulse job: `3bf70e7a-9e43-46af-af6c-7647ec999fea`; upstream: `2f40baac-053e-4a70-8c84-e21867136e0e`.
- Deduplication TradePulse job: `9aad2480-216e-427f-a8ce-0127010743ce`; upstream: `960008a3-d066-459b-a4e2-8016300b78f5`.
- Inspection and actual resulting external IDs confirm HTTP extraction submission. Source result-page execution was verified by complete ingestion/checkpoint counts and cross-database field comparison; no browser developer-tools trace was available.
- `POST /extractions` with `callbackUrl` returned **400 Unknown parameter callbackUrl** in 49 ms. Source `GET /events` returned 404. Enabling TradePulse webhook mode alone is therefore insufficient.

## Bugs, dependencies and minimal recommended fixes

1. **High — missing event-driven completion integration.** Exchange `src/services/extractions.js` commits completion but does not publish it externally. Its `src/controllers/index.js` rejects callback registration fields. TradePulse `server/src/services/ingestionService.js` has a receiver/manual refresh, while `server/src/clients/bseClient.js` sends unsupported callback fields in webhook mode. Add an agreed authenticated webhook contract to the independently hosted exchange, including job correlation and durable delivery/retry after committed completion, then configure the existing TradePulse receiver. Delivery retries must not become extraction-status polling. **Requires approval; not implemented.**
2. **Medium — deployment/database naming mismatch.** Actual names differ from the test brief. `server/src/config/env.js` explicitly rejects `tradepulse_db` outside tests. Agree on database naming and update documentation/config validation if needed; do not rename populated databases during QA.
3. **Recovery dependency — upstream idempotency.** Local request deduplication passed, but the source has no demonstrated idempotent extraction submission contract. Agree on upstream idempotency/correlation before enabling submission retries after uncertain acceptance. Full interruption and failure-job testing remains outstanding.

No application code, environment settings, schema or permanent test files were changed. Normal requested API tests retained three exchange extraction jobs and two TradePulse ingestion jobs and appended the 10,000 external trades. No unrelated process was terminated. This report is the only repository file added by the audit.

The system demonstrates working HTTP ingestion and PostgreSQL/SSE delivery after explicit completion discovery. It does **not** currently satisfy automatic dashboard arrival after a long extraction without manual intervention. Browser verification, full restart/failure tests and cloud callback reachability remain necessary after the completion integration is approved and implemented.

## Follow-up implementation (authorized after this audit)

TradePulse's receiver now rejects mismatched job correlation, validates notification bodies, and acknowledges repeated terminal source failures without duplicate events. Webhook configuration validates secret length and production HTTPS. Tests cover early callbacks, duplicate completion/failure delivery and an authenticated callback across an actual test HTTP boundary that triggers bounded ingestion without manual refresh or readiness polling. The complete backend suite passed 17 tests; frontend lint/build passed. Tests use a unique temporary PostgreSQL schema and leave application data untouched.

See [the independent BSE service implementation prompt](docs/BSE-SERVICE-FIX-PROMPT.md). The external publisher is still a deployment dependency. Local `.env` was not changed and webhook mode must only be activated after BSE implements registration and authenticated delivery. The original table above records the initial live audit, not a claim that the unchanged source now publishes notifications.
