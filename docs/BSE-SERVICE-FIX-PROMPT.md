# Prompt for the independent BSE API service

Paste the following into the coding agent working in the **BSE repository**, not TradePulse.

---

Fix the existing BSE Exchange API's missing event-driven extraction completion integration with TradePulse. Inspect the repository, README, configuration, migrations, tests and extraction worker before editing. Refactor incrementally; do not rebuild the service, create another exchange API, connect to TradePulse's database, change existing trade IDs, or reseed/delete its 10,000 records.

The current service at http://localhost:4001 accepts POST /extractions with delay/symbol/from/to, returns a job ID immediately, then exposes status and cursor results. It currently rejects callbackUrl/clientJobId and never sends completion notifications. This leaves TradePulse waiting indefinitely. Preserve GET /getTrades, GET /extractions/:jobId, GET /extractions/:jobId/results and their existing response shapes, pagination and 409 EXTRACTION_NOT_READY behavior.

## Required wire contract

Extend POST /extractions to accept optional callback registration:

```http
POST /extractions
Idempotency-Key: <TradePulse request key>
Content-Type: application/json

{
  "delay": 900000,
  "symbol": "TCS",
  "clientJobId": "TRADEPULSE_JOB_UUID",
  "callbackUrl": "http://localhost:4000/api/bse/notifications"
}
```

symbol/from/to remain optional existing filters. callbackUrl/clientJobId are registration metadata, not filters. If registration is requested, require both fields; validate the client UUID and URL. Existing calls without callback registration must continue to work. Return the existing short 202 receipt with jobId; do not hold this request open during extraction or delivery.

After the extraction's terminal status and completed results are committed, send:

```http
POST <stored callbackUrl>
Authorization: Bearer <BSE_WEBHOOK_SECRET>
Content-Type: application/json

{
  "externalJobId": "BSE_EXTRACTION_JOB_ID",
  "clientJobId": "TRADEPULSE_JOB_UUID",
  "status": "completed"
}
```

Use status failed for terminal extraction failures. Send identifiers/status only, not trade arrays. The TradePulse receiver responds 202 after durable acceptance and ingests results in the background. Do not wait for TradePulse ingestion to finish. Repeated notifications must use the same job correlation. Never transmit the secret in extraction registration, responses, URLs or logs. Configure the same random secret (at least 32 characters) independently on both services.

## Durable event delivery

- Add versioned PostgreSQL migrations for persisted callback registration and an outbox with delivery state, attempts, next attempt time, bounded diagnostic error and a unique extraction/terminal-event identity.
- Commit terminal extraction status and its outbox row in one transaction. Dispatch only after commit. Delivery failure must not change a completed extraction to failed or make results unavailable.
- Trigger delivery from the committed completion event. Use bounded HTTP deadlines below 30 seconds (for example 10 seconds), a limited response body, and no redirect following. A 2xx response acknowledges delivery. Retry disconnects/timeouts/429/5xx with bounded exponential backoff using one-shot timers tied to this delivery. Respect bounded Retry-After values. Keep exhausted/permanent failures durably visible and provide explicit redelivery.
- At startup, scan unfinished extraction and outbox state once and re-arm their one-shot timers. Use locks/atomic claims to prevent concurrent workers from duplicating state transitions. At-least-once delivery is expected; the receiver deduplicates notifications.
- Do not add periodic extraction-status polling, setInterval-based queue scans, cron jobs, frontend polling, or repeated 27-second fetches. Delivery retries of a known notification are distinct from polling for completion.
- Prevent arbitrary callback destinations: use an environment-configured exact callback origin/path allowlist. Permit the local TradePulse callback explicitly in development. Require HTTPS in cloud production; reject credentials/fragments and unauthorized hosts/ports/paths. Validate stored destinations on dispatch as well. Use a configured shared secret for the approved TradePulse recipient.

## Safe extraction submission retries

Implement durable Idempotency-Key support for POST /extractions: same key and equivalent filters/delay/callback metadata returns the original job ID; conflicting reuse returns 409. Make concurrent requests safe with a database constraint/transaction. Without a key, preserve existing semantics. This prevents duplicate extractions after ambiguous network acceptance. Do not advertise support until tests pass. TradePulse enables its BSE_SUPPORTS_IDEMPOTENCY flag separately afterward.

## Verification and delivery

Use isolated fixtures/schemas. Test existing requests, callback registration validation, prompt 202, delayed responsiveness, early results 409, paginated results, actual authenticated HTTP callback, completed and failed delivery, duplicate deliveries, callback arriving before the client receives the extraction receipt, HTTP timeout/retry, receiver downtime, source restart with pending delivery, rejected callback destinations and idempotency races. Verify source trades remain unchanged. Do not claim a 15-minute test if only a short delay was executed; include one extraction exceeding 30 seconds.

Update README/.env.example with secret/allowlist/deployment configuration, callback workflow, migrations, retry/recovery and tests. Run tests and report changed files and remaining limitations. Do not change TradePulse code or its database.

Once BSE supports the contract, configure TradePulse:

```env
BSE_COMPLETION_MODE=webhook
BSE_CALLBACK_URL=http://localhost:4000/api/bse/notifications
BSE_WEBHOOK_SECRET=<same random secret configured on BSE; at least 32 characters>
# Set true only after the source's durable idempotency implementation is verified.
BSE_SUPPORTS_IDEMPOTENCY=true
```

For cloud hosting, use the publicly reachable HTTPS TradePulse backend callback URL. localhost only works when both services share the same machine/network namespace. Restart both services after configuration and migrations. Verify a pull completes and the open dashboard updates without calling /refresh or reloading the page.

---

TradePulse-side changes and automated fixtures demonstrate the receiver and HTTP adapter contract. They do not add this missing publisher to the independently hosted BSE service.
