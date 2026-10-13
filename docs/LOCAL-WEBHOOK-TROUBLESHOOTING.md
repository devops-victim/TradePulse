# Local webhook and SSE troubleshooting

## Extraction fails with HTTP 400

If the error includes `INVALID_CALLBACK_URL`, verify the independently hosted BSE service's `.env` contains:

```env
BSE_CALLBACK_ALLOWLIST=http://localhost:4000/api/bse/notifications
```

An explicitly empty allowlist overrides the development default and rejects every callback. Restart BSE after editing its `.env`. Keep the same webhook secret on both services, without committing either file. For cloud deployment, approve the exact public HTTPS TradePulse callback instead of localhost.

TradePulse's local settings should include:

```env
BSE_COMPLETION_MODE=webhook
BSE_CALLBACK_URL=http://localhost:4000/api/bse/notifications
```

Restart TradePulse after changing its environment. A failed extraction request remains a failed historical job; start a new pull after fixing configuration. The client now reports the safe upstream error code and a callback-allowlist hint instead of only an HTTP status.

## Repeated events?cursor requests fail

These requests reconnect the SSE stream; they do not initiate new extraction jobs. Verify `http://localhost:4000/api/health` is online and start the backend with `npm run server`. Start Vite with `npm run client` in a separate terminal.

Healthy streams intentionally rotate after 25 seconds and reconnect in one second, before a 30-second network cutoff. Failed connections now back off through 1, 2, 4, 8, 16 and 30 seconds. Successful connection resets the backoff. The cursor preserves missed event recovery. No trade/status polling is introduced.

## Tests with cloud PostgreSQL

The test setup pool uses the same verified SSL configuration as the application. Integration fixtures live in a unique temporary schema. Acceptance tests withhold the source receipt explicitly rather than assuming a local database can respond within 800 ms; WAN database latency must not be mistaken for waiting on extraction.
