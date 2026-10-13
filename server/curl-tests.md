# API checks

Start PostgreSQL, the independently hosted BSE service on 4001, and TradePulse on 4000. On PowerShell use curl.exe.

```sh
curl http://localhost:4000/api/health
curl 'http://localhost:4000/api/trades?page=1'
curl 'http://localhost:4000/api/trades?page=2&symbol=TCS'
curl http://localhost:4000/api/trades/BSE-TRD-000001
curl -N http://localhost:4000/api/events
curl -X POST http://localhost:4000/api/pulls -H 'Content-Type: application/json' -H 'Idempotency-Key: example-1' -d '{"delayMs":900000,"mode":"append"}'
curl http://localhost:4000/api/pulls/JOB_UUID
# Only when explicitly requested by the user; never schedule this call.
curl -X POST http://localhost:4000/api/pulls/JOB_UUID/refresh
curl -X POST http://localhost:4000/api/pulls/JOB_UUID/retry
curl http://localhost:4000/api/orders
```

Automatic source completion depends on the independent BSE service implementing the webhook contract documented in README.md. Existing support routes are aliases; manual orders use /api/orders, not /api/trades.
