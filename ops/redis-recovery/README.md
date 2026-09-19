# Redis concurrency recovery validation

This lab uses its own database, Redis, simulated Anthropic upstream, and internal Docker network. Its credentials are lab-only. Do not attach it to a production network or database; `/lab/pool/hold` deliberately exhausts the proxy's Redis connection pool

## Reproduced failure

With the previous image, eight streaming requests completed with HTTP 200 while the Redis connection pool was held for 25 seconds. Eight concurrency slots remained after all requests ended. During the 60-second Redis circuit-breaker interval requests could use the local fallback; after that interval all five new probes returned HTTP 429 despite no active upstream requests

The ordinary pool default was retained: 50 connections, with one reserved by pub/sub and 49 held by the fault probe. This demonstrates a recovery defect without changing the configured key concurrency of eight. It does not establish what first exhausted the production connection pool

## Fix

Stream finalization releases concurrency independently of the best-effort spend logging worker. Failed or cancelled Redis releases are retried by one per-handler task, with the original slot IDs, bounded attempts, batches, and a one-hour expiry matching the slot TTL. Redis admission consults Redis rather than rejecting from a stale local mirror. Pending releases for a key are also flushed before its next Redis admission

The queue is process-local. A hard process crash still relies on the existing Redis slot TTL. The existing local fallback policy during Redis outages remains; this patch does not promise strict cluster-wide limits during a network partition

## Run

The baseline image `litellm-local:1.99.1-no-shadow-pr40387-pr40843-504-timing-d1bd21e0` must already exist locally

```sh
docker build -f docker/Dockerfile.no-shadow-eval-overlay -t litellm-local:1.99.1-redis-recovery-candidate .
docker compose -p litellm-redis-recovery-test -f ops/redis-recovery/compose.yml -f ops/redis-recovery/fixed.yml up -d
docker compose -p litellm-redis-recovery-test -f ops/redis-recovery/compose.yml -f ops/redis-recovery/fixed.yml exec -T -e EXPECT_RECOVERY=1 proxy python /lab/http_probe.py
docker compose -p litellm-redis-recovery-test -f ops/redis-recovery/compose.yml -f ops/redis-recovery/fixed.yml exec -T proxy python /lab/traffic_matrix.py
```

Run the same compose configuration without `fixed.yml` to reproduce the baseline failure. Omit `EXPECT_RECOVERY=1` for that baseline run. Scripts create fresh virtual keys in the isolated database and never use production credentials

## Observed results

The final candidate passed 147 limiter tests (one Redis-environment-dependent test skipped) and 48 streaming cleanup/disconnect tests. The latter ran with process isolation because the existing suite shares callback state across tests; the order-dependent failure reproduced independently of this patch. Ruff checks and formatting checks passed, with no increase in the touched modules' strict-Ruff warning counts

| Scenario | Previous image | Fixed candidate |
| --- | --- | --- |
| Normal sequential streaming | 12 HTTP 200 | 12 HTTP 200 |
| Eight streams ending during pool exhaustion | Eight leaked Redis slots | Deferred releases clear the eight slots |
| Five probes after breaker recovery | Five HTTP 429 | Five HTTP 200 |
| 640 calls, 64 concurrent, mixed streaming/nonstreaming | Not run | 640 HTTP 200, zero remaining slots |
| Nine concurrent calls with a limit of eight | Not run | Eight HTTP 200, one HTTP 429 |
| Twelve client cancellations | Not run | Zero remaining slots |
| Four upstream 504s, then eight new requests | Not run | Four HTTP 504, then eight HTTP 200; zero remaining slots |

During the injected outage, requests with a two-second simulated upstream took about 7 to 8.3 seconds to finish, including Redis cleanup waits. Under normal conditions the recovery probes completed in about 0.12 seconds. Request bodies and upstream behavior are synthetic, so production-sized prompt and token-accounting performance is not certified by this lab
