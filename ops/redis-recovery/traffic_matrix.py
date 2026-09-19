# ruff: noqa: T201  # diagnostic CLI prints structured verification evidence
import asyncio
import collections
import hashlib
import json
import time
import uuid

import httpx


async def main():
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:4000", timeout=45, limits=httpx.Limits(max_connections=150)
    ) as client:
        keys = []
        for i in range(16):
            r = await client.post(
                "/key/generate",
                headers={"Authorization": "Bearer sk-redis-lab-only"},
                json={
                    "models": ["lab-model"],
                    "max_parallel_requests": 8,
                    "key_alias": "matrix-" + uuid.uuid4().hex[:10],
                },
            )
            r.raise_for_status()
            keys.append(r.json()["key"])

        async def request(key, text="ok", stream=True):
            r = await client.post(
                "/v1/messages",
                headers={"Authorization": "Bearer " + key},
                json={
                    "model": "lab-model",
                    "max_tokens": 32,
                    "stream": stream,
                    "messages": [{"role": "user", "content": text}],
                },
            )
            return r.status_code

        async def check_slots(label):
            gauge_keys = {
                "{api_key:" + hashlib.sha256(key.encode()).hexdigest() + "}:max_parallel_requests" for key in keys
            }
            for _ in range(100):
                state = (await client.get("/lab/state")).json()
                remaining = {k: v for k, v in state["gauges"].items() if k in gauge_keys and v}
                if not remaining:
                    print(json.dumps({"phase": label, "remaining_slots": 0}), flush=True)
                    return
                await asyncio.sleep(0.1)
            raise AssertionError((label, remaining))

        start = time.monotonic()
        results = []
        for wave in range(10):
            results.extend(
                await asyncio.gather(*(request(key, stream=(wave % 2 == 0)) for key in keys for _ in range(4)))
            )
        print(
            json.dumps(
                {
                    "phase": "sustained_64_concurrent",
                    "requests": len(results),
                    "status_counts": dict(collections.Counter(results)),
                    "seconds": round(time.monotonic() - start, 2),
                }
            ),
            flush=True,
        )
        assert results == [200] * 640
        await check_slots("after_load")
        limited = await asyncio.gather(*(request(keys[0], "slow") for _ in range(9)))
        print(json.dumps({"phase": "real_limit_8", "counts": dict(collections.Counter(limited))}), flush=True)
        assert collections.Counter(limited) == {200: 8, 429: 1}
        await check_slots("after_real_limit")

        async def cancel(key):
            async with client.stream(
                "POST",
                "/v1/messages",
                headers={"Authorization": "Bearer " + key},
                json={
                    "model": "lab-model",
                    "max_tokens": 32,
                    "stream": True,
                    "messages": [{"role": "user", "content": "slow"}],
                },
            ) as r:
                assert r.status_code == 200
                async for line in r.aiter_lines():
                    if "message_start" in line:
                        return

        for _ in range(3):
            await asyncio.gather(*(cancel(keys[1]) for _ in range(4)))
            await asyncio.sleep(0.2)
        await check_slots("after_12_client_cancellations")
        failures = await asyncio.gather(*(request(keys[2], "fail504", False) for _ in range(4)))
        assert failures == [504] * 4
        await check_slots("after_4_upstream_504")
        recovered = await asyncio.gather(*(request(keys[2]) for _ in range(8)))
        assert recovered == [200] * 8
        await check_slots("final")
        print(json.dumps({"all_checks": "passed"}), flush=True)


asyncio.run(main())
