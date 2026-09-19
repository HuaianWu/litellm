# ruff: noqa: T201  # diagnostic CLI prints structured verification evidence
import asyncio
import collections
import hashlib
import json
import os
import time
import uuid

import httpx

BASE = "http://127.0.0.1:4000"
MASTER = {"Authorization": "Bearer sk-redis-lab-only"}


async def main():
    async with httpx.AsyncClient(base_url=BASE, timeout=60) as client:
        for _ in range(40):
            try:
                if (await client.get("/health/liveliness")).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
        r = await client.post(
            "/key/generate",
            headers=MASTER,
            json={
                "models": ["lab-model"],
                "max_parallel_requests": 8,
                "key_alias": "isolated-redis-fault-" + uuid.uuid4().hex[:8],
            },
        )
        r.raise_for_status()
        key = r.json()["key"]
        gauge_key = "{api_key:" + hashlib.sha256(key.encode()).hexdigest() + "}:max_parallel_requests"
        headers = {"Authorization": "Bearer " + key, "anthropic-version": "2023-06-01"}

        async def request(text="ok", stream=False):
            t = time.monotonic()
            r = await client.post(
                "/v1/messages",
                headers=headers,
                json={
                    "model": "lab-model",
                    "max_tokens": 32,
                    "stream": stream,
                    "messages": [{"role": "user", "content": text}],
                },
            )
            return {
                "status": r.status_code,
                "seconds": round(time.monotonic() - t, 3),
                "error": r.text[:160] if r.status_code != 200 else None,
            }

        normal = [await request(stream=True) for _ in range(12)]
        assert all(result["status"] == 200 for result in normal)
        await asyncio.sleep(1)
        print(
            json.dumps(
                {
                    "phase": "normal",
                    "counts": dict(collections.Counter(x["status"] for x in normal)),
                    "state": (await client.get("/lab/state")).json(),
                }
            ),
            flush=True,
        )
        pending = [asyncio.create_task(request("slow", True)) for _ in range(8)]
        for _ in range(40):
            state = (await client.get("/lab/state")).json()
            if state["gauges"].get(gauge_key, 0) >= 8:
                break
            await asyncio.sleep(0.03)
        print(json.dumps({"phase": "before_fault", "state": state}), flush=True)
        assert state["gauges"].get(gauge_key, 0) == 8
        print(json.dumps({"phase": "pool_hold", "result": (await client.post("/lab/pool/hold")).json()}), flush=True)
        try:
            await asyncio.sleep(25)
        finally:
            print(
                json.dumps({"phase": "pool_restore", "result": (await client.post("/lab/pool/release")).json()}),
                flush=True,
            )
        completed = await asyncio.gather(*pending)
        await asyncio.sleep(2)
        print(
            json.dumps({"phase": "completed", "requests": completed, "state": (await client.get("/lab/state")).json()}),
            flush=True,
        )
        probes = []
        for _ in range(5):
            probes.append(await request())
            await asyncio.sleep(1)
        print(
            json.dumps(
                {"phase": "recovery_probes", "results": probes, "state": (await client.get("/lab/state")).json()}
            ),
            flush=True,
        )
        await asyncio.sleep(50)
        later = [await request() for _ in range(5)]
        final_state = (await client.get("/lab/state")).json()
        print(
            json.dumps(
                {
                    "phase": "after_breaker_recovery",
                    "results": later,
                    "own_remaining_slots": final_state["gauges"].get(gauge_key, 0),
                }
            ),
            flush=True,
        )
        if os.environ.get("EXPECT_RECOVERY") == "1":
            assert all(result["status"] == 200 for result in later)
            assert final_state["gauges"].get(gauge_key, 0) == 0
            print("recovery_assertions=passed", flush=True)


asyncio.run(main())
