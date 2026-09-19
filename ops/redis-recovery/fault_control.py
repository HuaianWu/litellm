import asyncio

from litellm.integrations.custom_logger import CustomLogger


class Probe(CustomLogger):
    def __init__(self):
        super().__init__()
        self.held = []
        from litellm.proxy.proxy_server import app

        app.add_api_route("/lab/pool/hold", self.hold, methods=["POST"])
        app.add_api_route("/lab/pool/release", self.release, methods=["POST"])
        app.add_api_route("/lab/state", self.state, methods=["GET"])

    async def hold(self):
        from litellm.proxy.proxy_server import redis_usage_cache

        pool = redis_usage_cache.init_async_client().connection_pool
        for _ in range(pool.max_connections):
            try:
                self.held.append(await asyncio.wait_for(pool.get_connection(), timeout=1))
            except Exception:
                break
        return {"held": len(self.held), "capacity": pool.max_connections}

    async def release(self):
        from litellm.proxy.proxy_server import redis_usage_cache

        for connection in self.held:
            await redis_usage_cache.init_async_client().connection_pool.release(connection)
        self.held.clear()
        return {"held": 0}

    async def state(self):
        import redis.asyncio as redis

        from litellm.proxy.proxy_server import proxy_logging_obj, redis_usage_cache

        client = redis.Redis(host="redis", port=6379, decode_responses=True)
        gauges = {}
        async for key in client.scan_iter(match="*max_parallel_requests*"):
            if await client.type(key) == "zset":
                gauges[key] = await client.zcard(key)
        await client.aclose()
        pool = redis_usage_cache.async_redis_conn_pool
        limiter = proxy_logging_obj.get_proxy_hook("parallel_request_limiter")
        return {
            "held": len(self.held),
            "pool_in_use": len(pool._in_use_connections),
            "gauges": gauges,
            "limiter": type(limiter).__name__,
        }


probe = Probe()
