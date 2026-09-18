import asyncio
from types import SimpleNamespace
from typing import Final

import pytest

from litellm.constants import PROXY_DB_LOOKUP_MAX_CONCURRENCY
from litellm.proxy.db.spend_counter_reseed import SpendCounterReseed


class _InFlightCountingTable:
    def __init__(self) -> None:
        self.in_flight = 0
        self.max_in_flight = 0

    async def find_unique(self, where: dict[str, str]) -> SimpleNamespace:
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        await asyncio.sleep(0.001)
        self.in_flight -= 1
        return SimpleNamespace(token=where["token"], spend=1.0)


class _FakePrismaClient:
    def __init__(self) -> None:
        self.db = SimpleNamespace(litellm_verificationtoken=_InFlightCountingTable())


@pytest.mark.asyncio
async def test_from_db_bounds_in_flight_prisma_requests_across_counter_keys():
    prisma: Final = _FakePrismaClient()
    burst: Final = PROXY_DB_LOOKUP_MAX_CONCURRENCY * 5

    results: Final = await asyncio.gather(
        *(SpendCounterReseed.from_db(prisma, f"spend:key:hashed-{i}") for i in range(burst))
    )

    assert results == [1.0] * burst
    assert prisma.db.litellm_verificationtoken.max_in_flight == PROXY_DB_LOOKUP_MAX_CONCURRENCY
