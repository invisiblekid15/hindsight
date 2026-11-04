"""Another process's bank change reaches this process's bank caches through the change log.

One test process cannot host two pods, so "the other process" is played by plain SQL: it changes
the bank row and appends the change-log row its own ``invalidate`` would have written, and never
touches this process's cache.
"""

import uuid
from datetime import timedelta

import pytest

from hindsight_api import RequestContext
from hindsight_api.engine import bank_cache_sync
from hindsight_api.engine.bank_cache_sync import BankCacheSync
from hindsight_api.engine.db_utils import acquire_with_retry
from hindsight_api.engine.memory_engine import MemoryEngine
from hindsight_api.engine.retain import bank_utils
from hindsight_api.engine.schema import fq_table
from hindsight_api.schema_context import get_current_schema

pytestmark = [pytest.mark.asyncio]


async def _seed(memory: MemoryEngine, request_context: RequestContext) -> str:
    if memory._database_backend_type != "postgresql":
        pytest.skip("the bank cache sync is PostgreSQL-only")
    bank_id = f"test-cache-sync-{uuid.uuid4().hex[:8]}"
    await memory.ensure_bank_profile(bank_id=bank_id, request_context=request_context)
    return bank_id


async def _name(memory: MemoryEngine, bank_id: str) -> str:
    profile = await bank_utils.get_bank_profile_if_exists(memory._backend, bank_id)
    assert profile is not None
    return profile["name"]


async def test_a_change_made_elsewhere_reaches_this_process(memory: MemoryEngine, request_context: RequestContext):
    bank_id = await _seed(memory, request_context)
    sync = BankCacheSync(memory._require_backend(), 1.0)
    try:
        await sync.poll()
        assert await _name(memory, bank_id) == bank_id  # now cached here

        # Another process deletes the bank and recreates it under the same id.
        async with acquire_with_retry(memory._backend) as conn:
            await conn.execute(f"UPDATE {fq_table('banks')} SET name = 'fresh' WHERE bank_id = $1", bank_id)
        assert await _name(memory, bank_id) == bank_id, "served from this process's cache"
        async with acquire_with_retry(memory._backend) as conn:
            await conn.execute(
                f"INSERT INTO {bank_cache_sync._table()} (schema_name, bank_id) VALUES ($1, $2)",
                get_current_schema() or "",
                bank_id,
            )

        await sync.poll()
        assert await _name(memory, bank_id) == "fresh"
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


async def test_invalidate_publishes_for_the_other_processes(memory: MemoryEngine, request_context: RequestContext):
    bank_id = await _seed(memory, request_context)
    try:
        await memory.update_bank(bank_id, name="renamed", request_context=request_context)
        async with acquire_with_retry(memory._backend) as conn:
            published = await conn.fetchval(
                f"SELECT count(*) FROM {bank_cache_sync._table()} WHERE bank_id = $1", bank_id
            )
        assert published >= 1
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)


async def test_a_process_that_fell_behind_drops_everything(memory: MemoryEngine, request_context: RequestContext):
    bank_id = await _seed(memory, request_context)
    sync = BankCacheSync(memory._require_backend(), 1.0)
    try:
        await sync.poll()
        assert await _name(memory, bank_id) == bank_id
        async with acquire_with_retry(memory._backend) as conn:
            await conn.execute(f"UPDATE {fq_table('banks')} SET name = 'fresh' WHERE bank_id = $1", bank_id)

        # Its last read is older than rows are kept, so whatever changed may already be pruned.
        assert sync._since is not None
        sync._since -= timedelta(seconds=bank_cache_sync._RETENTION_SECONDS)
        await sync.poll()
        assert await _name(memory, bank_id) == "fresh"
    finally:
        await memory.delete_bank(bank_id, request_context=request_context)
