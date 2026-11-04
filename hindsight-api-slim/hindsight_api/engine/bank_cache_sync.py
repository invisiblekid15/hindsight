"""Keep the per-process bank caches in step across processes, through the database.

Each process caches a bank's profile and config (``bank_info_cache``) and alias routing
(``bank_aliases``) in memory. Invalidating them used to reach only the process that made the change,
so after a bank was deleted and recreated under the same id, every other API pod and worker kept
serving the deleted bank's name, mission and config until the TTL ran out.

No pod-to-pod channel is used, and nothing that needs a session-pinned connection (LISTEN/NOTIFY
does not survive PgBouncer in transaction mode). Instead:

* ``publish`` appends ``(schema, bank_id)`` to the base schema's ``bank_cache_events`` table. The
  two caches call it from their ``invalidate``, so every path that already invalidates publishes.
* every process runs :class:`BankCacheSync`, which reads the rows newer than its last read every
  ``bank_cache_sync_interval_seconds`` and drops those banks from its own caches.

Each read reaches back ``_OVERLAP_SECONDS`` past the previous one, so every row is applied several
times. That is what makes the plain timestamp cursor safe: a row whose insert commits a moment after
a later one was read, and an invalidate published before its change committed (the reload then saw
the old row), are both caught by the next pass. Dropping an entry twice costs one re-read.

A process that could not read the log for longer than rows are kept may have missed some, so it
drops everything instead. Failures to publish or poll are logged and never fail a request: the TTLs
remain the backstop. PostgreSQL only (see the migration).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from ..config import get_config
from .db_utils import acquire_with_retry
from .schema import _is_oracle, fq_table_explicit

if TYPE_CHECKING:
    from .db.base import DatabaseBackend

logger = logging.getLogger(__name__)

_OVERLAP_SECONDS = 5.0
_RETENTION_SECONDS = 600.0
_PRUNE_EVERY_SECONDS = 60.0

# The backend of the sync loop running in this process; None when none runs (Oracle, disabled, or a
# test without an engine), and then publishing is skipped as there is no one to keep in step.
_backend: DatabaseBackend | None = None


def _table() -> str:
    return fq_table_explicit("bank_cache_events", get_config().database_schema)


async def publish(schema: str, bank_id: str) -> None:
    """Tell every process that this bank (or alias) changed, so each drops its cached copy."""
    backend = _backend
    if backend is None:
        return
    try:
        async with acquire_with_retry(backend) as conn:
            await conn.execute(f"INSERT INTO {_table()} (schema_name, bank_id) VALUES ($1, $2)", schema, bank_id)
    except Exception as exc:  # noqa: BLE001 — the TTL still bounds staleness; never fail the write
        logger.warning(
            "bank cache event for %s.%s not published (%s); other processes wait out the TTL", schema, bank_id, exc
        )


async def _forget(schema: str, bank_id: str) -> None:
    from . import bank_aliases, bank_info_cache  # noqa: PLC0415 — they import this module

    await bank_info_cache.forget(schema, bank_id)
    await bank_aliases.forget(schema, bank_id)


async def _forget_everything() -> None:
    from . import bank_aliases, bank_info_cache  # noqa: PLC0415

    await bank_info_cache.forget_all()
    await bank_aliases.forget_all()


class BankCacheSync:
    """The per-process loop that applies other processes' bank changes to this one's caches."""

    def __init__(self, backend: DatabaseBackend, interval_seconds: float) -> None:
        self._backend = backend
        self._interval = interval_seconds
        self._since: datetime | None = None
        self._last_prune = 0.0
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        global _backend
        if self._interval <= 0 or _is_oracle():
            return
        _backend = self._backend
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        global _backend
        if _backend is self._backend:
            _backend = None
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.poll()
            except Exception as exc:  # noqa: BLE001 — keep polling; a long outage is handled in poll()
                logger.warning("bank cache sync poll failed (%s)", exc)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass

    async def poll(self) -> None:
        """Apply the changes published since the last poll, and prune old rows now and then."""
        table = _table()
        async with acquire_with_retry(self._backend) as conn:
            now: datetime = await conn.fetchval("SELECT clock_timestamp()")
            if self._since is not None and now - self._since > timedelta(seconds=_RETENTION_SECONDS / 2):
                # Behind for longer than rows are sure to be kept: some may be gone unread.
                rows = None
            else:
                rows = await conn.fetch(
                    f"SELECT DISTINCT schema_name, bank_id FROM {table} WHERE created_at > $1",
                    (self._since or now) - timedelta(seconds=_OVERLAP_SECONDS),
                )
            if time.monotonic() - self._last_prune > _PRUNE_EVERY_SECONDS:
                await conn.execute(
                    f"DELETE FROM {table} WHERE created_at < $1", now - timedelta(seconds=_RETENTION_SECONDS)
                )
                self._last_prune = time.monotonic()
        if rows is None:
            logger.warning("bank cache sync fell behind the change log; dropping every cached bank")
            await _forget_everything()
        else:
            for row in rows:
                await _forget(row["schema_name"], row["bank_id"])
        self._since = now
