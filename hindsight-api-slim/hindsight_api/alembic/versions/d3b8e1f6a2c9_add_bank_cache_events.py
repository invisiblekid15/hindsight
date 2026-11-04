"""Add the bank change log that keeps per-process bank caches in step across pods

Every process caches a bank's profile, config and alias routing in memory. A process that changes
or deletes a bank appends one row here; every process reads the rows newer than its last read about
once a second and drops those banks from its own caches (engine/bank_cache_sync.py). Rows are
short-lived: readers prune anything older than a few minutes.

Only the base schema's table is read and written (one log for all tenants, so one poll per process
whatever the tenant count). Created in every schema anyway, like every other table, so a deployment
whose base schema is not ``public`` gets it without a special case.

PostgreSQL only: the sync loop does not run on Oracle, which keeps the cache TTLs as its only bound
(the same split as the maintenance loop). The Oracle slot is intentionally absent.

Revision ID: d3b8e1f6a2c9
Revises: c7e2f9a4d1b8
Create Date: 2026-11-04
"""

from collections.abc import Sequence

from alembic import context, op

from hindsight_api.alembic._dialect import run_for_dialect

revision: str = "d3b8e1f6a2c9"
down_revision: str | Sequence[str] | None = "c7e2f9a4d1b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _pg_schema_prefix() -> str:
    """Schema-qualifier for raw SQL on PG (multi-tenant search_path)."""
    schema = context.config.get_main_option("target_schema")
    return f'"{schema}".' if schema else ""


def _pg_upgrade() -> None:
    schema = _pg_schema_prefix()
    # clock_timestamp(), not now(): the row is read by time, and now() is the transaction's start.
    op.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}bank_cache_events (
            id BIGSERIAL PRIMARY KEY,
            schema_name TEXT NOT NULL,
            bank_id TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
        )
        """
    )
    op.execute(f"CREATE INDEX IF NOT EXISTS idx_bank_cache_events_created_at ON {schema}bank_cache_events (created_at)")


def _pg_downgrade() -> None:
    schema = _pg_schema_prefix()
    op.execute(f"DROP TABLE IF EXISTS {schema}bank_cache_events")


def upgrade() -> None:
    run_for_dialect(pg=_pg_upgrade)  # oracle slot intentionally absent → no-op


def downgrade() -> None:
    run_for_dialect(pg=_pg_downgrade)
