"""Versioned data migrations for the edata SQLite store.

Each migration is a self-contained module exposing ``NAME`` and an async
``apply(db, storage_dir, cups)`` that returns a :class:`MigrationResult` (or
``None`` when it does not apply). ``run_migrations`` runs the ordered registry.

To retire a migration in a future version, delete its module and drop it from
``MIGRATIONS``.
"""

import logging

from edata.database.controller import EdataDB
from edata.database.migrations import legacy_json_1_3_3
from edata.database.migrations.base import MigrationResult

_LOGGER = logging.getLogger(__name__)

# Ordered registry of migrations to run.
MIGRATIONS = [legacy_json_1_3_3]

__all__ = ["MIGRATIONS", "MigrationResult", "run_migrations"]


async def run_migrations(
    db: EdataDB, storage_dir: str, cups: str
) -> list[MigrationResult]:
    """Run every registered migration; return the results that applied."""

    results: list[MigrationResult] = []
    for migration in MIGRATIONS:
        result = await migration.apply(db, storage_dir, cups)
        if result is not None:
            _LOGGER.info("Applied migration '%s'", result.name)
            results.append(result)
    return results
