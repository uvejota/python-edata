"""Shared types for the migrations package.

Kept separate from ``__init__`` and from the individual migration modules so a
migration file can be deleted in a future version without breaking imports.
"""

from dataclasses import dataclass


@dataclass
class MigrationResult:
    """Summary of what a single migration wrote."""

    name: str
    supplies: int = 0
    contracts: int = 0
    energy: int = 0
    power: int = 0
    pvpc: int = 0
