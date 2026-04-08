import types
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, TypedDict

type MigrationId = str
type Checksum = str


class MigrationRecord(TypedDict):
    """Document stored in the tracking collection for each applied migration."""

    _id: MigrationId
    applied_at: datetime
    checksum: Checksum
    direction: Literal["up", "down"]
    duration_ms: int


@dataclass
class MigrationFile:
    """Represents a discovered migration file on disk."""

    id: MigrationId
    path: Path
    checksum: Checksum
    module: types.ModuleType | None = field(default=None, repr=False)

    @property
    def up(self) -> Callable[..., Any] | None:
        if self.module is None:
            return None
        return getattr(self.module, "up", None)

    @property
    def down(self) -> Callable[..., Any] | None:
        if self.module is None:
            return None
        return getattr(self.module, "down", None)

    def has_up(self) -> bool:
        return self.up is not None

    def has_down(self) -> bool:
        return self.down is not None


@dataclass
class MigrationStatus:
    """Status of a single migration as reported by runner.status()."""

    id: MigrationId
    applied: bool
    applied_at: datetime | None = None
    checksum_ok: bool = True
