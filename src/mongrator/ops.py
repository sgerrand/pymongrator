"""Declarative operation helpers for MongoDB migrations.

Each helper returns an Operation whose apply() and revert() methods perform the
forward and reverse changes respectively. When a migration's up() function returns
a list[Operation] and no down() is defined, the runner auto-generates rollback by
calling revert() on each operation in reverse order.

Usage in a migration file::

    from mongrator import ops

    def up(db):
        return [
            ops.create_index("users", {"email": 1}, unique=True),
            ops.rename_field("users", "name", "full_name"),
        ]
"""

from dataclasses import dataclass, field
from typing import Any

from pymongo.database import Database


@dataclass
class Operation:
    """An atomic, reversible database operation."""

    description: str
    _apply: Any = field(repr=False)
    _revert: Any = field(repr=False)

    def apply(self, db: Database) -> None:  # type: ignore[type-arg]
        self._apply(db)

    def revert(self, db: Database) -> None:  # type: ignore[type-arg]
        self._revert(db)


def create_index(
    collection: str,
    keys: dict[str, int],
    **kwargs: Any,
) -> Operation:
    """Create an index. Reverts by dropping the index."""
    index_name: str | None = kwargs.get("name")

    def apply(db: Database) -> None:  # type: ignore[type-arg]
        db[collection].create_index(list(keys.items()), **kwargs)

    def revert(db: Database) -> None:  # type: ignore[type-arg]
        name = index_name or "_".join(f"{k}_{v}" for k, v in keys.items())
        db[collection].drop_index(name)

    key_repr = ", ".join(f"{k}: {v}" for k, v in keys.items())
    return Operation(
        description=f"create_index({collection!r}, {{{key_repr}}})",
        _apply=apply,
        _revert=revert,
    )


def drop_index(collection: str, index_name: str) -> Operation:
    """Drop an index by name. Not auto-reversible (index spec is unknown)."""

    def apply(db: Database) -> None:  # type: ignore[type-arg]
        db[collection].drop_index(index_name)

    def revert(db: Database) -> None:  # type: ignore[type-arg]
        raise NotImplementedError(
            f"drop_index({collection!r}, {index_name!r}) cannot be auto-reverted. "
            "Define a down() function to recreate the index."
        )

    return Operation(
        description=f"drop_index({collection!r}, {index_name!r})",
        _apply=apply,
        _revert=revert,
    )


def rename_field(
    collection: str,
    old_name: str,
    new_name: str,
    filter: dict[str, Any] | None = None,
) -> Operation:
    """Rename a field across all (or filtered) documents. Reverts by renaming back."""
    query = filter or {}

    def apply(db: Database) -> None:  # type: ignore[type-arg]
        db[collection].update_many(query, {"$rename": {old_name: new_name}})

    def revert(db: Database) -> None:  # type: ignore[type-arg]
        db[collection].update_many(query, {"$rename": {new_name: old_name}})

    return Operation(
        description=f"rename_field({collection!r}, {old_name!r} → {new_name!r})",
        _apply=apply,
        _revert=revert,
    )


def add_field(
    collection: str,
    field_name: str,
    default_value: Any,
    filter: dict[str, Any] | None = None,
) -> Operation:
    """Add a field with a default value to all (or filtered) documents.
    Reverts by unsetting the field.
    """
    query = filter or {}

    def apply(db: Database) -> None:  # type: ignore[type-arg]
        db[collection].update_many(
            {**query, field_name: {"$exists": False}},
            {"$set": {field_name: default_value}},
        )

    def revert(db: Database) -> None:  # type: ignore[type-arg]
        db[collection].update_many(query, {"$unset": {field_name: ""}})

    return Operation(
        description=f"add_field({collection!r}, {field_name!r}={default_value!r})",
        _apply=apply,
        _revert=revert,
    )
