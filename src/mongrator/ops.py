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


def drop_index(
    collection: str,
    index_name: str,
    keys: list[tuple[str, int]] | None = None,
    **kwargs: Any,
) -> Operation:
    """Drop an index by name. Reverts by recreating the index.

    When *keys* (and optional index options) are provided, revert is fully
    stateless — it recreates the index from the supplied spec without needing
    to have run apply() first.  This is required for the ops-based auto-
    rollback path where the runner calls ``up(db)`` a second time and then
    immediately calls ``revert()`` on fresh Operation instances.

    If *keys* is omitted, apply() will attempt to capture the index spec at
    runtime; however this only works when revert() is called on the **same**
    Operation instance that ran apply().
    """
    _captured_spec: dict[str, Any] = {}

    def apply(db: Database) -> None:  # type: ignore[type-arg]
        if keys is None:
            _captured_spec.clear()
            indexes = db[collection].index_information()
            if index_name in indexes:
                info = indexes[index_name]
                _captured_spec["key"] = info["key"]
                opts = {k: v for k, v in info.items() if k not in ("key", "v", "ns")}
                _captured_spec["opts"] = opts
        db[collection].drop_index(index_name)

    def revert(db: Database) -> None:  # type: ignore[type-arg]
        if keys is not None:
            db[collection].create_index(keys, name=index_name, **kwargs)
        elif _captured_spec:
            _captured_spec["opts"]["name"] = index_name
            db[collection].create_index(_captured_spec["key"], **_captured_spec["opts"])
        else:
            raise NotImplementedError(
                f"drop_index({collection!r}, {index_name!r}) cannot be auto-reverted: "
                "index spec was not captured. Supply keys= or define a down() function."
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


def drop_field(
    collection: str,
    field_name: str,
    filter: dict[str, Any] | None = None,
) -> Operation:
    """Remove a field from all (or filtered) documents. Not auto-reversible
    because the original values are lost.
    """
    query = filter or {}

    def apply(db: Database) -> None:  # type: ignore[type-arg]
        db[collection].update_many(
            {**query, field_name: {"$exists": True}},
            {"$unset": {field_name: ""}},
        )

    def revert(db: Database) -> None:  # type: ignore[type-arg]
        raise NotImplementedError(
            f"drop_field({collection!r}, {field_name!r}) cannot be auto-reverted. "
            "Define a down() function to restore the field."
        )

    return Operation(
        description=f"drop_field({collection!r}, {field_name!r})",
        _apply=apply,
        _revert=revert,
    )


def create_collection(collection: str, **kwargs: Any) -> Operation:
    """Create a collection. Reverts by dropping it."""

    def apply(db: Database) -> None:  # type: ignore[type-arg]
        db.create_collection(collection, **kwargs)

    def revert(db: Database) -> None:  # type: ignore[type-arg]
        db.drop_collection(collection)

    return Operation(
        description="create_collection({!r}{})".format(
            collection,
            ", " + ", ".join(f"{k}={v!r}" for k, v in kwargs.items()) if kwargs else "",
        ),
        _apply=apply,
        _revert=revert,
    )


def drop_collection(collection: str) -> Operation:
    """Drop a collection. Not auto-reversible because the data is lost."""

    def apply(db: Database) -> None:  # type: ignore[type-arg]
        db.drop_collection(collection)

    def revert(db: Database) -> None:  # type: ignore[type-arg]
        raise NotImplementedError(
            f"drop_collection({collection!r}) cannot be auto-reverted. "
            "Define a down() function to recreate the collection."
        )

    return Operation(
        description=f"drop_collection({collection!r})",
        _apply=apply,
        _revert=revert,
    )
