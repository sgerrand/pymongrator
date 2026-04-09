"""Unit tests for mongrator.ops — uses MagicMock in place of a real pymongo Database."""

from unittest.mock import MagicMock

import pytest

from mongrator.ops import (
    Operation,
    add_field,
    create_collection,
    create_index,
    drop_collection,
    drop_field,
    drop_index,
    rename_field,
)


def _db() -> MagicMock:
    return MagicMock()


# ---------------------------------------------------------------------------
# Operation dataclass
# ---------------------------------------------------------------------------


def test_operation_apply_delegates() -> None:
    called_with: list[object] = []
    op = Operation("desc", _apply=lambda db: called_with.append(db), _revert=lambda db: None)
    sentinel = MagicMock()
    op.apply(sentinel)
    assert called_with == [sentinel]


def test_operation_revert_delegates() -> None:
    called_with: list[object] = []
    op = Operation("desc", _apply=lambda db: None, _revert=lambda db: called_with.append(db))
    sentinel = MagicMock()
    op.revert(sentinel)
    assert called_with == [sentinel]


# ---------------------------------------------------------------------------
# create_index
# ---------------------------------------------------------------------------


def test_create_index_description() -> None:
    op = create_index("users", {"email": 1})
    assert "create_index" in op.description
    assert "users" in op.description
    assert "email" in op.description


def test_create_index_apply() -> None:
    db = _db()
    op = create_index("users", {"email": 1}, unique=True)
    op.apply(db)
    db["users"].create_index.assert_called_once_with([("email", 1)], unique=True)


def test_create_index_revert_auto_name() -> None:
    db = _db()
    op = create_index("users", {"email": 1})
    op.revert(db)
    db["users"].drop_index.assert_called_once_with("email_1")


def test_create_index_revert_explicit_name() -> None:
    db = _db()
    op = create_index("users", {"email": 1}, name="my_index")
    op.revert(db)
    db["users"].drop_index.assert_called_once_with("my_index")


def test_create_index_compound_auto_name() -> None:
    db = _db()
    op = create_index("orders", {"user_id": 1, "created_at": -1})
    op.revert(db)
    db["orders"].drop_index.assert_called_once_with("user_id_1_created_at_-1")


# ---------------------------------------------------------------------------
# drop_index
# ---------------------------------------------------------------------------


def test_drop_index_description() -> None:
    op = drop_index("users", "email_1")
    assert "drop_index" in op.description
    assert "email_1" in op.description


def test_drop_index_apply() -> None:
    db = _db()
    op = drop_index("users", "email_1")
    op.apply(db)
    db["users"].drop_index.assert_called_once_with("email_1")


def test_drop_index_revert_raises() -> None:
    db = _db()
    op = drop_index("users", "email_1")
    with pytest.raises(NotImplementedError):
        op.revert(db)


# ---------------------------------------------------------------------------
# rename_field
# ---------------------------------------------------------------------------


def test_rename_field_description() -> None:
    op = rename_field("users", "name", "full_name")
    assert "rename_field" in op.description
    assert "name" in op.description
    assert "full_name" in op.description


def test_rename_field_apply() -> None:
    db = _db()
    op = rename_field("users", "name", "full_name")
    op.apply(db)
    db["users"].update_many.assert_called_once_with({}, {"$rename": {"name": "full_name"}})


def test_rename_field_revert() -> None:
    db = _db()
    op = rename_field("users", "name", "full_name")
    op.revert(db)
    db["users"].update_many.assert_called_once_with({}, {"$rename": {"full_name": "name"}})


def test_rename_field_with_filter() -> None:
    db = _db()
    op = rename_field("users", "name", "full_name", filter={"active": True})
    op.apply(db)
    db["users"].update_many.assert_called_once_with({"active": True}, {"$rename": {"name": "full_name"}})


def test_rename_field_revert_with_filter() -> None:
    db = _db()
    op = rename_field("users", "name", "full_name", filter={"active": True})
    op.revert(db)
    db["users"].update_many.assert_called_once_with({"active": True}, {"$rename": {"full_name": "name"}})


# ---------------------------------------------------------------------------
# add_field
# ---------------------------------------------------------------------------


def test_add_field_description() -> None:
    op = add_field("users", "verified", False)
    assert "add_field" in op.description
    assert "verified" in op.description


def test_add_field_apply_sets_only_missing() -> None:
    db = _db()
    op = add_field("users", "verified", False)
    op.apply(db)
    db["users"].update_many.assert_called_once_with(
        {"verified": {"$exists": False}},
        {"$set": {"verified": False}},
    )


def test_add_field_apply_with_filter() -> None:
    db = _db()
    op = add_field("users", "verified", False, filter={"role": "admin"})
    op.apply(db)
    db["users"].update_many.assert_called_once_with(
        {"role": "admin", "verified": {"$exists": False}},
        {"$set": {"verified": False}},
    )


def test_add_field_revert_unsets() -> None:
    db = _db()
    op = add_field("users", "verified", False)
    op.revert(db)
    db["users"].update_many.assert_called_once_with({}, {"$unset": {"verified": ""}})


def test_add_field_revert_with_filter() -> None:
    db = _db()
    op = add_field("users", "verified", False, filter={"role": "admin"})
    op.revert(db)
    db["users"].update_many.assert_called_once_with({"role": "admin"}, {"$unset": {"verified": ""}})


# ---------------------------------------------------------------------------
# drop_field
# ---------------------------------------------------------------------------


def test_drop_field_description() -> None:
    op = drop_field("users", "legacy_flag")
    assert "drop_field" in op.description
    assert "legacy_flag" in op.description


def test_drop_field_apply() -> None:
    db = _db()
    op = drop_field("users", "legacy_flag")
    op.apply(db)
    db["users"].update_many.assert_called_once_with({"legacy_flag": {"$exists": True}}, {"$unset": {"legacy_flag": ""}})


def test_drop_field_apply_with_filter() -> None:
    db = _db()
    op = drop_field("users", "legacy_flag", filter={"active": False})
    op.apply(db)
    db["users"].update_many.assert_called_once_with(
        {"active": False, "legacy_flag": {"$exists": True}}, {"$unset": {"legacy_flag": ""}}
    )


def test_drop_field_revert_raises() -> None:
    db = _db()
    op = drop_field("users", "legacy_flag")
    with pytest.raises(NotImplementedError):
        op.revert(db)


# ---------------------------------------------------------------------------
# create_collection
# ---------------------------------------------------------------------------


def test_create_collection_description() -> None:
    op = create_collection("audit_log")
    assert "create_collection" in op.description
    assert "audit_log" in op.description


def test_create_collection_apply() -> None:
    db = _db()
    op = create_collection("audit_log")
    op.apply(db)
    db.create_collection.assert_called_once_with("audit_log")


def test_create_collection_apply_with_kwargs() -> None:
    db = _db()
    op = create_collection("audit_log", capped=True, size=1048576)
    op.apply(db)
    db.create_collection.assert_called_once_with("audit_log", capped=True, size=1048576)


def test_create_collection_revert_drops() -> None:
    db = _db()
    op = create_collection("audit_log")
    op.revert(db)
    db.drop_collection.assert_called_once_with("audit_log")


# ---------------------------------------------------------------------------
# drop_collection
# ---------------------------------------------------------------------------


def test_drop_collection_description() -> None:
    op = drop_collection("old_logs")
    assert "drop_collection" in op.description
    assert "old_logs" in op.description


def test_drop_collection_apply() -> None:
    db = _db()
    op = drop_collection("old_logs")
    op.apply(db)
    db.drop_collection.assert_called_once_with("old_logs")


def test_drop_collection_revert_raises() -> None:
    db = _db()
    op = drop_collection("old_logs")
    with pytest.raises(NotImplementedError):
        op.revert(db)
