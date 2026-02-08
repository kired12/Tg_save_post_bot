from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aiogram import types
from aiogram.filters import BaseFilter

from config import get_runtime_settings

PREFIX = "tps_"
MAX_DB_SIZE_BYTES = 10 * 1024 * 1024
DEFAULT_USER = {
    "username": "None",
    "fullname": "*",
    "role": "member",
    "first_login": "*",
    "last_seen": "*",
}


@dataclass(frozen=True)
class EventRecord:
    timestamp: str
    user_id: int
    username: str
    action: str
    status: str
    meta: dict[str, Any]


class Database:
    def __init__(self, filename: str | Path | None = None) -> None:
        if filename is None:
            filename = get_runtime_settings().storage.database_path
        self.filename = Path(filename)
        self.filename.parent.mkdir(parents=True, exist_ok=True)
        self.data: dict[str, Any] = {}
        self.load_data()

    def load_data(self) -> None:
        if not self.filename.exists():
            self.data = self._default_structure()
            return

        try:
            loaded = json.loads(self.filename.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            loaded = {}

        self.data = self._normalize_structure(loaded)

    @staticmethod
    def _default_structure() -> dict[str, Any]:
        return {
            "users": {},
            "events": [],
            "stats": {
                "capture_success": 0,
                "capture_failed": 0,
                "admin_views": 0,
            },
        }

    def _normalize_structure(self, loaded: Any) -> dict[str, Any]:
        normalized = self._default_structure()
        if not isinstance(loaded, dict):
            return normalized

        if "users" in loaded:
            users = loaded.get("users")
            if isinstance(users, dict):
                normalized["users"] = users
        else:
            # миграция legacy-формата: ключи tps_<id> были в корне
            normalized["users"] = {
                key: value
                for key, value in loaded.items()
                if isinstance(key, str) and key.startswith(PREFIX) and isinstance(value, dict)
            }

        events = loaded.get("events")
        if isinstance(events, list):
            normalized["events"] = events

        stats = loaded.get("stats")
        if isinstance(stats, dict):
            normalized["stats"].update(stats)

        return normalized

    def save_data(self) -> None:
        self.filename.write_text(
            json.dumps(self.data, indent=4, ensure_ascii=False),
            encoding="utf-8",
        )

    def users(self) -> dict[str, dict[str, Any]]:
        users = self.data.setdefault("users", {})
        if not isinstance(users, dict):
            users = {}
            self.data["users"] = users
        return users

    def events(self) -> list[dict[str, Any]]:
        events = self.data.setdefault("events", [])
        if not isinstance(events, list):
            events = []
            self.data["events"] = events
        return events

    def stats(self) -> dict[str, Any]:
        stats = self.data.setdefault("stats", {})
        if not isinstance(stats, dict):
            stats = {}
            self.data["stats"] = stats
        return stats

    def insert_user(self, key: str, value: dict[str, Any]) -> None:
        self.users()[key] = value
        self.save_data()

    def get_user(self, key: str) -> dict[str, Any] | None:
        user = self.users().get(key)
        return user if isinstance(user, dict) else None

    def delete_user(self, key: str) -> None:
        users = self.users()
        if key in users:
            del users[key]
            self.save_data()

    def append_event(self, event: EventRecord) -> None:
        self.events().append(
            {
                "timestamp": event.timestamp,
                "user_id": event.user_id,
                "username": event.username,
                "action": event.action,
                "status": event.status,
                "meta": event.meta,
            }
        )
        self._update_stats_for_event(event)
        self.save_data()

    def _update_stats_for_event(self, event: EventRecord) -> None:
        stats = self.stats()
        if event.action == "capture_success":
            stats["capture_success"] = int(stats.get("capture_success", 0)) + 1
        if event.action == "capture_failed":
            stats["capture_failed"] = int(stats.get("capture_failed", 0)) + 1
        if event.action.startswith("admin_"):
            stats["admin_views"] = int(stats.get("admin_views", 0)) + 1

    def file_size_bytes(self) -> int:
        if not self.filename.exists():
            return 0
        return self.filename.stat().st_size

    def db_size_warning(self) -> str | None:
        size = self.file_size_bytes()
        if size > MAX_DB_SIZE_BYTES:
            size_mb = size / (1024 * 1024)
            return f"Размер базы {size_mb:.2f} MB превышает рекомендуемый лимит 10 MB."
        return None


_db = Database()


def _get_key(user_id: int) -> str:
    return f"{PREFIX}{user_id}"


def _normalize_username(username: str | None) -> str:
    if username and re.match(r"^[a-zA-Z0-9_]+$", username):
        return username
    return "None"


def _normalize_fullname(fullname: str | None) -> str:
    return fullname if fullname else "*"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def db_load() -> None:
    print(_db.data)


def db_register(user_id: int, username: str | None, fullname: str | None) -> None:
    key = _get_key(user_id)
    user_data = _db.get_user(key) or DEFAULT_USER.copy()
    user_data["username"] = _normalize_username(username)
    user_data["fullname"] = _normalize_fullname(fullname)
    user_data["last_seen"] = _now_iso()
    _db.insert_user(key, user_data)


def db_delete(user_id: int) -> None:
    _db.delete_user(_get_key(user_id))


def db_info(user_id: int) -> dict[str, Any] | None:
    return _db.get_user(_get_key(user_id))


def db_change(user_id: int, subject: str, value: Any) -> None:
    key = _get_key(user_id)
    user_data = _db.get_user(key) or DEFAULT_USER.copy()
    user_data[subject] = value
    _db.insert_user(key, user_data)


def db_role(user_id: int) -> str:
    user_data = _db.get_user(_get_key(user_id)) or DEFAULT_USER
    return str(user_data.get("role", "member"))


def db_get_value(user_id: int, field: str) -> Any:
    user_data = _db.get_user(_get_key(user_id))
    return user_data.get(field) if user_data else None


def db_log_event(
    *,
    user_id: int,
    username: str | None,
    action: str,
    status: str,
    meta: dict[str, Any] | None = None,
) -> None:
    _db.append_event(
        EventRecord(
            timestamp=_now_iso(),
            user_id=user_id,
            username=_normalize_username(username),
            action=action,
            status=status,
            meta=meta or {},
        )
    )


def db_recent_users(limit: int = 10) -> list[tuple[str, dict[str, Any]]]:
    users = list(_db.users().items())
    users.sort(key=lambda item: str(item[1].get("last_seen", "")), reverse=True)
    return users[:limit]


def db_recent_events(limit: int = 20, *, status: str | None = None) -> list[dict[str, Any]]:
    events = _db.events()
    if status:
        events = [event for event in events if event.get("status") == status]
    return list(reversed(events[-limit:]))


def db_stats() -> dict[str, Any]:
    stats = _db.stats().copy()
    stats["total_users"] = len(_db.users())
    stats["total_events"] = len(_db.events())
    return stats


def db_find_user(query: str) -> tuple[str, dict[str, Any]] | None:
    query = query.strip()
    users = _db.users()

    if query.isdigit():
        key = _get_key(int(query))
        user = users.get(key)
        if isinstance(user, dict):
            return key, user

    needle = query.lower().lstrip("@")
    for key, user in users.items():
        username = str(user.get("username", "")).lower()
        if username == needle:
            return key, user
    return None


def db_size_warning() -> str | None:
    return _db.db_size_warning()


def db_compact_events(keep_last: int = 2000) -> int:
    events = _db.events()
    if keep_last < 1:
        keep_last = 1
    removed = max(len(events) - keep_last, 0)
    if removed:
        _db.data["events"] = events[-keep_last:]
        _db.save_data()
    return removed


class IsUser(BaseFilter):
    def __init__(self, required_role: str) -> None:
        self.required_role = required_role

    async def __call__(self, message: types.Message) -> bool:
        if message.from_user is None:
            return False
        user_id = message.from_user.id
        user_role = db_role(user_id)
        return user_role == self.required_role
