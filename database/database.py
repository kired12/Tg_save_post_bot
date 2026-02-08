from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from aiogram import types
from aiogram.filters import BaseFilter

from config import get_runtime_settings

PREFIX = "tsp_"
LEGACY_PREFIX = "tps_"
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
        self._lock = threading.RLock()
        self.data: dict[str, Any] = {}
        self.load_data()

    def load_data(self) -> None:
        with self._lock:
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
                normalized["users"] = self._normalize_user_keys(users)
        else:
            # миграция legacy-формата: ключи пользователей были в корне
            legacy_users = {
                key: value
                for key, value in loaded.items()
                if isinstance(key, str)
                and (key.startswith(PREFIX) or key.startswith(LEGACY_PREFIX))
                and isinstance(value, dict)
            }
            normalized["users"] = self._normalize_user_keys(legacy_users)

        events = loaded.get("events")
        if isinstance(events, list):
            normalized["events"] = events

        stats = loaded.get("stats")
        if isinstance(stats, dict):
            normalized["stats"].update(stats)

        return normalized

    @staticmethod
    def _normalize_user_keys(users: dict[str, Any]) -> dict[str, dict[str, Any]]:
        normalized_users: dict[str, dict[str, Any]] = {}
        for key, value in users.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                continue
            normalized_key = key
            if key.startswith(LEGACY_PREFIX):
                normalized_key = PREFIX + key[len(LEGACY_PREFIX) :]
            normalized_users[normalized_key] = value
        return normalized_users

    def save_data(self) -> None:
        payload = json.dumps(self.data, indent=4, ensure_ascii=False)
        tmp_path = self.filename.with_suffix(self.filename.suffix + ".tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        tmp_path.replace(self.filename)

    def users(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            users = self.data.setdefault("users", {})
            if not isinstance(users, dict):
                users = {}
                self.data["users"] = users
            return users

    def events(self) -> list[dict[str, Any]]:
        with self._lock:
            events = self.data.setdefault("events", [])
            if not isinstance(events, list):
                events = []
                self.data["events"] = events
            return events

    def stats(self) -> dict[str, Any]:
        with self._lock:
            stats = self.data.setdefault("stats", {})
            if not isinstance(stats, dict):
                stats = {}
                self.data["stats"] = stats
            return stats

    def users_snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            users = self.data.setdefault("users", {})
            if not isinstance(users, dict):
                users = {}
                self.data["users"] = users
            return {key: value.copy() if isinstance(value, dict) else {} for key, value in users.items()}

    def events_snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            events = self.data.setdefault("events", [])
            if not isinstance(events, list):
                events = []
                self.data["events"] = events
            return [event.copy() if isinstance(event, dict) else {} for event in events]

    def stats_snapshot(self) -> dict[str, Any]:
        with self._lock:
            stats = self.data.setdefault("stats", {})
            if not isinstance(stats, dict):
                stats = {}
                self.data["stats"] = stats
            return stats.copy()

    def insert_user(self, key: str, value: dict[str, Any]) -> None:
        with self._lock:
            users = self.data.setdefault("users", {})
            if not isinstance(users, dict):
                users = {}
                self.data["users"] = users
            users[key] = value
            self.save_data()

    def update_user(self, key: str, updater: Callable[[dict[str, Any]], dict[str, Any]]) -> None:
        with self._lock:
            users = self.data.setdefault("users", {})
            if not isinstance(users, dict):
                users = {}
                self.data["users"] = users
            base = users.get(key)
            user_data = base.copy() if isinstance(base, dict) else DEFAULT_USER.copy()
            updated = updater(user_data)
            users[key] = updated if isinstance(updated, dict) else user_data
            self.save_data()

    def get_user(self, key: str) -> dict[str, Any] | None:
        with self._lock:
            users = self.data.setdefault("users", {})
            if not isinstance(users, dict):
                users = {}
                self.data["users"] = users
            user = users.get(key)
            return user.copy() if isinstance(user, dict) else None

    def delete_user(self, key: str) -> None:
        with self._lock:
            users = self.data.setdefault("users", {})
            if not isinstance(users, dict):
                users = {}
                self.data["users"] = users
            if key in users:
                del users[key]
                self.save_data()

    def append_event(self, event: EventRecord) -> None:
        with self._lock:
            events = self.data.setdefault("events", [])
            if not isinstance(events, list):
                events = []
                self.data["events"] = events
            events.append(
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
        stats = self.data.setdefault("stats", {})
        if not isinstance(stats, dict):
            stats = {}
            self.data["stats"] = stats
        if event.action == "capture_success":
            stats["capture_success"] = int(stats.get("capture_success", 0)) + 1
        if event.action == "capture_failed":
            stats["capture_failed"] = int(stats.get("capture_failed", 0)) + 1
        if event.action.startswith("admin_"):
            stats["admin_views"] = int(stats.get("admin_views", 0)) + 1

    def file_size_bytes(self) -> int:
        with self._lock:
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
    def _updater(user_data: dict[str, Any]) -> dict[str, Any]:
        user_data["username"] = _normalize_username(username)
        user_data["fullname"] = _normalize_fullname(fullname)
        user_data["last_seen"] = _now_iso()
        return user_data

    _db.update_user(key, _updater)


def db_delete(user_id: int) -> None:
    _db.delete_user(_get_key(user_id))


def db_info(user_id: int) -> dict[str, Any] | None:
    return _db.get_user(_get_key(user_id))


def db_change(user_id: int, subject: str, value: Any) -> None:
    key = _get_key(user_id)
    def _updater(user_data: dict[str, Any]) -> dict[str, Any]:
        user_data[subject] = value
        return user_data

    _db.update_user(key, _updater)


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
    users = list(_db.users_snapshot().items())
    users.sort(key=lambda item: str(item[1].get("last_seen", "")), reverse=True)
    return users[:limit]


def db_recent_events(limit: int = 20, *, status: str | None = None) -> list[dict[str, Any]]:
    events = _db.events_snapshot()
    if status:
        events = [event for event in events if event.get("status") == status]
    return list(reversed(events[-limit:]))


def db_stats() -> dict[str, Any]:
    stats = _db.stats_snapshot()
    stats["total_users"] = len(_db.users_snapshot())
    stats["total_events"] = len(_db.events_snapshot())
    return stats


def db_find_user(query: str) -> tuple[str, dict[str, Any]] | None:
    query = query.strip()
    users = _db.users_snapshot()

    if query.isdigit():
        key = _get_key(int(query))
        user = users.get(key)
        if isinstance(user, dict):
            return key, user.copy()

    needle = query.lower().lstrip("@")
    for key, user in users.items():
        username = str(user.get("username", "")).lower()
        if username == needle:
            return key, user.copy()
    return None


def db_size_warning() -> str | None:
    return _db.db_size_warning()


def db_compact_events(keep_last: int = 2000) -> int:
    with _db._lock:
        events = _db.data.setdefault("events", [])
        if not isinstance(events, list):
            events = []
            _db.data["events"] = events
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
