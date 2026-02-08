from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = PROJECT_ROOT / "database" / "database.json"
DEFAULT_CHROMEDRIVER_DIR = PROJECT_ROOT / "chromedriver"
ENV_PATH = PROJECT_ROOT / ".env"


@dataclass(frozen=True)
class BotSettings:
    bot_token: str
    admin_ids: set[int]
    log_level: str
    default_locale: str
    supported_locales: tuple[str, ...]


@dataclass(frozen=True)
class DriverSettings:
    chrome_binary: str | None
    chromedriver_dir: Path
    chromedriver_path_override: str | None


@dataclass(frozen=True)
class StorageSettings:
    database_path: Path


@dataclass(frozen=True)
class Settings:
    bot: BotSettings
    driver: DriverSettings
    storage: StorageSettings


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]

        # Explicitly set environment variables have priority over .env.
        os.environ.setdefault(key, value)


def _normalize_optional(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned if cleaned else None


def _parse_admin_ids(value: str | None) -> set[int]:
    if not value:
        return set()

    parsed: set[int] = set()
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if not part.lstrip("-").isdigit():
            raise RuntimeError(f"Invalid ADMIN_IDS: '{part}' is not a number.")
        parsed.add(int(part))
    return parsed


def _parse_supported_locales(value: str | None) -> tuple[str, ...]:
    if not value:
        return ("en", "ru")

    parsed: list[str] = []
    for raw_part in value.split(","):
        locale = raw_part.strip().lower()
        if locale and locale not in parsed:
            parsed.append(locale)

    if "en" not in parsed:
        parsed.insert(0, "en")
    return tuple(parsed or ("en", "ru"))


def load_settings(*, require_bot_token: bool = True) -> Settings:
    _load_env_file(ENV_PATH)

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if require_bot_token and not bot_token:
        raise RuntimeError("Environment variable BOT_TOKEN is not set.")

    log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
    admin_ids = _parse_admin_ids(_normalize_optional(os.getenv("ADMIN_IDS")))
    supported_locales = _parse_supported_locales(_normalize_optional(os.getenv("SUPPORTED_LOCALES")))
    default_locale = (_normalize_optional(os.getenv("DEFAULT_LOCALE")) or "en").lower()
    if default_locale not in supported_locales:
        default_locale = "en" if "en" in supported_locales else supported_locales[0]

    db_env = _normalize_optional(os.getenv("DATABASE_PATH"))
    database_path = Path(db_env).expanduser() if db_env else DEFAULT_DB_PATH

    chromedriver_dir_env = _normalize_optional(os.getenv("CHROMEDRIVER_DIR"))
    chromedriver_dir = Path(chromedriver_dir_env).expanduser() if chromedriver_dir_env else DEFAULT_CHROMEDRIVER_DIR

    chromedriver_override = _normalize_optional(os.getenv("CHROMEDRIVER_PATH"))
    if chromedriver_override and not Path(chromedriver_override).exists():
        chromedriver_override = None

    return Settings(
        bot=BotSettings(
            bot_token=bot_token,
            admin_ids=admin_ids,
            log_level=log_level,
            default_locale=default_locale,
            supported_locales=supported_locales,
        ),
        driver=DriverSettings(
            chrome_binary=_normalize_optional(os.getenv("CHROME_BINARY")),
            chromedriver_dir=chromedriver_dir,
            chromedriver_path_override=chromedriver_override,
        ),
        storage=StorageSettings(database_path=database_path),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings(require_bot_token=True)


def get_runtime_settings() -> Settings:
    return load_settings(require_bot_token=False)


def reset_settings_cache() -> None:
    get_settings.cache_clear()
