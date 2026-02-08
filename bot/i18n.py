from __future__ import annotations

import json
import logging
from pathlib import Path

from config import get_runtime_settings


logger = logging.getLogger("tsp.i18n")
_SETTINGS = get_runtime_settings()
_LOCALES_DIR = Path(__file__).resolve().parent / "locales"
_DEFAULT_LOCALE = (_SETTINGS.bot.default_locale or "en").lower()
_SUPPORTED_LOCALES = tuple(locale.lower() for locale in _SETTINGS.bot.supported_locales)
_TRANSLATIONS: dict[str, dict[str, str]] = {}


def supported_locales() -> tuple[str, ...]:
    return _SUPPORTED_LOCALES


def default_locale() -> str:
    return _DEFAULT_LOCALE


def normalize_locale(locale: str | None) -> str:
    if not locale:
        return _DEFAULT_LOCALE
    normalized = locale.strip().lower().replace("_", "-").split("-", maxsplit=1)[0]
    if normalized in _SUPPORTED_LOCALES:
        return normalized
    return _DEFAULT_LOCALE


def _load_locale(locale: str) -> dict[str, str]:
    path = _LOCALES_DIR / f"{locale}.json"
    if not path.exists():
        logger.warning("Locale file is missing: %s", path)
        return {}

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load locale file %s: %s", path, exc)
        return {}

    if not isinstance(payload, dict):
        logger.warning("Locale file has invalid format (expected object): %s", path)
        return {}

    return {str(key): str(value) for key, value in payload.items()}


def _get_bundle(locale: str) -> dict[str, str]:
    if locale not in _TRANSLATIONS:
        _TRANSLATIONS[locale] = _load_locale(locale)
    return _TRANSLATIONS[locale]


def translate(locale: str | None, key: str, **kwargs: object) -> str:
    resolved = normalize_locale(locale)
    fallback_chain = (resolved, _DEFAULT_LOCALE, "en")

    template: str | None = None
    for candidate in fallback_chain:
        template = _get_bundle(candidate).get(key)
        if template is not None:
            break

    if template is None:
        return key

    try:
        return template.format(**kwargs)
    except Exception as exc:
        logger.warning("Failed to format i18n key '%s' for locale '%s': %s", key, resolved, exc)
        return template
