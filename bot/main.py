from __future__ import annotations

import asyncio
import io
import logging
import os
import platform
import re
import shutil
import tempfile
import time
import uuid
from datetime import datetime
from functools import partial
from pathlib import Path

from aiogram import F, Router, types
from aiogram.filters import Command
from aiogram.types import BufferedInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from PIL import Image
from selenium import webdriver
from selenium.common.exceptions import SessionNotCreatedException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from bot.driver_manager import BrowserNotFoundError, DriverManager, DriverResolution
from config import get_runtime_settings
from database.database import (
    db_change,
    db_compact_events,
    db_find_user,
    db_get_value,
    db_log_event,
    db_recent_events,
    db_recent_users,
    db_register,
    db_size_warning,
    db_stats,
)

settings = get_runtime_settings()
logging.basicConfig(
    level=getattr(logging, settings.bot.log_level, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)
driver_logger = logging.getLogger("tps.driver")

router_main = Router()
_driver_manager = DriverManager(settings.driver)
_driver: webdriver.Chrome | None = None
_driver_resolution: DriverResolution | None = None
_driver_error_hint: str | None = None
_driver_boot_lock = asyncio.Lock()
_driver_lock = asyncio.Lock()
_process_pool = None
_temp_profile_dirs: list[str] = []
_CHROMEDRIVER_LOG_PATH = Path("chromedriver.log")
_CHROMEDRIVER_LOG_MAX_BYTES = max(int(os.getenv("CHROMEDRIVER_LOG_MAX_BYTES", "10485760")), 1)
_CHROMEDRIVER_LOG_KEEP_LINES = max(int(os.getenv("CHROMEDRIVER_LOG_KEEP_LINES", "5000")), 1)
_first_image_notice_lock = asyncio.Lock()
_is_first_image_after_start = True


async def _consume_first_image_flag() -> bool:
    global _is_first_image_after_start
    async with _first_image_notice_lock:
        if _is_first_image_after_start:
            _is_first_image_after_start = False
            return True
        return False


def _mark_driver(driver: webdriver.Chrome, profile_dir: str | None) -> None:
    if profile_dir:
        setattr(driver, "_tps_profile_dir", profile_dir)
        _temp_profile_dirs.append(profile_dir)


def _cleanup_profile_from_driver(driver: webdriver.Chrome | None) -> None:
    if not driver:
        return
    profile_dir = getattr(driver, "_tps_profile_dir", None)
    if profile_dir:
        try:
            shutil.rmtree(profile_dir, ignore_errors=True)
        except Exception:
            pass
        try:
            _temp_profile_dirs.remove(profile_dir)
        except ValueError:
            pass


def _build_chrome_options(resolution: DriverResolution) -> tuple[Options, str | None]:
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-browser-side-navigation")
    chrome_options.add_argument("--window-size=1200,1200")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    temp_profile_dir = None
    if resolution.browser_binary:
        chrome_options.binary_location = resolution.browser_binary

    is_linux = platform.system().lower() == "linux"
    if is_linux:
        chrome_options.add_argument("--disable-setuid-sandbox")

    # У Linux headless часто стабильнее с отдельным профилем.
    if is_linux:
        temp_profile_dir = tempfile.mkdtemp(prefix="tps-chrome-profile-")
        chrome_options.add_argument(f"--user-data-dir={temp_profile_dir}")
        chrome_options.add_argument(f"--data-path={temp_profile_dir}")
        chrome_options.add_argument("--remote-debugging-port=0")

    return chrome_options, temp_profile_dir


def _trim_chromedriver_log_if_needed() -> None:
    try:
        if not _CHROMEDRIVER_LOG_PATH.exists():
            return
        if _CHROMEDRIVER_LOG_PATH.stat().st_size <= _CHROMEDRIVER_LOG_MAX_BYTES:
            return

        with _CHROMEDRIVER_LOG_PATH.open("r+", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
            if len(lines) <= _CHROMEDRIVER_LOG_KEEP_LINES:
                return
            tail = lines[-_CHROMEDRIVER_LOG_KEEP_LINES :]
            fh.seek(0)
            fh.writelines(tail)
            fh.truncate()
    except Exception as exc:
        logger.warning("Не удалось обрезать chromedriver.log: %s", exc)


def _create_driver_sync(resolution: DriverResolution) -> webdriver.Chrome:
    _trim_chromedriver_log_if_needed()
    chrome_options, temp_profile_dir = _build_chrome_options(resolution)

    if resolution.driver_path:
        service = Service(
            executable_path=str(resolution.driver_path),
            service_args=["--verbose", "--log-path=chromedriver.log"],
        )
        driver = webdriver.Chrome(service=service, options=chrome_options)
        _mark_driver(driver, temp_profile_dir)
        return driver

    driver = webdriver.Chrome(options=chrome_options)
    _mark_driver(driver, temp_profile_dir)
    return driver


def _build_candidate_urls(url: str) -> list[str]:
    urls: list[str] = []
    normalized = url.strip()
    if not normalized.startswith("http"):
        normalized = "https://" + normalized
    urls.append(normalized)

    match = re.match(r"https?://t\.me/([^/?#]+)/([\d/]+)", normalized)
    if match:
        slug, ids = match.group(1), match.group(2)
        post_id = ids.split("/")[0]
        urls.append(f"https://t.me/s/{slug}/{post_id}")

    if "embed=1" not in normalized:
        urls.append(normalized + ("&embed=1" if "?" in normalized else "?embed=1"))
        urls.append(normalized + ("&embed=1&mode=tme" if "?" in normalized else "?embed=1&mode=tme"))

    if "?single" not in normalized and "&single" not in normalized:
        urls.append(normalized + ("&single" if "?" in normalized else "?single"))

    unique: list[str] = []
    seen: set[str] = set()
    for item in urls:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def _selenium_capture(driver: webdriver.Chrome, url: str) -> bytes:
    candidates = _build_candidate_urls(url)
    last_exc: Exception | None = None
    post_element = None
    active_frame = None

    def _wait_and_pick() -> object | None:
        WebDriverWait(driver, 20).until(
            EC.any_of(
                EC.presence_of_element_located((By.CLASS_NAME, "tgme_widget_message")),
                EC.presence_of_element_located((By.CLASS_NAME, "tgme_widget_message_wrap")),
                EC.presence_of_element_located((By.CLASS_NAME, "tgme_page_widget")),
            )
        )
        for cls in ("tgme_widget_message", "tgme_widget_message_wrap", "tgme_page_widget"):
            try:
                return driver.find_element(By.CLASS_NAME, cls)
            except Exception:
                continue
        return None

    for attempt_url in candidates:
        try:
            driver.switch_to.default_content()
        except Exception:
            pass

        driver.get(attempt_url)
        try:
            post_element = _wait_and_pick()
            active_frame = None
        except TimeoutException as exc:
            last_exc = exc
            post_element = None

        if post_element is None:
            frames = driver.find_elements(
                By.CSS_SELECTOR,
                "iframe[src*='embed'], iframe[src*='widget'], iframe[src*='t.me'], iframe[src*='telegram']",
            )
            for frame in frames:
                try:
                    driver.switch_to.default_content()
                    driver.switch_to.frame(frame)
                    post_element = _wait_and_pick()
                    if post_element is not None:
                        active_frame = frame
                        break
                except TimeoutException as exc:
                    last_exc = exc
                    post_element = None
                except Exception:
                    post_element = None

        if post_element is not None:
            break

    if post_element is None:
        raise last_exc or TimeoutException("Не удалось найти элемент поста на странице")

    if active_frame is not None:
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(active_frame)
        except Exception:
            pass

    driver.execute_script(
        """
        const el = arguments[0];
        (function clearBackgrounds(root){
          const all = [root, ...root.querySelectorAll('*')];
          for (const e of all){
            e.style.background = 'transparent';
            e.style.backgroundColor = 'transparent';
            e.style.boxShadow = 'none';
            e.style.filter = 'none';
          }
        })(el);
        document.documentElement.style.background = '#39ff00';
        document.body.style.background = '#39ff00';
        document.body.style.overflow = 'visible';
        document.documentElement.style.overflow = 'visible';
        document.querySelector('.tgme_background_wrap')?.remove();
        document.querySelector('.tgme_page_extra')?.remove();
        document.querySelector('.tgme_page_widget_actions_wrap')?.remove();
        return true;
        """,
        post_element,
    )

    rect = None
    for _ in range(20):
        try:
            rect = driver.execute_script(
                "const r = arguments[0].getBoundingClientRect(); return {left:r.left, top:r.top, width:r.width, height:r.height};",
                post_element,
            )
        except Exception:
            rect = None
        if rect and rect["width"] > 1 and rect["height"] > 1:
            break
        time.sleep(0.1)

    try:
        screenshot = post_element.screenshot_as_png
    except WebDriverException as exc:
        logger.warning("Element screenshot failed (%s). Using viewport crop fallback.", exc)
        if rect is None:
            raise
        scale = driver.execute_script("return window.devicePixelRatio") or 1.0
        full_png = driver.get_screenshot_as_png()
        full_img = Image.open(io.BytesIO(full_png))

        left = max(int(rect["left"] * scale), 0)
        top = max(int(rect["top"] * scale), 0)
        width = max(int(rect["width"] * scale), 1)
        height = max(int(rect["height"] * scale), 1)
        right = min(left + width, full_img.width)
        bottom = min(top + height, full_img.height)
        if right <= left or bottom <= top:
            raise

        cropped = full_img.crop((left, top, right, bottom))
        buf = io.BytesIO()
        cropped.save(buf, format="PNG")
        screenshot = buf.getvalue()

    return screenshot


def _remove_green_pixels_sync(png_bytes: bytes) -> Image.Image:
    try:
        import numpy as np

        use_numpy = True
    except Exception:
        use_numpy = False

    img = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    max_dim = 1200
    if max(img.size) > max_dim:
        img.thumbnail((max_dim, max_dim), Image.LANCZOS)

    if use_numpy:
        arr = np.array(img)
        r = arr[..., 0].astype("int16")
        g = arr[..., 1].astype("int16")
        b = arr[..., 2].astype("int16")
        mask = (r >= 30) & (r <= 90) & (g >= 220) & (b <= 50) & (g > r + 20) & (g > b + 20)
        arr[mask, 3] = 0
        return Image.fromarray(arr, mode="RGBA")

    datas = img.getdata()
    new_data = []
    for r, g, b, a in datas:
        if 30 <= r <= 90 and g >= 220 and b <= 50 and g > r + 20 and g > b + 20:
            new_data.append((0, 0, 0, 0))
        else:
            new_data.append((r, g, b, a))
    img.putdata(new_data)
    return img


async def preflight_driver_startup() -> None:
    global _driver_resolution, _driver_error_hint
    try:
        _driver_resolution = await asyncio.to_thread(_driver_manager.preflight)
        _driver_error_hint = None
        driver_logger.info(
            "Driver preflight ok. browser_major=%s, local_driver=%s, selenium_manager=%s",
            _driver_resolution.browser_major,
            _driver_resolution.driver_path,
            _driver_resolution.used_selenium_manager,
        )
    except BrowserNotFoundError as exc:
        _driver_error_hint = str(exc)
        driver_logger.warning("Chrome/Chromium не найден: %s", exc)
    except Exception as exc:
        _driver_error_hint = f"Preflight драйвера завершился ошибкой: {exc}"
        driver_logger.exception("Ошибка preflight драйвера")


async def _ensure_driver(*, force_refresh: bool = False) -> None:
    global _driver, _driver_resolution, _process_pool, _driver_error_hint

    if _process_pool is None:
        from concurrent.futures import ProcessPoolExecutor

        _process_pool = ProcessPoolExecutor(max_workers=1)

    async with _driver_boot_lock:
        if force_refresh and _driver is not None:
            stale_driver = _driver
            _driver = None
            await asyncio.to_thread(stale_driver.quit)
            await asyncio.to_thread(_cleanup_profile_from_driver, stale_driver)

        if _driver is not None:
            return

        try:
            _driver_resolution = await asyncio.to_thread(
                partial(_driver_manager.resolve_driver, force_refresh=force_refresh)
            )
            _driver_error_hint = None
        except BrowserNotFoundError as exc:
            _driver_error_hint = str(exc)
            raise

        _driver = await asyncio.to_thread(_create_driver_sync, _driver_resolution)


async def capture_telegram_post(url: str, progress_message: types.Message, cid: str) -> tuple[str, bytes] | None:
    global _driver
    logger.info("[cid=%s] Запуск обработки поста: %s", cid, url)
    await progress_message.edit_text("🔄 Запускаю обработку поста...")

    try:
        await _ensure_driver(force_refresh=False)
    except BrowserNotFoundError:
        hint = _driver_error_hint or _driver_manager.browser_install_hint()
        await progress_message.edit_text(f"❌ Chrome/Chromium не найден. {hint}")
        return None
    except Exception as exc:
        logger.exception("[cid=%s] Ошибка инициализации браузера: %s", cid, exc)
        await progress_message.edit_text("❌ Ошибка инициализации браузера")
        return None

    await progress_message.edit_text("⏳ Ожидание загрузки...")
    async with _driver_lock:
        try:
            _trim_chromedriver_log_if_needed()
            post_screenshot = await asyncio.to_thread(_selenium_capture, _driver, url)
        except SessionNotCreatedException:
            logger.warning("[cid=%s] SessionNotCreated, перезапускаем драйвер", cid)
            try:
                await _ensure_driver(force_refresh=True)
                post_screenshot = await asyncio.to_thread(_selenium_capture, _driver, url)
            except Exception as exc:
                logger.exception("[cid=%s] Повтор после переустановки драйвера не удался: %s", cid, exc)
                await progress_message.edit_text(f"❌ Не удалось обновить ChromeDriver: {exc}")
                return None
        except Exception as exc:
            logger.exception("[cid=%s] Ошибка захвата страницы: %s", cid, exc)
            stale_driver = _driver
            _driver = None
            if stale_driver is not None:
                try:
                    await asyncio.to_thread(stale_driver.quit)
                    await asyncio.to_thread(_cleanup_profile_from_driver, stale_driver)
                except Exception:
                    pass
            await progress_message.edit_text(f"❌ Ошибка при загрузке страницы: {str(exc)[:200]}")
            return None

    await progress_message.edit_text("📸 Делаю скриншот поста...")
    loop = asyncio.get_running_loop()
    try:
        img = await loop.run_in_executor(_process_pool, _remove_green_pixels_sync, post_screenshot)
    except Exception as exc:
        logger.exception("[cid=%s] Ошибка обработки изображения: %s", cid, exc)
        await progress_message.edit_text(f"❌ Ошибка при обработке изображения: {str(exc)[:200]}")
        return None

    cleaned_url = re.sub(r"[^a-zA-Z0-9]", "_", url.split("t.me/")[-1].split("?embed=1")[0]) or "post"
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)

    await progress_message.edit_text("✅ Изображение готово!")
    return cleaned_url + ".png", buf.read()


def validate_tg_link(text: str) -> str | None:
    match = re.match(r"^(https://)?(t\.me|telegram\.me)/[\w\d_]+(/\d+)+$", text.strip())
    if not match:
        return None
    return f"https://{text.strip()}" if not text.startswith("https://") else text.strip()


def _is_admin(user_id: int) -> bool:
    return user_id in settings.bot.admin_ids


def _admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Live-сводка", callback_data="admin:live")],
            [InlineKeyboardButton(text="👤 Последние пользователи", callback_data="admin:users")],
            [InlineKeyboardButton(text="🕒 Последние действия", callback_data="admin:events")],
            [InlineKeyboardButton(text="⚠️ Ошибки", callback_data="admin:errors")],
            [InlineKeyboardButton(text="🔎 Поиск пользователя", callback_data="admin:find_help")],
            [InlineKeyboardButton(text="🗜 Сжать историю", callback_data="admin:compact")],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data="admin:refresh")],
        ]
    )


def _format_live_summary() -> str:
    stats = db_stats()
    warning = db_size_warning()
    lines = [
        "<b>Live-сводка</b>",
        f"Пользователей: <b>{stats.get('total_users', 0)}</b>",
        f"Событий: <b>{stats.get('total_events', 0)}</b>",
        f"Успешных захватов: <b>{stats.get('capture_success', 0)}</b>",
        f"Ошибок захвата: <b>{stats.get('capture_failed', 0)}</b>",
        f"Просмотров админки: <b>{stats.get('admin_views', 0)}</b>",
    ]
    if warning:
        lines.append(f"\n⚠️ {warning}")
    return "\n".join(lines)


def _format_users_page() -> tuple[str, InlineKeyboardMarkup]:
    users = db_recent_users(10)
    lines = ["<b>Последние пользователи</b>"]
    keyboard_rows: list[list[InlineKeyboardButton]] = []

    if not users:
        lines.append("Пока нет пользователей.")
    else:
        for key, data in users:
            username = data.get("username", "None")
            fullname = data.get("fullname", "*")
            last_seen = data.get("last_seen", "*")
            lines.append(f"• <code>{key}</code> @{username} ({fullname})\n  last_seen: {last_seen}")
            keyboard_rows.append([InlineKeyboardButton(text=f"Открыть {key}", callback_data=f"admin:user:{key}")])

    keyboard_rows.append([InlineKeyboardButton(text="⬅️ В меню", callback_data="admin:refresh")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=keyboard_rows)


def _format_events_page(*, status: str | None = None) -> str:
    events = db_recent_events(20, status=status)
    title = "Ошибки" if status == "error" else "Последние действия"
    lines = [f"<b>{title}</b>"]

    if not events:
        lines.append("События не найдены.")
        return "\n".join(lines)

    for event in events:
        lines.append(
            "• {timestamp} | uid={user_id} | @{username} | {action} | {status}".format(
                timestamp=event.get("timestamp", "?"),
                user_id=event.get("user_id", "?"),
                username=event.get("username", "None"),
                action=event.get("action", "?"),
                status=event.get("status", "?"),
            )
        )
    return "\n".join(lines)


@router_main.message(Command("start"))
async def start(message: types.Message) -> None:
    if message.chat.type == "private" and message.from_user:
        user_id = message.from_user.id
        username = message.from_user.username
        fullname = message.from_user.first_name
        db_register(user_id, username, fullname)

        current_time = datetime.now().strftime("%H:%M %d.%m.%Y")
        first_login = db_get_value(user_id, "first_login")
        if first_login == "*":
            db_change(user_id, "first_login", current_time)

        db_log_event(
            user_id=user_id,
            username=username,
            action="user_started_bot",
            status="ok",
            meta={},
        )

    await message.answer(
        "Отправьте ссылку на пост в Telegram в формате:\n\n"
        "✅ <code>https://t.me/channel/post</code>\n"
        "✅ <code>t.me/channel/post</code>",
        disable_web_page_preview=True,
        parse_mode="HTML",
    )


@router_main.message(Command("admin"))
async def admin_panel(message: types.Message) -> None:
    if not message.from_user or not _is_admin(message.from_user.id):
        await message.answer("⛔ Доступ запрещён.")
        return

    db_log_event(
        user_id=message.from_user.id,
        username=message.from_user.username,
        action="admin_opened_panel",
        status="ok",
        meta={},
    )
    await message.answer(_format_live_summary(), parse_mode="HTML", reply_markup=_admin_menu())


@router_main.message(Command("admin_find"))
async def admin_find_user(message: types.Message) -> None:
    if not message.from_user or not _is_admin(message.from_user.id):
        await message.answer("⛔ Доступ запрещён.")
        return

    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: /admin_find <user_id|@username>")
        return

    query = parts[1]
    found = db_find_user(query)
    if not found:
        await message.answer("Пользователь не найден.")
        return

    key, user = found
    await message.answer(
        "<b>Пользователь найден</b>\n"
        f"key: <code>{key}</code>\n"
        f"username: @{user.get('username', 'None')}\n"
        f"fullname: {user.get('fullname', '*')}\n"
        f"role: {user.get('role', 'member')}\n"
        f"first_login: {user.get('first_login', '*')}\n"
        f"last_seen: {user.get('last_seen', '*')}",
        parse_mode="HTML",
    )


@router_main.callback_query(F.data.startswith("admin:"))
async def admin_callbacks(callback: types.CallbackQuery) -> None:
    if not callback.from_user or not _is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    if callback.message is None:
        await callback.answer()
        return

    action = callback.data.split(":", maxsplit=2)
    section = action[1] if len(action) > 1 else "refresh"

    db_log_event(
        user_id=callback.from_user.id,
        username=callback.from_user.username,
        action="admin_viewed_section",
        status="ok",
        meta={"section": section},
    )

    if section in {"live", "refresh"}:
        await callback.message.edit_text(_format_live_summary(), parse_mode="HTML", reply_markup=_admin_menu())
    elif section == "users":
        text, markup = _format_users_page()
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    elif section == "events":
        await callback.message.edit_text(
            _format_events_page(), parse_mode="HTML", reply_markup=_admin_menu()
        )
    elif section == "errors":
        await callback.message.edit_text(
            _format_events_page(status="error"),
            parse_mode="HTML",
            reply_markup=_admin_menu(),
        )
    elif section == "find_help":
        await callback.message.edit_text(
            "Поиск пользователя:\nИспользуйте команду <code>/admin_find &lt;user_id|@username&gt;</code>",
            parse_mode="HTML",
            reply_markup=_admin_menu(),
        )
    elif section == "compact":
        removed = db_compact_events(keep_last=2000)
        await callback.message.edit_text(
            f"История событий сжата. Удалено записей: <b>{removed}</b>",
            parse_mode="HTML",
            reply_markup=_admin_menu(),
        )
    elif section == "user" and len(action) == 3:
        user_key = action[2]
        found = db_find_user(user_key)
        if not found and user_key.startswith("tps_"):
            found = db_find_user(user_key.replace("tps_", ""))
        if found:
            key, user = found
            await callback.message.edit_text(
                "<b>Карточка пользователя</b>\n"
                f"key: <code>{key}</code>\n"
                f"username: @{user.get('username', 'None')}\n"
                f"fullname: {user.get('fullname', '*')}\n"
                f"role: {user.get('role', 'member')}\n"
                f"first_login: {user.get('first_login', '*')}\n"
                f"last_seen: {user.get('last_seen', '*')}",
                parse_mode="HTML",
                reply_markup=_admin_menu(),
            )
        else:
            await callback.message.edit_text(
                "Пользователь не найден.", parse_mode="HTML", reply_markup=_admin_menu()
            )

    await callback.answer()


@router_main.message()
async def link_check(message: types.Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id
    username = message.from_user.username
    fullname = message.from_user.first_name
    db_register(user_id, username, fullname)

    try:
        await handle_post_link(message)
    except Exception as exc:
        logger.exception("Ошибка в обработке сообщения: %s", exc)
        db_log_event(
            user_id=user_id,
            username=username,
            action="runtime_error",
            status="error",
            meta={"error": str(exc)[:200]},
        )


async def handle_post_link(message: types.Message) -> None:
    if not message.from_user:
        return

    user_id = message.from_user.id
    username = message.from_user.username
    cid = uuid.uuid4().hex[:8]

    url = validate_tg_link(message.text or "")
    db_log_event(
        user_id=user_id,
        username=username,
        action="link_received",
        status="ok" if url else "error",
        meta={"text": (message.text or "")[:200]},
    )

    if not url:
        await message.answer(
            "❌ Некорректный формат ссылки!\n\n"
            "Пример правильной ссылки:\n"
            "✅ <code>https://t.me/channel/12345</code>\n"
            "✅ <code>t.me/channel/12345</code>",
            disable_web_page_preview=True,
            parse_mode="HTML",
        )
        return

    is_first_image = await _consume_first_image_flag()
    if is_first_image:
        await message.answer(
            "⚠️ Внимание: это первое изображение после запуска бота.\n"
            "Иногда оно может отобразиться некорректно.\n"
            "Если результат будет неправильным, отправьте ту же ссылку ещё раз — обычно всё работает нормально."
        )

    progress_message = await message.answer("🔄 Обрабатываю пост...")
    result = await capture_telegram_post(url, progress_message, cid)

    if result:
        filename, data = result
        await progress_message.edit_text("📤 Отправляю скриншот...")
        await message.answer_document(BufferedInputFile(data, filename=filename))
        await progress_message.edit_text("✅ Скриншот успешно отправлен!")
        db_log_event(
            user_id=user_id,
            username=username,
            action="capture_success",
            status="ok",
            meta={"url": url},
        )
    else:
        await progress_message.edit_text("❌ Не удалось обработать пост. Попробуйте еще раз!")
        db_log_event(
            user_id=user_id,
            username=username,
            action="capture_failed",
            status="error",
            meta={"url": url},
        )
