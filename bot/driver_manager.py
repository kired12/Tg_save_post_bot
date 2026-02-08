from __future__ import annotations

import json
import logging
import platform
import re
import shutil
import stat
import subprocess
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen

from config import DriverSettings


logger = logging.getLogger("tps.driver")

_CFT_MILESTONES_URL = (
    "https://googlechromelabs.github.io/chrome-for-testing/"
    "latest-versions-per-milestone-with-downloads.json"
)


class BrowserNotFoundError(RuntimeError):
    pass


class DriverInstallError(RuntimeError):
    pass


@dataclass(frozen=True)
class PlatformTarget:
    os_name: str
    arch: str
    cft_platform: str
    executable_name: str

    @property
    def folder_name(self) -> str:
        return f"{self.os_name}-{self.arch}"


@dataclass(frozen=True)
class DriverResolution:
    driver_path: Path | None
    browser_binary: str
    browser_major: int
    used_selenium_manager: bool


class DriverManager:
    def __init__(self, settings: DriverSettings) -> None:
        self.settings = settings
        self.chromedriver_dir = settings.chromedriver_dir
        self.chromedriver_dir.mkdir(parents=True, exist_ok=True)

    def preflight(self) -> DriverResolution:
        return self.resolve_driver(force_refresh=False)

    def resolve_driver(self, *, force_refresh: bool) -> DriverResolution:
        browser_binary = self._resolve_browser_binary()
        browser_major = self._browser_major_version(browser_binary)

        if self.settings.chromedriver_path_override:
            override = Path(self.settings.chromedriver_path_override)
            if override.exists():
                return DriverResolution(
                    driver_path=override,
                    browser_binary=browser_binary,
                    browser_major=browser_major,
                    used_selenium_manager=False,
                )

        target = self._current_target()
        destination = self.chromedriver_dir / target.folder_name / target.executable_name
        destination.parent.mkdir(parents=True, exist_ok=True)

        if not force_refresh and destination.exists():
            current_major = self._driver_major_version(destination)
            if current_major == browser_major:
                return DriverResolution(
                    driver_path=destination,
                    browser_binary=browser_binary,
                    browser_major=browser_major,
                    used_selenium_manager=False,
                )

        try:
            self._download_and_install_for_major(browser_major, target, destination)
            installed_major = self._driver_major_version(destination)
            if installed_major != browser_major:
                raise DriverInstallError(
                    f"После установки версия драйвера {installed_major}, ожидалась {browser_major}."
                )
            return DriverResolution(
                driver_path=destination,
                browser_binary=browser_binary,
                browser_major=browser_major,
                used_selenium_manager=False,
            )
        except Exception as exc:
            logger.warning(
                "Автоустановка ChromeDriver не удалась, переключаемся на Selenium Manager: %s",
                exc,
            )
            return DriverResolution(
                driver_path=None,
                browser_binary=browser_binary,
                browser_major=browser_major,
                used_selenium_manager=True,
            )

    def browser_install_hint(self) -> str:
        system = platform.system().lower()
        if system == "darwin":
            return "Установите Google Chrome: brew install --cask google-chrome"
        if system == "linux":
            return "Установите Google Chrome/Chromium через пакетный менеджер вашей ОС."
        if system == "windows":
            return "Установите Google Chrome с https://www.google.com/chrome/"
        return "Установите Google Chrome или Chromium и перезапустите бота."

    def _resolve_browser_binary(self) -> str:
        if self.settings.chrome_binary:
            binary = Path(self.settings.chrome_binary)
            if binary.exists():
                return str(binary)

        candidates = self._browser_candidates_for_system()
        for candidate in candidates:
            resolved = shutil.which(candidate)
            if resolved:
                return resolved
            candidate_path = Path(candidate)
            if candidate_path.exists():
                return str(candidate_path)

        raise BrowserNotFoundError(self.browser_install_hint())

    @staticmethod
    def _browser_candidates_for_system() -> list[str]:
        system = platform.system().lower()
        if system == "darwin":
            return [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Chromium.app/Contents/MacOS/Chromium",
                "google-chrome",
                "chromium",
            ]
        if system == "linux":
            return ["google-chrome", "google-chrome-stable", "chromium-browser", "chromium"]
        if system == "windows":
            return [
                "chrome",
                r"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
                r"C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
            ]
        return ["google-chrome", "chromium"]

    @staticmethod
    def _extract_major(version_output: str, name: str) -> int:
        match = re.search(r"(\d+)\.", version_output)
        if not match:
            raise RuntimeError(f"Не удалось определить major-версию {name}: {version_output}")
        return int(match.group(1))

    def _browser_major_version(self, binary: str) -> int:
        try:
            completed = subprocess.run(
                [binary, "--version"],
                check=True,
                capture_output=True,
                text=True,
            )
            return self._extract_major(completed.stdout.strip() or completed.stderr.strip(), "браузера")
        except (subprocess.SubprocessError, OSError) as exc:
            raise BrowserNotFoundError(f"Не удалось получить версию браузера: {exc}") from exc

    def _driver_major_version(self, driver: Path) -> int:
        try:
            completed = subprocess.run(
                [str(driver), "--version"],
                check=True,
                capture_output=True,
                text=True,
            )
            return self._extract_major(completed.stdout.strip() or completed.stderr.strip(), "драйвера")
        except (subprocess.SubprocessError, OSError) as exc:
            raise DriverInstallError(f"Не удалось получить версию драйвера {driver}: {exc}") from exc

    @staticmethod
    def _current_target() -> PlatformTarget:
        system = platform.system().lower()
        machine = platform.machine().lower()

        if system == "darwin":
            arch = "arm64" if "arm" in machine else "x64"
            cft_platform = "mac-arm64" if arch == "arm64" else "mac-x64"
            return PlatformTarget("mac", arch, cft_platform, "chromedriver")

        if system == "linux":
            return PlatformTarget("linux", "x64", "linux64", "chromedriver")

        if system == "windows":
            arch = "x64" if any(token in machine for token in ("amd64", "x86_64", "x64")) else "x86"
            cft_platform = "win64" if arch == "x64" else "win32"
            return PlatformTarget("win", arch, cft_platform, "chromedriver.exe")

        raise DriverInstallError(f"Неподдерживаемая ОС: {system}")

    def _download_and_install_for_major(
        self,
        browser_major: int,
        target: PlatformTarget,
        destination: Path,
    ) -> None:
        metadata = self._fetch_json(_CFT_MILESTONES_URL)
        milestones = metadata.get("milestones", {})
        milestone = milestones.get(str(browser_major))
        if not milestone:
            raise DriverInstallError(f"Для major-версии {browser_major} нет данных в Chrome for Testing.")

        downloads = milestone.get("downloads", {}).get("chromedriver", [])
        selected = next((item for item in downloads if item.get("platform") == target.cft_platform), None)
        if not selected:
            raise DriverInstallError(f"Нет chromedriver для платформы {target.cft_platform}.")

        download_url = selected.get("url")
        if not isinstance(download_url, str) or not download_url.startswith("https://storage.googleapis.com/chrome-for-testing-public/"):
            raise DriverInstallError("Обнаружен небезопасный URL для скачивания драйвера.")

        with tempfile.TemporaryDirectory(prefix="tps-driver-") as tmp_dir_name:
            tmp_dir = Path(tmp_dir_name)
            archive_path = tmp_dir / "chromedriver.zip"
            self._download_with_retries(download_url, archive_path)

            with zipfile.ZipFile(archive_path, "r") as archive:
                archive.extractall(tmp_dir)

            extracted = self._find_extracted_binary(tmp_dir, target.executable_name)
            if not extracted:
                raise DriverInstallError("После распаковки не найден бинарник chromedriver.")

            final_tmp = destination.with_suffix(destination.suffix + ".tmp")
            final_tmp.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(extracted, final_tmp)
            if target.executable_name == "chromedriver":
                final_tmp.chmod(final_tmp.stat().st_mode | stat.S_IEXEC)
            final_tmp.replace(destination)

        self._write_metadata(
            destination=destination,
            browser_major=browser_major,
            source_url=download_url,
            platform_name=target.cft_platform,
        )

    @staticmethod
    def _find_extracted_binary(tmp_dir: Path, executable_name: str) -> Path | None:
        for path in tmp_dir.rglob(executable_name):
            if path.is_file():
                return path
        return None

    @staticmethod
    def _download_with_retries(url: str, destination: Path, retries: int = 3) -> None:
        last_error: Exception | None = None
        for _ in range(retries):
            try:
                with urlopen(url, timeout=30) as response:
                    destination.write_bytes(response.read())
                return
            except (URLError, TimeoutError, OSError) as exc:
                last_error = exc
        raise DriverInstallError(f"Не удалось скачать драйвер после {retries} попыток: {last_error}")

    @staticmethod
    def _fetch_json(url: str) -> dict[str, Any]:
        try:
            with urlopen(url, timeout=20) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            raise DriverInstallError(f"Не удалось получить метаданные Chrome for Testing: {exc}") from exc

    def _write_metadata(self, *, destination: Path, browser_major: int, source_url: str, platform_name: str) -> None:
        metadata_path = self.chromedriver_dir / "metadata.json"
        metadata: dict[str, Any]
        if metadata_path.exists():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                metadata = {}
        else:
            metadata = {}

        records = metadata.setdefault("drivers", {})
        records[platform_name] = {
            "path": str(destination),
            "browser_major": browser_major,
            "download_url": source_url,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        metadata_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
