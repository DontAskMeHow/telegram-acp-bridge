"""Сетевой слой моста tg: проверка связи с Telegram, фолбэк на прокси.

Сначала проверяется связь с api.telegram.org напрямую; если её нет —
включается прокси из конфига (поле proxy) или из окружения (PROXY /
HTTPS_PROXY) и проверка повторяется.
"""

import json
import os
from pathlib import Path

import aiohttp
from aiohttp_socks import ProxyConnector

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "tg.local.json"
CHECK_URL = "https://api.telegram.org/bot000/getMe"
CHECK_TIMEOUT = 8


def proxy_from_env():
    """Прокси из переменных окружения (PROXY, HTTPS_PROXY, https_proxy)."""
    for var in ("PROXY", "HTTPS_PROXY", "https_proxy"):
        val = os.environ.get(var)
        if val:
            return val
    return None


def load_config(required=True):
    if not CONFIG_PATH.exists():
        if required:
            raise SystemExit(
                f"Нет конфигурации {CONFIG_PATH}.\n"
                "Создайте её: {\n  \"bot_token\": \"<токен от @BotFather>\",\n"
                "  \"allowed_user_ids\": []\n}\n"
                "(файл хранится локально и не попадает в git)")
        return {}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def save_config(cfg):
    CONFIG_PATH.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


async def _reachable(session):
    try:
        async with session.get(
                CHECK_URL, timeout=aiohttp.ClientTimeout(total=CHECK_TIMEOUT)) as r:
            return r.status is not None
    except Exception:
        return False


async def check_direct():
    async with aiohttp.ClientSession() as s:
        return await _reachable(s)


async def check_proxy(proxy_url):
    try:
        async with aiohttp.ClientSession(
                connector=ProxyConnector.from_url(proxy_url)) as s:
            return await _reachable(s)
    except Exception:
        return False


async def ensure_network(cfg=None):
    """Проверка связи и выбор маршрута.

    Возвращает (route, proxy_url, connector_factory), где route — "direct"
    или "proxy", connector_factory — функция без аргументов, создающая
    aiohttp-коннектор для aiogram, либо None для прямого соединения.
    Выбрасывает SystemExit, если Telegram недоступен ни напрямую, ни через
    прокси.
    """
    cfg = cfg if cfg is not None else load_config()
    if await check_direct():
        return "direct", None, None
    proxy = cfg.get("proxy") or proxy_from_env()
    if proxy:
        if await check_proxy(proxy):
            return "proxy", proxy, (lambda: ProxyConnector.from_url(proxy))
    raise SystemExit(
        "Telegram недоступен ни напрямую, ни через прокси "
        f"(использован прокси: {proxy!r}). Проверьте VPN и адрес прокси "
        f"в {CONFIG_PATH} (поле proxy) или в PROXY/HTTPS_PROXY.")
