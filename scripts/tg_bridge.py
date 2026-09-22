"""Мост Telegram <-> kimi (ACP): управление kimi с телефона.

Демон: aiogram-бот (Bot API, long-polling) в простое kimi постоянно НЕ держит:
процесс `kimi acp` (JSON-RPC ACP поверх stdio) поднимается при получении
сообщения владельца и после ответа живёт ещё час (по умолчанию) — если
диалог в течение часа продолжен, сессия уже warm и продолжается мгновенно
с того же места; если час прошёл без сообщений — kimi закрывается, а
следующее сообщение поднимает сессию заново с диска (session/load). Сообщение -> сессия kimi в воркспейсе (чат в Telegram = сессия
kimi; контекст сохраняется на диске и поднимается по session/load к
следующему сообщению), ход работы виден в чате редактируемым сообщением
(мысли, инструменты, ответ), по завершении — полный ответ и затраченный
контекст, процесс kimi закрывается.

При каждом старте проверяется связь с api.telegram.org; если её нет —
используется прокси из конфига; недоступны оба канала — мост не падает,
а повторяет попытку каждые net_retry_sec (по умолчанию 180 с, конфиг);
во время работы мост сам переключается прямой/прокси (net_watchdog).

Команды:
  python tg_bridge.py install       # задача Планировщика tg-bridge (вход в систему)
  python tg_bridge.py start         # демон (вызывается задачей или вручную)
  python tg_bridge.py stop          # остановить демон
  python tg_bridge.py status        # состояние + хвост лога
  python tg_bridge.py log           # хвост лога
  python tg_bridge.py uninstall     # снять задачу и остановить демон
"""

import argparse
import asyncio
import base64
import html as html_mod
import io
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
WORKSPACE = str(REPO_ROOT)  # воркспейс по умолчанию: корень чек-аута репозитория
DATA = REPO_ROOT / "data"
LOG_PATH = DATA / "tg.log"
STATE_PATH = DATA / "bridge.state.json"
PID_PATH = DATA / "bridge.pid"
TASK_NAME = "tg-bridge"
LOG_LIMIT = 512 * 1024

sys.path.insert(0, str(ROOT))
from tg_net import CONFIG_PATH, load_config, save_config, proxy_from_env  # noqa: E402
from tg_net import check_direct, check_proxy, ensure_network  # noqa: E402
from kimi_acp import AcpClient, AcpError  # noqa: E402

MAX_MSG = 3800  # лимит сообщения Telegram 4096, с запасом
NO_WINDOW = 0x08000000 if os.name == "nt" else 0
POLL_RESTART_SEC = 5  # пауза перед рестартом polling после падения

MODE_DESCRIPTIONS = {
    "default": "обычный режим (в ACP безопасные действия и так идут без спроса)",
    "plan": "только чтение и план — инструменты изменений недоступны",
    "auto": "автоодобрение безопасных операций",
    "yolo": "автоодобрение всего",
}

_log_lock = threading.Lock()


def esc(s):
    return html_mod.escape(s)


def _now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _fix_console():
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        if stream is None:
            setattr(sys, name, open(os.devnull, "w", encoding="utf-8", errors="replace"))
        else:
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass


def log(msg):
    line = f"[{_now()}] {msg}"
    DATA.mkdir(parents=True, exist_ok=True)
    with _log_lock:
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        try:
            if LOG_PATH.stat().st_size > LOG_LIMIT:
                text = LOG_PATH.read_text(encoding="utf-8")
                LOG_PATH.write_text(text[-LOG_LIMIT // 2:], encoding="utf-8")
        except Exception:
            pass
    if sys.stdout is not None:
        try:
            print(line, flush=True)
        except Exception:
            pass


def _setup_aiogram_logging():
    """Логи aiogram (сетевые ретраи polling и т.п.) — в файл.

    Демон живёт в pythonw: stdout/stderr нет, поэтому без этого хвоста
    внутренние ошибки aiogram невидимы — именно так прошлый зависон удалось
    найти только по косвенным признакам.
    """
    try:
        import logging
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler(
            DATA / "aiogram.log", maxBytes=256 * 1024, backupCount=1,
            encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s"))
        logging.basicConfig(level=logging.INFO, handlers=[fh], force=True)
    except Exception:
        pass


def read_state():
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def write_state(**kw):
    DATA.mkdir(parents=True, exist_ok=True)
    state = read_state()
    state.update(kw)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def _pid_alive(pid):
    if not pid:
        return False
    if os.name == "nt":
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
                capture_output=True, text=True, encoding="utf-8", errors="replace",
                creationflags=NO_WINDOW,
            ).stdout
            return str(pid) in out
        except Exception:
            return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _chunk(text, limit=MAX_MSG):
    """Разбить текст на куски по строкам, не длиннее limit."""
    if not text:
        return ["(пусто)"]
    if len(text) <= limit:
        return [text]
    parts = []
    cur = ""
    for line in text.splitlines(keepends=True):
        if len(cur) + len(line) > limit and cur:
            parts.append(cur)
            cur = ""
        while len(line) > limit:
            if cur:
                parts.append(cur)
                cur = ""
            parts.append(line[:limit])
            line = line[limit:]
        cur += line
    if cur:
        parts.append(cur)
    return parts


# ---- Markdown -> HTML (для сообщений Telegram) ----

_FENCE_RE = re.compile(r"```(.*?)```", re.S)
_LINK_RE = re.compile(r"\[([^\[\]]+)\]\(([^()\s]+)\)")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")
_ITALIC_RE = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)")
_CODE_RE = re.compile(r"`([^`\n]+)`")


def _inline(s):
    """s уже экранирован: ссылки, жирный, код, затем курсив (аккуратно)."""
    s = _LINK_RE.sub(lambda m: f'<a href="{m.group(2)}">{m.group(1)}</a>', s)
    s = _BOLD_RE.sub(r"<b>\1</b>", s)
    s = _CODE_RE.sub(r"<code>\1</code>", s)
    # курсив — построчно и только при чётном числе оставшихся одиночных звёзд,
    # иначе звёздочки остаются текстом (шаблоны, пути, несбаланс из окон)
    out = []
    for line in s.split("\n"):
        if line.count("*") >= 2 and line.count("*") % 2 == 0:
            line = _ITALIC_RE.sub(r"<i>\1</i>", line)
        out.append(line)
    return "\n".join(out)


_INLINE_TAGS = {"b", "i", "code", "pre", "u", "s", "tg-spoiler", "a"}


def html_ok(html):
    """Валидная ли вложенность тегов (для Telegram parse_mode=HTML)."""
    stack = []
    for m in re.finditer(r"</?([a-z][a-z0-9-]*)(?:\s[^>]*)?>", html):
        tag, name = m.group(0), m.group(1)
        if tag.startswith("</"):
            if name not in _INLINE_TAGS:
                continue
            if not stack or stack[-1] != name:
                return False
            stack.pop()
        elif tag.endswith("/>"):
            continue
        elif name in _INLINE_TAGS:
            if name in stack:
                return False  # повторное открытие того же тега — Telegram такое не ест
            stack.append(name)
    return not stack


def md_to_html_safe(text):
    """md_to_html с гарантией валидного HTML (иначе — экранированный текст)."""
    html = md_to_html(text)
    if html_ok(html):
        return html
    return esc(text)


def md_to_html(text):
    """Распространённый Markdown kimi -> HTML Telegram."""
    parts = _FENCE_RE.split(text)
    out = []
    for i, part in enumerate(parts):
        if i % 2 == 1:  # содержимое код-блока
            body = part
            first, _, rest = body.partition("\n")
            if first.strip() and " " not in first.strip():
                body = rest  # первая строка — имя языка
            out.append("<pre>" + esc(body) + "</pre>")
            continue
        lines = []
        for line in part.splitlines():
            m = re.match(r"^\s{0,3}(#{1,6})\s+(.*)$", line)
            if m:
                # **жирный** вычёркивается заранее: строка вся в <b>...</b>,
                # вложенный <b> внутри <b> Telegram не принимает
                body = _BOLD_RE.sub(r"\1", esc(m.group(2)))
                lines.append("<b>" + _inline(body) + "</b>")
            elif re.match(r"^\s*[-*•]\s+", line):
                lines.append("• " + _inline(esc(re.sub(r"^\s*[-*•]\s+", "", line))))
            else:
                lines.append(_inline(esc(line)))
        out.append("\n".join(lines))
    return "\n".join(p for p in out if p != "")


def _split_raw(u, raw_limit):
    """Режет длинный сырой блок по границам слов (до конвертации)."""
    if len(u) <= raw_limit:
        return [u]
    out = []
    acc = ""
    for word in re.findall(r"\S+\s*", u):
        if acc and len(acc) + len(word) > raw_limit:
            out.append(acc)
            acc = word
        else:
            acc += word
    if acc:
        out.append(acc)
    return out


def html_parts(text, limit=MAX_MSG):
    """Разбивает ответ на сообщения, каждое — валидный HTML ≤ limit.

    Режем сырой текст заранее (запас под экранирование), код-блоки оставляем
    целиком — поэтому ни один HTML-тег не режется посередине.
    """
    raw_limit = int(limit * 0.75)
    chunks = []
    cur = ""
    units = _FENCE_RE.split(text)
    for i, u in enumerate(units):
        if i % 2 == 1:
            u = "```" + u + "```"
        for piece in _split_raw(u, raw_limit):
            html = md_to_html_safe(piece)
            if not html.strip():
                continue
            if cur and len(cur) + len(html) + 2 > limit:
                chunks.append(cur)
                cur = html
            else:
                cur = (cur + "\n\n" + html) if cur else html
    if cur:
        chunks.append(cur)
    return chunks or ["(нет ответа)"]


class ChatProgress:
    """Редактируемое сообщение-«терминал»: мысли, инструменты, ответ (HTML)."""

    def __init__(self, bot, chat_id):
        self.bot = bot
        self.chat_id = chat_id
        self.msg_id = None
        self.answer = ""
        self.tool_lines = []
        self.thought = ""
        self._last_edit = 0.0
        self._pending = False

    async def start(self):
        msg = await self.bot.send_message(
            chat_id=self.chat_id, text="Принято. Начинаю…")
        self.msg_id = msg.message_id

    def add_tool_line(self, line):
        self.tool_lines.append(line)
        if len(self.tool_lines) > 10:
            self.tool_lines.pop(0)
        self._pending = True

    def add_answer(self, text):
        self.answer += text
        self._pending = True

    def set_thought(self, text):
        self.thought += text
        if len(self.thought) > 12000:  # держим хвост потока мыслей
            self.thought = self.thought[-12000:]
        self._pending = True

    def _thought_view(self):
        """Читаемый хвост потока мыслей: срез по границам предложений."""
        t = self.thought.strip()
        limit = 900
        if len(t) <= limit:
            return t
        window = t[-limit:]
        m = re.search(r"[.!?…]\s+\S", window)
        if m and m.end() < len(window):
            window = window[m.end() - 1:]  # с начала предложения
        else:
            m = re.search(r"\s\S", window)
            if m:
                window = window[m.start() + 1:]
        return "…" + window

    def build(self):
        sep = "────────"
        parts = []
        if self.thought.strip():
            parts.append("<b>Мыслю…</b>\n" + md_to_html_safe(self._thought_view()))
        if self.tool_lines:
            parts.append("<b>Делаю:</b>\n" + "\n".join(
                "• " + esc(line) for line in self.tool_lines[-6:]))
        answer = self.answer[-1200:] if len(self.answer) > 1200 else self.answer
        parts.append("<b>Ответ:</b>\n" + (md_to_html_safe(answer) if answer else "…"))
        return ("\n" + sep + "\n").join(parts)

    async def _edit(self, text, force=False):
        now = time.time()
        if not force and now - self._last_edit < 2.0:
            return
        self._last_edit = now
        try:
            await self.bot.edit_message_text(
                text=text[:MAX_MSG], chat_id=self.chat_id,
                message_id=self.msg_id, parse_mode="HTML")
        except Exception as e:
            if "not modified" not in str(e):
                log(f"progress edit не удался: {e}")

    async def tick(self, force=False):
        if not self._pending and not force:
            return
        self._pending = False
        await self._edit(self.build(), force=force)

    async def _send_final(self, chunk):
        """Отправка куска финального ответа: HTML, при отказе — чистый текст.

        Валидация html_ok выше гарантирует корректную разметку, но любая
        ошибка Telegram (лимиты, edge-кейсы парсера) не должна молча съедать
        ответ — поэтому есть фолбэк без parse_mode, который не может упасть
        по разметке.
        """
        try:
            await self.bot.send_message(
                chat_id=self.chat_id, text=chunk, parse_mode="HTML")
            return
        except Exception as e:
            log(f"final send HTML не удался: {e}")
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=chunk)
        except Exception as e:
            log(f"final send plain не удался: {e}")

    async def finish(self):
        """Финальная отдача: только чистый ответ, аккуратно разделённый на сообщения."""
        chunks = html_parts(self.answer.strip()) if self.answer.strip() else ["(нет ответа)"]
        try:
            await self.bot.edit_message_text(
                text=chunks[0], chat_id=self.chat_id,
                message_id=self.msg_id, parse_mode="HTML")
        except Exception as e:
            log(f"final edit не удался: {e}")
            await self._send_final(chunks[0])
        for extra in chunks[1:]:
            await self._send_final(extra)


class TgBridge:
    def __init__(self, cfg):
        self.cfg = cfg
        self.bot = None
        self.dp = None
        self.acp = None
        self.loaded_sessions = set()
        self.turn_lock = asyncio.Lock()
        self.queue = asyncio.Queue()
        self.in_turn = False
        self.stopping = False
        self.perm_futures = {}
        self.started_at = time.time()
        self.last_usage = None
        self.last_error = None
        self.workspace = cfg.get("workspace") or WORKSPACE
        self.kimi_bin = cfg.get("kimi_bin") or None
        self.net_retry_sec = float(cfg.get("net_retry_sec") or 180)
        self.chat_cfg = {}     # chat_id -> {model/thinking/mode: {current, options}}
        self.chat_usage = {}   # chat_id -> {used, size, cost}
        self.chat_by_sid = {}  # session_id -> chat_id
        state = read_state()
        self.route = state.get("route")
        self.proxy = state.get("proxy")
        self.chat_sessions = {
            str(k): v for k, v in (state.get("chat_sessions") or {}).items()}

    # ---------- ACP ----------

    async def ensure_acp(self):
        if self.acp and self.acp.is_alive():
            return
        if self.acp:
            await self.acp.shutdown()
            self.acp = None
        acp = AcpClient(self.workspace, kimi_bin=self.kimi_bin)
        last = None
        for attempt in range(5):
            try:
                await acp.start(on_stderr=lambda line: log(f"[kimi] {line}"))
                await acp.initialize()
                self.acp = acp
                self.loaded_sessions = set()
                log("kimi acp готов")
                return
            except Exception as e:
                last = e
                log(f"kimi acp: попытка {attempt + 1} не удалась: {e}")
                await acp.shutdown()
                await asyncio.sleep(5)
        self.last_error = str(last)
        raise AcpError(f"kimi acp не стартовал: {last}")

    def _capture_config(self, chat_id, session_res):
        entry = {}
        for opt in session_res.get("configOptions") or []:
            cfg_id = opt.get("id")
            if cfg_id in ("model", "thinking", "mode"):
                entry[cfg_id] = {
                    "current": opt.get("currentValue"),
                    "options": {o["value"]: o["name"] for o in opt.get("options") or []},
                }
        if entry:
            self.chat_cfg[chat_id] = entry

    async def _load_draining(self, session_id):
        """session/load с проглатыванием реплея истории (не показываем его в чат)."""
        fut = self.acp.send_request("session/load", {
            "sessionId": session_id, "cwd": self.workspace, "mcpServers": []})
        ev = asyncio.create_task(self.acp.events.get())
        rq = asyncio.create_task(self.acp.requests.get())
        try:
            while True:
                done, _ = await asyncio.wait(
                    {fut, ev, rq}, return_when=asyncio.FIRST_COMPLETED, timeout=600)
                if fut in done:
                    return fut.result()
                if ev in done:
                    ev = asyncio.create_task(self.acp.events.get())
                if rq in done:
                    msg = rq.result()
                    await self.acp.respond(msg["id"], {"outcome": {"outcome": "cancelled"}})
                    rq = asyncio.create_task(self.acp.requests.get())
        finally:
            for t in (ev, rq):
                t.cancel()

    async def ensure_session(self, chat_id):
        await self.ensure_acp()
        sid = self.chat_sessions.get(str(chat_id))
        if not sid:
            res = await self.acp.new_session()
            sid = res["sessionId"]
            self._capture_config(chat_id, res)
            self.chat_sessions[str(chat_id)] = sid
            self.chat_by_sid[sid] = chat_id
            write_state(chat_sessions=self.chat_sessions)
            log(f"чат {chat_id}: новая сессия {sid}")
            self.loaded_sessions.add(sid)
            return sid
        self.chat_by_sid[sid] = chat_id
        if sid not in self.loaded_sessions:
            res = await self._load_draining(sid)
            self._capture_config(chat_id, res)
            self.loaded_sessions.add(sid)
            log(f"чат {chat_id}: сессия {sid} восстановлена")
            return sid
        try:
            await self.acp.resume_session(sid)
        except AcpError as e:
            log(f"чат {chat_id}: resume {sid} не удался ({e}), пробую load")
            try:
                res = await self._load_draining(sid)
                self._capture_config(chat_id, res)
                self.loaded_sessions.add(sid)
            except AcpError as e2:
                log(f"чат {chat_id}: load тоже не удался ({e2}), завожу новую сессию")
                res = await self.acp.new_session()
                sid = res["sessionId"]
                self._capture_config(chat_id, res)
                self.chat_sessions[str(chat_id)] = sid
                write_state(chat_sessions=self.chat_sessions)
        return sid

    async def close_session(self, chat_id):
        sid = self.chat_sessions.pop(str(chat_id), None)
        write_state(chat_sessions=self.chat_sessions)
        self.chat_cfg.pop(chat_id, None)
        self.chat_usage.pop(chat_id, None)
        if sid:
            self.chat_by_sid.pop(sid, None)
        if sid and self.acp and self.acp.is_alive():
            await self.acp.close_session(sid)
            self.loaded_sessions.discard(sid)
        log(f"чат {chat_id}: сессия {sid} закрыта")

    async def park_sessions(self):
        """Аккуратно закрыть все живые сессии перед выключением."""
        if not self.acp or not self.acp.is_alive():
            return
        for sid in list(self.chat_by_sid):
            try:
                await self.acp.call("session/close", {"sessionId": sid}, timeout=30)
            except Exception:
                pass
        self.loaded_sessions.clear()
        log("сессии припаркованы перед остановкой")

    async def stop_watcher(self):
        """Файл data/stop.flag — мягкая остановка демона."""
        flag = DATA / "stop.flag"
        while True:
            await asyncio.sleep(5)
            if flag.exists():
                try:
                    flag.unlink()
                except Exception:
                    pass
                log("получен запрос остановки — аккуратно завершаюсь")
                self.stopping = True
                if self.dp:
                    await self.dp.stop_polling()
                return

    # ---------- обработка событий ACP ----------

    @staticmethod
    def _tool_update_text(upd):
        texts = []
        for item in upd.get("content") or []:
            if item.get("type") == "content":
                c = item.get("content") or {}
                if c.get("type") == "text" and c.get("text", "").strip():
                    texts.append(c["text"].strip())
            elif item.get("type") == "text" and item.get("text", "").strip():
                texts.append(item["text"].strip())
        return "\n".join(texts)

    async def handle_update(self, sid, upd, progress):
        kind = upd.get("sessionUpdate")
        if kind == "agent_message_chunk":
            text = (upd.get("content") or {}).get("text") or ""
            if text:
                self._turn_text += len(text)
                progress.add_answer(text)
        elif kind == "agent_thought_chunk":
            text = (upd.get("content") or {}).get("text") or ""
            if text.strip():
                progress.set_thought(text)
        elif kind == "tool_call":
            self._turn_tools += 1
            progress.add_tool_line(f"— {upd.get('title') or upd.get('toolCallId')}")
        elif kind == "tool_call_update":
            status = upd.get("status")
            if status == "completed":
                text = self._tool_update_text(upd)
                line = f"— {upd.get('title') or upd.get('toolCallId')} (готово)"
                if text:
                    line += "\n" + "\n".join("  " + t for t in text.splitlines()[:4])
                progress.add_tool_line(line)
            elif status == "failed":
                progress.add_tool_line(
                    f"— {upd.get('title') or upd.get('toolCallId')} (ошибка)")
        elif kind == "usage_update":
            self.last_usage = upd
            chat_id = self.chat_by_sid.get(sid)
            if chat_id:
                self.chat_usage[chat_id] = {
                    "used": upd.get("used"),
                    "size": upd.get("size"),
                    "cost": (upd.get("cost") or {}).get("amount"),
                }
        elif kind == "plan":
            for entry in (upd.get("entries") or [])[:5]:
                progress.add_tool_line(f"• {str(entry.get('content'))[:100]}")
        # user_message_chunk / current_mode_update и прочее — не показываем

    async def ask_permission(self, chat_id, req_msg):
        """Вопрос/запрос kimi к пользователю -> кнопки в чат."""
        rid = req_msg["id"]
        params = req_msg.get("params") or {}
        tc = params.get("toolCall") or {}
        title = tc.get("title") or "(действие)"
        raw = tc.get("rawInput") or {}
        if raw:
            try:
                raw_text = json.dumps(raw, ensure_ascii=False)[:500]
            except Exception:
                raw_text = str(raw)[:500]
        else:
            raw_text = ""
        options = params.get("options") or []
        text = f"Запрос kimi: {title}"
        if raw_text:
            text += f"\n{raw_text}"
        text += "\n\nВыберите вариант:"
        if not options:
            await self.bot.send_message(
                chat_id=chat_id, text=text + "\n(вариантов нет — отклоняю)")
            return {"outcome": "cancelled"}
        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
        buttons = [
            InlineKeyboardButton(
                text=(opt.get("name") or opt.get("optionId"))[:60],
                callback_data=f"perm:{rid}:{opt.get('optionId')}")
            for opt in options
        ]
        rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
        msg = await self.bot.send_message(
            chat_id=chat_id, text=text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        fut = asyncio.get_running_loop().create_future()
        self.perm_futures[rid] = fut
        try:
            option = await asyncio.wait_for(
                fut, timeout=float(self.cfg.get("perm_timeout_sec", 300)))
            if option == "__cancelled__":
                await self.bot.send_message(
                    chat_id=chat_id, text="Запрос отклонён (отмена).")
                return {"outcome": "cancelled"}
            picked = next((o for o in options if o.get("optionId") == option), None)
            label = (picked or {}).get("name") or option
            try:
                await msg.edit_text(f"{text}\n\nВыбрано: {label}")
            except Exception:
                pass
            return {"outcome": "selected", "optionId": option}
        except asyncio.TimeoutError:
            try:
                await msg.edit_text(f"{text}\n\n(ответа не было — отклонено)")
            except Exception:
                pass
            return {"outcome": "cancelled"}
        finally:
            self.perm_futures.pop(rid, None)

    # ---------- ход задачи ----------

    async def run_turn(self, chat_id, blocks):
        self.in_turn = True
        self._turn_text = 0
        self._turn_tools = 0
        progress = ChatProgress(self.bot, chat_id)
        prev_usage = self.chat_usage.get(chat_id)
        try:
            await progress.start()
            sid = await self.ensure_session(chat_id)
            await self.bot.send_chat_action(chat_id=chat_id, action="typing")
            if self.cfg.get("russian_hint", True):
                blocks = [{"type": "text", "text":
                    "[инструкция моста] Отвечай и рассуждай на русском языке."}] + list(blocks)
            fut = self.acp.send_request(
                "session/prompt", {"sessionId": sid, "prompt": blocks})
            ev = asyncio.create_task(self.acp.events.get())
            rq = asyncio.create_task(self.acp.requests.get())
            try:
                while True:
                    done, _ = await asyncio.wait(
                        {fut, ev, rq}, return_when=asyncio.FIRST_COMPLETED, timeout=120)
                    if fut in done:
                        result = fut.result()
                        log(f"чат {chat_id}: turn stop "
                            f"{json.dumps(result, ensure_ascii=False)[:200]}")
                        break
                    if ev in done:
                        notif = ev.result()
                        upd = (notif.get("params") or {}).get("update") or {}
                        await self.handle_update(sid, upd, progress)
                        ev = asyncio.create_task(self.acp.events.get())
                    if rq in done:
                        msg = rq.result()
                        outcome = await self.ask_permission(chat_id, msg)
                        await self.acp.respond(msg["id"], {"outcome": outcome})
                        rq = asyncio.create_task(self.acp.requests.get())
                    await progress.tick()
            finally:
                for t in (ev, rq):
                    t.cancel()
            if (result.get("stopReason") == "end_turn"
                    and self._turn_text == 0 and self._turn_tools == 0):
                log(f"чат {chat_id}: пустой ответ — сессия {sid} повреждена, пересоздаю")
                try:
                    await self.close_session(chat_id)
                except Exception:
                    pass
                progress.answer = (
                    "kimi молча завершил задачу — сессия была повреждена "
                    "принудительной остановкой демона. Я пересоздал сессию: "
                    "пожалуйста, повторите сообщение.")
            usage = self.chat_usage.get(chat_id)
            if usage and usage is not prev_usage:
                used = usage.get("used")
                size = usage.get("size")
                if used is not None and size:
                    progress.answer += f"\n\n(контекст: {used}/{size} токенов)"
            await progress.tick(force=True)
            await progress.finish()
        except Exception as e:
            self.last_error = f"{type(e).__name__}: {e}"
            log(f"чат {chat_id}: ошибка хода:\n{traceback.format_exc(limit=3)}")
            try:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=f"Ошибка моста: {type(e).__name__}: {e}\n"
                    "Подробности — `/status` и лог.")
            except Exception:
                pass
        finally:
            self.in_turn = False
            self.last_turn_end = time.time()

    async def _teardown_acp(self):
        """Закрыть kimi acp (после простоя или при остановке демона)."""
        if not self.acp:
            return
        try:
            await self.acp.shutdown()
        except Exception as e:
            log(f"acp shutdown: {e}")
        self.acp = None
        self.loaded_sessions.clear()
        log("kimi acp закрыт (idle) — следующее сообщение поднимет сессию с диска")

    async def acp_idle_watcher(self):
        """On-demand: если после ответа диалог не продолжен idle-секунд —
        закрываем kimi; новое сообщение поднимет сессию с того же места."""
        idle_sec = float(self.cfg.get("acp_idle_sec", 3600))
        while True:
            await asyncio.sleep(60)
            if not self.acp or self.in_turn:
                continue
            idle = time.time() - self.last_turn_end
            if idle >= idle_sec:
                log(f"kimi acp простой {idle / 60:.0f} мин >= {idle_sec / 60:.0f} мин — закрываю")
                await self._teardown_acp()

    async def worker(self):
        while True:
            chat_id, blocks = await self.queue.get()
            async with self.turn_lock:
                await self.run_turn(chat_id, blocks)

    async def perm_watchdog(self):
        """Запрос kimi вне активного хода — «cancelled», чтобы не завис."""
        while True:
            await asyncio.sleep(2)
            if self.in_turn or not self.acp:
                continue
            try:
                msg = self.acp.requests.get_nowait()
            except Exception:
                continue
            await self.acp.respond(msg["id"], {"outcome": {"outcome": "cancelled"}})
            log(f"осиротевший запрос kimi id={msg.get('id')} — cancelled")

    async def net_watchdog(self):
        """Периодическая проверка связи: прямой канал -> фолбэк на прокси и обратно.

        Смену маршрута нельзя делать через self.bot.session.proxy: в aiogram 3.31
        сеттер падает на None (TypeError) и убивает watchdog — демон молчит дальше.
        Вместо этого собираем нового бота под маршрут и перезапускаем polling.
        """
        proxy_url = self.cfg.get("proxy") or proxy_from_env()
        while True:
            await asyncio.sleep(60)
            if self.in_turn:
                continue  # не выбиваем сессию бота из-под активного хода
            try:
                direct = await check_direct()
            except Exception:
                direct = False
            new_route, new_proxy = None, None
            if direct:
                new_route, new_proxy = "direct", None
            else:
                ok_proxy = False
                try:
                    ok_proxy = await check_proxy(proxy_url)
                except Exception:
                    ok_proxy = False
                if ok_proxy:
                    new_route, new_proxy = "proxy", proxy_url
            if new_route is None:
                if self.route != "off":
                    log("сеть: Telegram недоступен ни напрямую, ни через прокси")
                    self.route = "off"
                    write_state(route="off")
                continue
            if new_route != self.route:
                log(f"сеть: переключаюсь на "
                    f"{'прямое соединение' if new_route == 'direct' else f'прокси {new_proxy}'}")
                self.route = new_route
                self.proxy = new_proxy
                write_state(route=new_route, proxy=new_proxy)
                await self._bounce_polling(new_proxy)

    async def _bounce_polling(self, proxy):
        """Пересоздать бота с сессией под новый маршрут и перезапустить polling.

        Останавливаем текущий polling; supervisor в run() стартует заново с
        новым self.bot. Сообщения, накопившиеся за паузу, не теряются.
        """
        from aiogram import Bot
        from aiogram.client.session.aiohttp import AiohttpSession
        try:
            new_bot = Bot(self.cfg["bot_token"], session=AiohttpSession(proxy=proxy))
        except Exception as e:
            log(f"сеть: не удалось собрать бота под новый маршрут: {e}")
            return
        self.bot = new_bot
        try:
            await self.dp.stop_polling()
        except Exception as e:
            log(f"сеть: stop_polling при смене маршрута: {e}")

    # ---------- обработчики aiogram ----------

    async def guard(self, message):
        status = self._owner_status(message.from_user.id)
        if status is True:
            return False
        if status is None:
            self.cfg["allowed_user_ids"] = [message.from_user.id]
            save_config(self.cfg)
            await message.answer(
                "Вы назначены владельцем моста, доступ получите только вы.\n"
                "Теперь можно ставить задачи. Команды: /help")
            log(f"владелец назначен: user_id={message.from_user.id}")
            return True
        await message.answer(
            f"Нет доступа. Ваш id: {message.from_user.id} — попросите владельца "
            "добавить его в allowed_user_ids (файл tg.local.json).")
        return True

    def _owner_status(self, user_id):
        allowed = self.cfg.get("allowed_user_ids") or []
        if not allowed:
            return None  # владельца ещё нет — первый обратившийся станет им
        return user_id in allowed

    async def _config_command(self, message, cfg_id, label, descriptions=None):
        parts = (message.text or "").split(maxsplit=1)
        arg = parts[1].strip() if len(parts) > 1 else ""
        entry = self.chat_cfg.get(message.chat.id) or {}
        info = entry.get(cfg_id) or {}
        options = info.get("options") or {}
        if not arg:
            if not options:
                await message.answer(
                    f"{label} этого чата ещё не известна. Напишите задачу, "
                    f"затем снова команда.")
                return
            names = "\n".join(
                f"  {v} — {n}" + (f": {descriptions[v]}" if descriptions and v in descriptions else "")
                for v, n in sorted(options.items()))
            await message.answer(
                f"Текущее значение: {info.get('current') or '—'}\n"
                f"{label}:\n{names}")
            return
        if options and arg not in options:
            await message.answer(
                f"Нет такого значения: {arg}. Доступно: {', '.join(sorted(options))}")
            return
        if self.in_turn:
            await message.answer("Сейчас выполняется задача — подождите завершения.")
            return
        sid = await self.ensure_session(message.chat.id)
        try:
            await self.acp.set_config_option(sid, cfg_id, arg)
        except AcpError as e:
            await message.answer(f"Не удалось установить {label}: {e}")
            return
        self.chat_cfg.setdefault(message.chat.id, {})[cfg_id] = {
            "current": arg, "options": options}
        await message.answer(f"{label} выставлена: {arg}")

    def register_handlers(self, dp):
        from aiogram import F
        from aiogram.filters import Command, CommandStart
        from aiogram.types import CallbackQuery, Message

        @dp.message(CommandStart())
        async def h_start(message: Message):
            if await self.guard(message):
                return
            await message.answer(
                "Привет! Я мост к kimi на твоём компьютере. Просто напиши задачу — "
                "передам её kimi и покажу ход работы.\n"
                "/new — новая сессия (забыть контекст)\n"
                "/cancel — отменить текущую задачу\n"
                "/status — состояние моста, конфигурация, контекст\n"
                "/model — модель этого чата\n"
                "/mode — режим (plan/auto/yolo)\n"
                "/thinking — уровень размышлений\n"
                "/help — все команды")

        @dp.message(Command("help"))
        async def h_help(message: Message):
            if await self.guard(message):
                return
            await message.answer(
                "/new — новая сессия\n"
                "/cancel — отменить задачу\n"
                "/status — состояние моста и kimi\n"
                "/model — список моделей\n"
                "/model gw-qwen35b — сменить модель\n"
                "/mode — список режимов\n"
                "/mode plan — включить режим (default/plan/auto/yolo)\n"
                "/thinking — уровень размышлений (off/low/high/max)\n"
                "/thinking low — установить\n"
                "Фото — можно прислать (попадёт в задачу)")

        @dp.message(Command("status"))
        async def h_status(message: Message):
            if await self.guard(message):
                return
            st = read_state()
            lines = []
            try:
                me = await self.bot.get_me()
            except Exception:
                me = None
            if me:
                lines.append(f"Бот: @{me.username} (id {me.id})")
            lines.append(f"kimi acp: {'жив' if self.acp and self.acp.is_alive() else 'НЕ запущен'}")
            if self.acp and self.acp.is_alive() and not self.in_turn:
                idle_min = (time.time() - self.last_turn_end) / 60
                idle_total = float(self.cfg.get("acp_idle_sec", 3600)) / 60
                lines.append(f"Простой: {idle_min:.0f} мин (закрою после {idle_total:.0f} мин без сообщений)")
            lines.append(f"Воркспейс: {self.workspace}")
            route = st.get("route")
            if route:
                extra = f" (прокси {st.get('proxy')})" if route == "proxy" else ""
                lines.append(f"Сеть до Telegram: {route}{extra}")
            lines.append(f"Демон с: {st.get('started_at') or '—'}")
            lines.append(f"Чатов с сессиями: {len(st.get('chat_sessions') or {})}")
            lines.append(f"Задач в очереди: {self.queue.qsize()}")
            if self.in_turn:
                lines.append("Сейчас: выполняется задача")
            usage = self.chat_usage.get(message.chat.id)
            if usage and usage.get("used") is not None:
                ctx = f"Контекст сессии: {usage['used']}/{usage.get('size', '?')} токенов"
                if usage.get("cost") is not None:
                    ctx += f" (стоимость {usage['cost']:.4f}$)"
                lines.append(ctx)
            cfg = self.chat_cfg.get(message.chat.id) or {}
            if cfg.get("model"):
                lines.append(f"Модель: {cfg['model'].get('current')}")
            if cfg.get("thinking"):
                lines.append(f"Размышления: {cfg['thinking'].get('current')}")
            if cfg.get("mode"):
                lines.append(f"Режим: {cfg['mode'].get('current')}")
            if self.last_error:
                lines.append(f"Последняя ошибка: {self.last_error}")
            await message.answer("\n".join(lines))

        @dp.message(Command("new"))
        async def h_new(message: Message):
            if await self.guard(message):
                return
            if self.in_turn:
                await message.answer("Сейчас выполняется задача — сначала /cancel.")
                return
            await self.close_session(message.chat.id)
            await message.answer("Сессия сброшена. Следующее сообщение начнёт новую.")

        @dp.message(Command("cancel"))
        async def h_cancel(message: Message):
            if await self.guard(message):
                return
            if not self.in_turn:
                await message.answer("Нет активной задачи.")
                return
            sid = self.chat_sessions.get(str(message.chat.id))
            if sid and self.acp and self.acp.is_alive():
                self.acp.cancel_session(sid)
            for rid, fut in list(self.perm_futures.items()):
                if not fut.done():
                    fut.set_result("__cancelled__")
            await message.answer("Отменяю… (kimi получит сигнал отмены)")

        @dp.message(Command("model"))
        async def h_model(message: Message):
            if await self.guard(message):
                return
            await self._config_command(message, "model", "Модель")

        @dp.message(Command("mode"))
        async def h_mode(message: Message):
            if await self.guard(message):
                return
            await self._config_command(message, "mode", "Режим", MODE_DESCRIPTIONS)

        @dp.message(Command("thinking"))
        async def h_thinking(message: Message):
            if await self.guard(message):
                return
            await self._config_command(message, "thinking", "Размышления")

        @dp.message(F.text)
        async def h_text(message: Message):
            if await self.guard(message):
                return
            await self.queue.put(
                (message.chat.id, [{"type": "text", "text": message.text}]))
            if self.in_turn:
                await message.answer("Принято. Задача в очереди.")

        @dp.message(F.photo)
        async def h_photo(message: Message):
            if await self.guard(message):
                return
            try:
                photo = message.photo[-1]
                file = await self.bot.get_file(photo.file_id)
                buf = io.BytesIO()
                await self.bot.download_file(file.file_path, buf)
                blocks = [
                    {"type": "image", "mimeType": "image/jpeg",
                     "data": base64.b64encode(buf.getvalue()).decode()},
                    {"type": "text", "text": message.caption or
                        "Рассмотри картинку и ответь, что на ней, исправь или опиши."},
                ]
                await self.queue.put((message.chat.id, blocks))
                if self.in_turn:
                    await message.answer("Принято. Задача в очереди.")
            except Exception as e:
                await message.answer(f"Не удалось загрузить фото: {e}")

        @dp.message(F.voice)
        async def h_voice(message: Message):
            if await self.guard(message):
                return
            await message.answer("Голосовые пока не поддерживаю. Отправьте текст или фото.")

        @dp.message(F.document)
        async def h_document(message: Message):
            if await self.guard(message):
                return
            await message.answer("Документы пока не принимаю. Отправьте текст или фото.")

        @dp.callback_query()
        async def h_callback(query: CallbackQuery):
            data = query.data or ""
            if not data.startswith("perm:"):
                await query.answer()
                return
            try:
                _, rid_s, option = data.split(":", 2)
                fut = self.perm_futures.get(int(rid_s))
            except Exception:
                fut = None
            if fut and not fut.done():
                fut.set_result(option)
            await query.answer()

    # ---------- жизненный цикл ----------

    async def run(self):
        from aiogram import Bot, Dispatcher
        from aiogram.client.session.aiohttp import AiohttpSession

        # сеть может отсутствовать на старте (VPN выключен, прокси мёртв):
        # не завершаемся, а пробуем снова с интервалом net_retry_sec —
        # иначе задача Планировщика «при входе» больше не поднимет мост
        while True:
            try:
                route, proxy, _ = await ensure_network(self.cfg)
                break
            except SystemExit as e:
                log(f"{e}; повтор через {self.net_retry_sec:.0f} с")
                # спим кусками по 5 с, чтобы мягкий stop сработал и в паузе
                flag = DATA / "stop.flag"
                for _ in range(max(1, int(self.net_retry_sec // 5))):
                    await asyncio.sleep(5)
                    if flag.exists():
                        try:
                            flag.unlink()
                        except Exception:
                            pass
                        raise SystemExit("остановлен командой stop")
        log(f"сеть до Telegram: {route}" + (f" (прокси {proxy})" if proxy else ""))
        write_state(route=route, proxy=proxy, started_at=_now())
        self.route, self.proxy = route, proxy
        if not self.cfg.get("bot_token"):
            raise SystemExit(
                f"В {CONFIG_PATH} нет bot_token. Создайте бота у @BotFather "
                "и впишите токен в поле bot_token.")
        self.bot = Bot(
            self.cfg["bot_token"],
            session=AiohttpSession(proxy=proxy if route == "proxy" else None))
        self.dp = Dispatcher()
        self.register_handlers(self.dp)
        # on-demand: в простое kimi держим по выходу из idle; acp поднимется к первому сообщению
        self.last_turn_end = time.time()
        if self.acp and self.acp.is_alive():
            await self.acp.shutdown()
            self.acp = None
        self.loaded_sessions.clear()
        log("kimi acp: поднимется к первому сообщению; после ответа живёт ещё час (idle)")
        asyncio.create_task(self.worker())
        asyncio.create_task(self.perm_watchdog())
        asyncio.create_task(self.net_watchdog())
        asyncio.create_task(self.stop_watcher())
        asyncio.create_task(self.acp_idle_watcher())
        first_poll = True
        while not self.stopping:
            try:
                await self.dp.start_polling(self.bot, drop_pending_updates=first_poll)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception:
                log(f"polling упал: {traceback.format_exc(limit=3)} — "
                    f"рестарт через {POLL_RESTART_SEC} с")
                await asyncio.sleep(POLL_RESTART_SEC)
                continue
            finally:
                first_poll = False
            log("polling остановлен")
        try:
            await self.park_sessions()
        except Exception:
            pass
        if self.acp:
            await self.acp.shutdown()


def cmd_start():
    alive = _pid_alive(read_state().get("pid"))
    if alive:
        raise SystemExit(
            f"Демон уже работает (pid {read_state().get('pid')}). "
            "Остановить: python tg_bridge.py stop")
    DATA.mkdir(parents=True, exist_ok=True)
    try:
        (DATA / "stop.flag").unlink()
    except FileNotFoundError:
        pass
    PID_PATH.write_text(str(os.getpid()), encoding="utf-8")
    write_state(pid=os.getpid(), started_at=_now(), last_error=None)
    log("мост tg запускается")
    _setup_aiogram_logging()
    try:
        asyncio.run(TgBridge(load_config()).run())
    except SystemExit as e:
        log(f"останов: {e}")
        raise
    except KeyboardInterrupt:
        log("остановлен (Ctrl+C)")
    finally:
        try:
            PID_PATH.unlink()
        except FileNotFoundError:
            pass


def cmd_stop():
    pid = read_state().get("pid")
    if not pid or not _pid_alive(pid):
        print("Демон не запущен.")
        return
    flag = DATA / "stop.flag"
    flag.write_text("", encoding="utf-8")
    print("Запрос остановки отправлен (демон завершится в пределах 10 секунд)…")
    for _ in range(12):  # до ~30 c
        time.sleep(2.5)
        if not _pid_alive(pid):
            break
    if _pid_alive(pid):
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                           capture_output=True, creationflags=NO_WINDOW)
        else:
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        print(f"Демон не ответил — принудительно остановлен (pid {pid}).")
    else:
        print("Демон остановлен аккуратно.")
    log("остановлен командой stop")


def cmd_install():
    if os.name != "nt":
        raise SystemExit(
            "install — только Windows (Task Scheduler); на других системах "
            "запускайте вручную: python tg_bridge.py start")
    pythonw = Path(sys.executable).with_name("pythonw.exe")
    if not pythonw.is_file():
        pythonw = Path(sys.executable)
    script = Path(__file__).resolve()
    ps = f"""
$action = New-ScheduledTaskAction -Execute '{pythonw}' -Argument '\"{script}\" start' -WorkingDirectory '{script.parent}'
$trigger = New-ScheduledTaskTrigger -AtLogOn -User \"$env:USERNAME\"
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan)
Register-ScheduledTask -TaskName '{TASK_NAME}' -Action $action -Trigger $trigger -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName '{TASK_NAME}'
"""
    proc = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        creationflags=NO_WINDOW)
    if proc.returncode != 0:
        raise SystemExit(f"Не удалось зарегистрировать задачу: {proc.stderr.strip()}")
    print(f"Задача {TASK_NAME} зарегистрирована (запуск при входе) и запущена.")
    log("задача планировщика установлена и запущена")


def cmd_uninstall():
    if os.name == "nt":
        for args in (["schtasks", "/End", "/TN", TASK_NAME],
                     ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"]):
            subprocess.run(args, capture_output=True, creationflags=NO_WINDOW)
    else:
        print("Задача планировщика: нет (install доступен только на Windows).")
    pid = read_state().get("pid")
    if pid and _pid_alive(pid):
        subprocess.run(["taskkill", "/F", "/PID", str(pid)],
                       capture_output=True, creationflags=NO_WINDOW)
    print(f"Задача {TASK_NAME} удалена, демон остановлен (лог: {LOG_PATH.name}).")


def cmd_status():
    st = read_state()
    pid = st.get("pid")
    alive = _pid_alive(pid)
    print(f"Мост tg: {'демон работает' if alive else 'демон не запущен'}"
          + (f" (pid {pid})" if alive else ""))
    if st.get("started_at"):
        print(f"Запущен: {st['started_at']}")
    if st.get("route"):
        extra = f" через прокси {st['proxy']}" if st["route"] == "proxy" else ""
        print(f"Сеть до Telegram (последняя проверка): {st['route']}{extra}")
    print(f"Чатов с сессиями: {len(st.get('chat_sessions') or {})}")
    if not CONFIG_PATH.exists():
        print(f"ВНИМАНИЕ: нет конфигурации {CONFIG_PATH} — "
              "создайте её (bot_token от @BotFather, allowed_user_ids).")
    elif not load_config(required=False).get("bot_token"):
        print("ВНИМАНИЕ: в конфигурации нет bot_token — мост не стартует.")
    if LOG_PATH.exists():
        lines = LOG_PATH.read_text(encoding="utf-8").strip().splitlines()
        print("\nПоследние записи лога:")
        for line in lines[-12:]:
            print("  " + line)


def cmd_log():
    if not LOG_PATH.exists():
        print("Лог пуст.")
        return
    for line in LOG_PATH.read_text(encoding="utf-8").strip().splitlines()[-40:]:
        print(line)


def main():
    _fix_console()
    ap = argparse.ArgumentParser(description="Мост Telegram <-> kimi (ACP)")
    ap.add_argument("cmd", choices=["start", "stop", "status", "log", "install", "uninstall"])
    args = ap.parse_args()
    if args.cmd == "install":
        cmd_install()
    elif args.cmd == "uninstall":
        cmd_uninstall()
    elif args.cmd == "start":
        cmd_start()
    elif args.cmd == "stop":
        cmd_stop()
    elif args.cmd == "status":
        cmd_status()
    elif args.cmd == "log":
        cmd_log()


if __name__ == "__main__":
    main()
