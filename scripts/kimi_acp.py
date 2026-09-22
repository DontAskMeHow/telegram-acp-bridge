"""Клиент ACP (Agent Client Protocol) для kimi acp — JSON-RPC 2.0 поверх stdio.

Один процесс `kimi acp` обслуживает несколько сессий; мост tg заводит на
каждый чат свою сессию. Поток-читатель разбирает stdout построчно (спека ACP:
строка JSON на строку) и раскладывает сообщения в asyncio-очереди: ответы
(futures), события session/update (self.events) и запросы сервера к клиенту,
в нашем случае — session/request_permission (self.requests).
"""

import asyncio
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

# Путь к бинарнику агента: окружение KIMI_BIN, поле kimi_bin в конфиге
# (tg.local.json), иначе `kimi` в PATH.
KIMI_BIN = os.environ.get("KIMI_BIN", "kimi")
NO_WINDOW = 0x08000000 if os.name == "nt" else 0

PROTOCOL_VERSION = 1
INIT_TIMEOUT = 300   # первый запуск kimi грузит конфигурацию и MCP
CALL_TIMEOUT = 180


class AcpError(Exception):
    def __init__(self, message, code=None, data=None):
        super().__init__(message)
        self.code = code
        self.data = data


class AcpClient:
    def __init__(self, cwd, kimi_bin=None, stderr_ring=200):
        self.cwd = str(Path(cwd).resolve())
        self.kimi_bin = kimi_bin or KIMI_BIN
        self.stderr_ring_size = stderr_ring
        self.proc = None
        self.capabilities = {}
        self.agent_info = {}
        self.stderr_tail = []
        self._loop = None
        self._wlock = threading.Lock()
        self._next_id = 1
        self._pending = {}
        self._on_stderr = None
        self._rt = None
        self._et = None

    async def start(self, on_stderr=None):
        self._loop = asyncio.get_running_loop()
        self.events = asyncio.Queue()    # уведомления (session/update)
        self.requests = asyncio.Queue()  # сервер -> клиент (request_permission)
        self._on_stderr = on_stderr
        try:
            self.proc = subprocess.Popen(
                [self.kimi_bin, "acp"], cwd=self.cwd,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, creationflags=NO_WINDOW)
        except Exception as e:
            raise AcpError(f"не удалось запустить kimi acp: {e}")
        self._rt = threading.Thread(target=self._read_stdout, name="acp-out", daemon=True)
        self._et = threading.Thread(target=self._read_stderr, name="acp-err", daemon=True)
        self._rt.start()
        self._et.start()

    async def initialize(self):
        res = await self.call("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "clientCapabilities": {},
            "clientInfo": {"name": "kimi-tg-bridge", "title": "KimI Telegram Bridge", "version": "1.0"},
        }, timeout=INIT_TIMEOUT)
        # Клиент может ответить своей версией — принимаем любую >=1.
        self.capabilities = res.get("agentCapabilities") or {}
        self.agent_info = res.get("agentInfo") or {}
        return res

    # ---------- ввод/вывод ----------

    def _write(self, obj):
        data = (json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        with self._wlock:
            self.proc.stdin.write(data)
            self.proc.stdin.flush()

    def _read_stdout(self):
        try:
            for raw in self.proc.stdout:
                line = raw.decode("utf-8", "replace").strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except Exception:
                    continue
                self._dispatch(msg)
        except Exception:
            pass

    def _read_stderr(self):
        try:
            for raw in self.proc.stderr:
                line = raw.decode("utf-8", "replace").rstrip()
                if not line:
                    continue
                self.stderr_tail.append(line)
                if len(self.stderr_tail) > self.stderr_ring_size:
                    self.stderr_tail.pop(0)
                cb = self._on_stderr
                if cb:
                    self._loop.call_soon_threadsafe(cb, line)
        except Exception:
            pass

    def _dispatch(self, msg):
        rid = msg.get("id")
        if rid is not None and "method" in msg:
            self._loop.call_soon_threadsafe(self.requests.put_nowait, msg)
        elif rid is not None:
            fut = self._pending.pop(rid, None)
            if fut is None or fut.done():
                return
            if "error" in msg:
                err = msg["error"]
                exc = AcpError(err.get("message", "ошибка ACP"),
                               code=err.get("code"), data=err.get("data"))
                self._loop.call_soon_threadsafe(fut.set_exception, exc)
            else:
                self._loop.call_soon_threadsafe(fut.set_result, msg.get("result"))
        else:
            self._loop.call_soon_threadsafe(self.events.put_nowait, msg)

    # ---------- API для вызывающего (вызовы только из event loop) ----------

    def is_alive(self):
        return self.proc is not None and self.proc.poll() is None

    def send_request(self, method, params):
        """Отправить запрос и вернуть Future результата (без ожидания)."""
        rid = self._next_id
        self._next_id += 1
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        try:
            self._write({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        except Exception as e:
            self._pending.pop(rid, None)
            fut.set_exception(AcpError(f"запись в kimi acp не удалась: {e}"))
        return fut

    async def call(self, method, params, timeout=CALL_TIMEOUT):
        fut = self.send_request(method, params)
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            for rid, f in list(self._pending.items()):
                if f is fut:
                    self._pending.pop(rid, None)

    def notify(self, method, params):
        try:
            self._write({"jsonrpc": "2.0", "method": method, "params": params})
        except Exception:
            pass

    async def respond(self, request_id, result):
        try:
            self._write({"jsonrpc": "2.0", "id": request_id, "result": result})
        except Exception:
            pass

    # ---------- сессии ----------

    async def new_session(self):
        return await self.call(
            "session/new", {"cwd": self.cwd, "mcpServers": []})

    async def load_session(self, session_id):
        return await self.call("session/load", {
            "sessionId": session_id, "cwd": self.cwd, "mcpServers": []})

    async def resume_session(self, session_id):
        return await self.call("session/resume", {
            "sessionId": session_id, "cwd": self.cwd, "mcpServers": []})

    async def close_session(self, session_id):
        try:
            return await self.call("session/close", {"sessionId": session_id}, timeout=60)
        except AcpError:
            return None

    def cancel_session(self, session_id):
        self.notify("session/cancel", {"sessionId": session_id})

    async def set_config_option(self, session_id, config_id, value):
        return await self.call("session/set_config_option", {
            "sessionId": session_id, "configId": config_id, "value": value})

    async def shutdown(self):
        if self.proc is None:
            return
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        for _ in range(30):  # даём до 3 с на аккуратный выход по EOF stdin
            if self.proc.poll() is not None:
                break
            await asyncio.sleep(0.1)
        if self.proc.poll() is None:
            try:
                self.proc.kill()
            except Exception:
                pass
        try:
            self.proc.wait(timeout=5)
        except Exception:
            pass
        self.proc = None
