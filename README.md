# telegram-acp-bridge

Control an AI coding agent (Kimi, `kimi acp`) from your phone via Telegram.
One Telegram chat = one agent session: the agent process is started lazily on
the first message, kept warm for an hour of idle, then shut down; the context
survives on disk and is reloaded for the next message. Live progress
(thoughts, tool calls, answer) is rendered into an editable chat message,
permission requests arrive as inline buttons, and photos can be attached to
a task.

## Architecture

- **aiogram 3 long-polling bot** — the daemon; no public IP or webhook needed.
- **Own ACP client** (`kimi_acp.py`) — Agent Client Protocol, JSON-RPC 2.0
  over stdio. A single `kimi acp` process serves several sessions, one per
  chat; a reader thread splits stdout into response futures, `session/update`
  events, and server-to-client `session/request_permission` requests.
- **Lazy agent lifecycle** — the process is spawned on demand, parked in
  idle after `acp_idle_sec` (default 3600 s), and sessions are restored with
  `session/load` (history replay is drained, not shown in the chat).
- **Net watchdog** — periodic reachability probe of `api.telegram.org`; on
  failure the bridge falls back to a proxy (config `proxy` or `PROXY` /
  `HTTPS_PROXY`) and back, rebinding the bot session without losing queued
  messages. At startup with no network it retries every `net_retry_sec`.

## Layout

```
scripts/tg_bridge.py   daemon + CLI (start/stop/status/log/install/uninstall)
scripts/kimi_acp.py    ACP client (JSON-RPC 2.0 over stdio)
scripts/tg_net.py      Telegram reachability check, proxy fallback, config
scripts/acp_probe.py   self-test: real handshake, prompt, permission flow
tg.local.example.json  config template (copy to tg.local.json)
```

## Quickstart

1. `pip install -r requirements.txt` (tested with Python 3.12+, aiogram 3.31).
2. Create the config: `cp tg.local.example.json tg.local.json` and fill it in:
   - `bot_token` — from @BotFather (required, the bridge exits with a clear
     error otherwise),
   - `allowed_user_ids` — your Telegram user id(s); **empty list = the first
     person who writes to the bot becomes the owner**,
   - `workspace` — directory the agent works in (default: repo checkout),
   - `kimi_bin` — path to the agent binary (default: `kimi` in PATH or
     `KIMI_BIN` env var),
   - `proxy` — SOCKS/HTTP proxy URL, optional (env `PROXY`/`HTTPS_PROXY`
     also works).
3. Run the self-test (no Telegram involved): `python scripts/acp_probe.py`.
4. Start the bridge: `python scripts/tg_bridge.py start`
   (on Windows `python scripts/tg_bridge.py install` also registers a
   Task Scheduler entry at logon; `stop` / `status` / `log` manage it).
5. Open the bot in Telegram and send `/start`.

State, logs and the pid file live in `data/` (gitignored) next to the
config. The config file contains a token — keep it out of version control.

## License

MIT — see [LICENSE](LICENSE).
