"""Самопроверка ACP-клиента против реального kimi acp.

  python acp_probe.py           # текст + стрим + close
  python acp_probe.py --edit    # + правка файла с автоодобрением разрешений
  python acp_probe.py --no-tools  # без инструментов (быстро)
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kimi_acp import AcpClient, AcpError  # noqa: E402
from tg_net import load_config  # noqa: E402

# Воркспейс и бинарник — из конфига tg.local.json (workspace, kimi_bin),
# по умолчанию — корень репозитория и `kimi` из PATH/окружения.
_CFG = load_config(required=False)
WORKSPACE = str(Path(_CFG.get("workspace") or Path(__file__).resolve().parents[1]))
KIMI_BIN = _CFG.get("kimi_bin")


def show(label, obj, limit=1600):
    text = json.dumps(obj, ensure_ascii=False, indent=1)
    print(f"=== {label} ===\n{text[:limit]}", flush=True)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--edit", action="store_true")
    ap.add_argument("--no-tools", action="store_true")
    args = ap.parse_args()

    client = AcpClient(WORKSPACE, kimi_bin=KIMI_BIN)
    await client.start(on_stderr=lambda line: print(f"[stderr] {line}", flush=True))

    res = await client.initialize()
    print(f"capabilities: {json.dumps(client.capabilities, ensure_ascii=False)}", flush=True)

    created = await client.new_session()
    show("session/new", created)
    sid = created["sessionId"]

    prompt = "Создай в корне воркспейса файл tg_probe_tmp.txt с текстом ПРИВЕТ. Ответь ровно: EDIT_OK" \
        if args.edit and not args.no_tools else "Ответь ровно: ACP_PROBE_OK"

    fut = client.send_request("session/prompt", {
        "sessionId": sid, "prompt": [{"type": "text", "text": prompt}]})

    perm_count = 0
    try:
        while True:
            ev_task = asyncio.create_task(client.events.get())
            rq_task = asyncio.create_task(client.requests.get())
            done, _ = await asyncio.wait({fut, ev_task, rq_task},
                                         return_when=asyncio.FIRST_COMPLETED)
            if fut in done:
                print(f"stop: {json.dumps(fut.result(), ensure_ascii=False)}", flush=True)
                break
            if ev_task in done:
                msg = ev_task.result()
                upd = msg.get("params", {}).get("update", {})
                kind = upd.get("sessionUpdate")
                if kind == "agent_message_chunk":
                    print(f"[text] {upd.get('content', {}).get('text', '')[:160]}", flush=True)
                elif kind == "tool_call":
                    show("tool_call", upd, 400)
                elif kind == "tool_call_update":
                    show("tool_call_update", upd, 400)
                elif kind not in ("user_message_chunk",):
                    show(f"update:{kind}", upd, 300)
                continue
            if rq_task in done:
                msg = rq_task.result()
                perm_count += 1
                show("request_permission", msg.get("params"), 800)
                options = msg["params"].get("options") or []
                pick = next((o["optionId"] for o in options
                             if o.get("kind") == "allow_once"), None)
                await client.respond(msg["id"], {
                    "outcome": {"outcome": "selected", "optionId": pick} if pick
                    else {"outcome": "cancelled"}})
                continue
            raise AcpError("ни одно событие не готово (не должно случиться)")
    finally:
        pass
    print(f"разрешений запрошено: {perm_count}", flush=True)
    await client.close_session(sid)
    print("session/close: OK", flush=True)
    await client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
