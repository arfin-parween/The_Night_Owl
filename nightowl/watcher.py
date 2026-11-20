"""Detection loop: watches OpenSearch for new FAILED runs and wakes the OpenClaw agent
through its Gateway webhook (POST /hooks/agent). Optionally pings Telegram directly
so the phone buzzes instantly while the agent investigates."""
from __future__ import annotations

import time

import requests

from . import os_client, settings, tools

ALERT_TEMPLATE = (
    "🚨 ALERT from OpenSearch: pipeline {pipeline} run {run_id} FAILED at stage '{stage}' "
    "with {error_class} at {when}.\n"
    "You are NightOwl, the on-call AI engineer. Use the nightowl skill to investigate end to end "
    "(timeline, schema diff, what changed, correlated upstream logs, similar past incidents, blast radius), "
    "open an incident with your proposed fix, and send the on-call human a concise RCA with evidence. "
    "Do NOT apply any fix until a human explicitly approves it."
)


def telegram_ping(text: str) -> None:
    if not (settings.TELEGRAM_BOT_TOKEN and settings.TELEGRAM_CHAT_ID):
        return
    try:
        requests.post(f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": settings.TELEGRAM_CHAT_ID, "text": text}, timeout=10)
    except Exception as exc:
        print(f"  telegram ping failed: {exc}")


def wake_agent(run: dict) -> None:
    message = ALERT_TEMPLATE.format(pipeline=run["pipeline"], run_id=run["run_id"],
                                    stage=run.get("failed_stage"), error_class=run.get("error_class"),
                                    when=tools.ist(run.get("ended_at")))
    payload = {"message": message, "name": "nightowl-alert", "agentId": settings.OPENCLAW_AGENT_ID,
               "timeoutSeconds": 600}
    if settings.TELEGRAM_CHAT_ID:
        payload.update({"deliver": True, "channel": "telegram", "to": settings.TELEGRAM_CHAT_ID})
    if not settings.OPENCLAW_HOOK_TOKEN:
        print("  OPENCLAW_HOOK_TOKEN not set; would have sent:\n" + message)
        return
    # A loaded laptop can stall the gateway for 10s+ (seen while Spark runs), so wait longer and retry.
    # The Idempotency-Key makes a retry safe: the gateway won't start a second investigation.
    for attempt in (1, 2, 3):
        try:
            r = requests.post(settings.OPENCLAW_HOOK_URL, json=payload, timeout=60,
                              headers={"Authorization": f"Bearer {settings.OPENCLAW_HOOK_TOKEN}",
                                       "Idempotency-Key": f"nightowl-{run['run_id']}"})
            print(f"  woke OpenClaw agent: HTTP {r.status_code} {r.text[:200]}")
            return
        except Exception as exc:
            print(f"  OpenClaw hook attempt {attempt}/3 failed: {str(exc)[:160]}")
            time.sleep(5)
    print(f"  could not reach OpenClaw hook at {settings.OPENCLAW_HOOK_URL}. "
          "Fallback: tell the bot in Telegram \"daily_city_sales just failed, investigate\".")


def watch(interval: int = 5) -> None:
    print(f"NightOwl watcher: polling {settings.IDX_RUNS} every {interval}s for failed runs (Ctrl+C to stop)")
    if not os_client.wait_until_up(60):
        raise SystemExit("OpenSearch not reachable")
    since = settings.iso()
    seen: set[str] = set()
    while True:
        try:
            resp = os_client.search(settings.IDX_RUNS, {
                "size": 5, "sort": [{"started_at": "asc"}],
                "query": {"bool": {"filter": [{"term": {"status": "FAILED"}},
                                              {"range": {"ended_at": {"gt": since}}}],
                                   # re-runs started by the agent itself are already being handled
                                   "must_not": [{"prefix": {"trigger": "nightowl-fix"}}]}}})
            for run in os_client.hits(resp):
                if run["run_id"] in seen:
                    continue
                seen.add(run["run_id"])
                print(f"[{tools.ist(run['ended_at'])}] FAILED run detected: {run['run_id']} ({run.get('error_class')})")
                telegram_ping(f"🚨 {run['pipeline']} FAILED ({run.get('error_class')}).\n"
                              f"🦉 NightOwl is investigating via OpenSearch…")
                wake_agent(run)
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            print(f"  watcher error (will retry): {exc}")
        time.sleep(interval)
