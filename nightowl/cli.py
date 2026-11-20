"""nightowl: one command for the presenter and for the AI agent.

Presenter:  setup | run | break <scenario> | reset | deliver | watch | doctor
Agent:      status | failures | timeline | schema-diff | changes | logs-around | search-logs |
            similar | impact | incident open/pending/show | fix apply
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys

from . import settings


def _need_os():
    from . import os_client

    if not os_client.available(fresh=True):
        raise SystemExit(f"OpenSearch is not reachable at {settings.OPENSEARCH_URL}. Run `docker compose up -d`.")


def cmd_doctor(_args) -> None:
    from . import os_client, tools

    ok = True

    def check(name, passed, hint=""):
        nonlocal ok
        ok &= bool(passed)
        print(f"  {'✅' if passed else '❌'} {name}" + ("" if passed else f"  -> {hint}"))

    print("NightOwl doctor")
    java = shutil.which("java")
    check("Java on PATH", java, "sudo apt install -y openjdk-17-jdk")
    try:
        import pyspark  # noqa: F401
        check(f"PySpark {pyspark.__version__}", True)
    except ImportError:
        check("PySpark installed", False, "pip install -e .")
    else:
        if java:
            from . import pipeline

            check("Spark start-up cache (fast runs)", pipeline.ensure_cds_archive(), "see output above; runs still work, just slower")
    up = os_client.available(fresh=True)
    check(f"OpenSearch reachable at {settings.OPENSEARCH_URL}", up, "docker compose up -d (wait ~60s)")
    if up:
        c = os_client.client()
        missing = [i for i in settings.ALL_INDICES if not c.indices.exists(index=i)]
        check("NightOwl indices exist", not missing, f"missing {missing}; run `nightowl setup`")
        mid = settings.load_state().get("model_id")
        check("Embedding model registered (hybrid search)", mid, "run `nightowl setup` (or accept BM25 fallback)")
        if mid and not missing:
            try:
                out = tools.similar("column renamed upstream", k=1)
                check("Hybrid search works", "hybrid" in out, out.splitlines()[0])
            except Exception as exc:
                check("Hybrid search works", False, str(exc)[:200])
    check("OpenClaw CLI on PATH", shutil.which("openclaw"), "curl -fsSL https://openclaw.ai/install.sh | bash")
    from pathlib import Path

    skill = Path.home() / ".openclaw" / "workspace" / "skills" / "nightowl" / "SKILL.md"
    check("NightOwl skill installed for OpenClaw", skill.exists(), "bash scripts/install_openclaw_skill.sh")
    check("OPENCLAW_HOOK_TOKEN set in .env", settings.OPENCLAW_HOOK_TOKEN, "see README step 6")
    check("TELEGRAM_CHAT_ID set in .env", settings.TELEGRAM_CHAT_ID, "see README step 6")
    print("All good. Break a leg! 🦉" if ok else "Fix the ❌ items above.")


def cmd_setup(args):
    from . import datagen, pipeline, scenarios, setup_os

    setup_os.run_setup(reset=args.reset, no_ml=args.no_ml)
    datagen.write_stores()
    scenarios.reset()
    pipeline.ensure_cds_archive()
    print("Running one clean pipeline run so there is a fresh 'last known good' baseline ...")
    s = pipeline.run(trigger="setup")
    print(f"Baseline run: {s['run_id']} -> {s['status']}")


def cmd_run(args):
    from . import pipeline

    s = pipeline.run(args.date)
    print(json.dumps({k: s.get(k) for k in ("run_id", "status", "failed_stage", "error_class", "rows_in",
                                            "rows_out", "rows_quarantined", "duration_s")}, indent=2))
    sys.exit(0 if s["status"] == "SUCCESS" else 1)


def cmd_break(args):
    from . import scenarios

    dt = scenarios.break_pipeline(args.scenario, args.date, with_injection=args.with_injection)
    print(f"💥 Scenario '{args.scenario}' armed for dt={dt}: {scenarios.SCENARIOS[args.scenario]}")
    if args.run:
        cmd_run(argparse.Namespace(date=dt))
    else:
        print("Now run: nightowl run")


def cmd_reset(args):
    from . import scenarios

    dt = scenarios.reset(args.date)
    print(f"Reset: clean input for dt={dt} and default config. (Incident memory is kept.)")
    from . import pipeline

    pipeline.ensure_cds_archive()  # no-op once built; reset is part of the pre-talk checklist
    if not args.no_run:
        s = pipeline.run(dt, trigger="reset-baseline")
        print(f"Fresh baseline run: {s['run_id']} -> {s['status']}")


def cmd_deliver(args):
    from . import scenarios

    dt = scenarios.deliver_late_partition(args.date)
    print(f"Upstream retry delivered the orders partition for dt={dt}.")


def cmd_watch(args):
    from . import watcher

    watcher.watch(args.interval)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(prog="nightowl", description="NightOwl: autonomous AI on-call engineer toolkit")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("setup", help="[presenter] create indices, model, seed data, baseline run")
    s.add_argument("--reset", action="store_true", help="drop and recreate all NightOwl indices")
    s.add_argument("--no-ml", action="store_true", help="skip the embedding model (BM25 only)")
    s.set_defaults(func=cmd_setup)
    s = sub.add_parser("run", help="[presenter] run the Spark pipeline now")
    s.add_argument("--date")
    s.set_defaults(func=cmd_run)
    s = sub.add_parser("break", help="[presenter] arm a failure scenario")
    s.add_argument("scenario", choices=["schema_drift", "bad_data", "missing_partition"])
    s.add_argument("--date")
    s.add_argument("--run", action="store_true", help="also run the pipeline immediately")
    s.add_argument("--with-injection", action="store_true", help="bad_data only: plant a prompt injection in logs")
    s.set_defaults(func=cmd_break)
    s = sub.add_parser("reset", help="[presenter] clean input + default config + green baseline run")
    s.add_argument("--date")
    s.add_argument("--no-run", action="store_true", help="skip the baseline run")
    s.set_defaults(func=cmd_reset)
    s = sub.add_parser("deliver", help="[presenter] simulate upstream finally delivering a late partition")
    s.add_argument("--date")
    s.set_defaults(func=cmd_deliver)
    s = sub.add_parser("watch", help="[presenter] detect failures in OpenSearch and wake the OpenClaw agent")
    s.add_argument("--interval", type=int, default=5)
    s.set_defaults(func=cmd_watch)
    sub.add_parser("doctor", help="check the whole demo setup").set_defaults(func=cmd_doctor)

    # ---- agent tools
    from . import tools

    def simple(name, help_, fn, *arg_specs):
        sp = sub.add_parser(name, help=f"[agent] {help_}")
        for spec in arg_specs:
            sp.add_argument(*spec[0], **spec[1])
        sp.set_defaults(func=lambda a: (_need_os(), print(fn(a)))[1])

    run_arg = (["run_id"], {"nargs": "?", "default": "latest", "help": "run id or 'latest' (latest failed run)"})
    simple("status", "recent runs, last-24h health, today's source partition", lambda a: tools.status())
    simple("failures", "failed runs in the last N hours", lambda a: tools.failures(a.hours),
           (["--hours"], {"type": int, "default": 24}))
    simple("timeline", "every structured event of one run", lambda a: tools.timeline(a.run_id), run_arg)
    simple("schema-diff", "compare input schema with the last successful run", lambda a: tools.schema_diff(a.run_id),
           run_arg)
    simple("changes", "what changed? deploys/rollouts/config/maintenance", lambda a: tools.changes(a.hours),
           (["--hours"], {"type": int, "default": 6}))
    simple("logs-around", "WARN/ERROR logs of OTHER services around a run", lambda a: tools.logs_around(
        a.run_id, a.minutes, a.include_info), run_arg, (["--minutes"], {"type": int, "default": 90}),
        (["--include-info"], {"action": "store_true"}))
    simple("search-logs", "free-text search across all logs", lambda a: tools.search_logs(a.text, a.hours),
           (["text"], {}), (["--hours"], {"type": int, "default": 24}))
    simple("similar", "hybrid search of past incidents (team memory)", lambda a: tools.similar(a.text, a.k),
           (["text"], {}), (["-k"], {"type": int, "default": 3}))
    simple("impact", "downstream blast radius + time to SLA breach", lambda a: tools.impact(a.pipeline),
           (["pipeline"], {"nargs": "?", "default": settings.PIPELINE}))

    inc = sub.add_parser("incident", help="[agent] open / list / show incidents")
    isub = inc.add_subparsers(dest="icmd", required=True)
    o = isub.add_parser("open", help="open an incident with a PROPOSED fix (does not apply it)")
    o.add_argument("--run-id", default="latest")
    o.add_argument("--title", required=True)
    o.add_argument("--root-cause", required=True)
    o.add_argument("--evidence", required=True)
    o.add_argument("--fix-type", required=True, choices=sorted(tools.ALLOWED_FIXES))
    o.add_argument("--from", dest="from_col")
    o.add_argument("--to", dest="to_col")
    o.add_argument("--severity", default="SEV2", choices=["SEV1", "SEV2", "SEV3"])
    o.add_argument("--prevention", default="")

    def _open(a):
        _need_os()
        params = {"from": a.from_col, "to": a.to_col} if a.fix_type == "add_column_alias" else {}
        print(tools.open_incident(a.run_id, a.title, a.root_cause, a.evidence, a.fix_type, params,
                                  a.severity, a.prevention))

    o.set_defaults(func=_open)
    isub.add_parser("pending", help="open incidents awaiting approval").set_defaults(
        func=lambda a: (_need_os(), print(tools.pending())))
    sh = isub.add_parser("show")
    sh.add_argument("incident_id")
    sh.set_defaults(func=lambda a: (_need_os(), print(json.dumps(tools.show_incident(a.incident_id), indent=2,
                                                                 default=str, ensure_ascii=False))))

    fx = sub.add_parser("fix", help="[agent] apply an APPROVED fix")
    fsub = fx.add_subparsers(dest="fcmd", required=True)
    ap = fsub.add_parser("apply", help="apply the proposed fix of an open incident, re-run, record postmortem")
    ap.add_argument("incident_id")
    ap.add_argument("--approved-by", required=True, help="name of the human who approved")

    def _apply(a):
        _need_os()
        from . import fixes

        print(fixes.apply(a.incident_id, a.approved_by))

    ap.set_defaults(func=_apply)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
