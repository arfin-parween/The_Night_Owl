"""Guarded remediation. The agent can only PROPOSE (open_incident). Applying a fix:
  * requires an open incident with a whitelisted fix type,
  * requires a named human approver,
  * changes config only (never code), shows a diff, audits everything,
  * re-runs the pipeline and, on success, writes the postmortem into memory.
"""
from __future__ import annotations

import difflib
from datetime import datetime

import yaml

from . import os_client, pipeline, settings, tools

NOT_HUMAN = {"", "agent", "ai", "bot", "nightowl", "nightowl-agent", "openclaw", "assistant", "system"}


def _audit(action: str, incident_id: str, actor: str, **details) -> None:
    os_client.index_doc(settings.IDX_AUDIT, {"@timestamp": settings.iso(), "actor": actor, "action": action,
                                             "incident_id": incident_id, "details": details})


def _config_change(fix_type: str, params: dict) -> str:
    before = settings.CONFIG_FILE.read_text(encoding="utf-8")
    cfg = yaml.safe_load(before)
    if fix_type == "add_column_alias":
        cfg["column_aliases"] = {**(cfg.get("column_aliases") or {}), params["from"]: params["to"]}
    elif fix_type == "enable_quarantine":
        cfg["data_quality"]["quarantine_bad_rows"] = True
    else:
        return "(no config change; re-run only)"
    after = yaml.safe_dump(cfg, sort_keys=False)
    settings.CONFIG_FILE.write_text(after, encoding="utf-8")
    diff = difflib.unified_diff(before.splitlines(), after.splitlines(), "config/pipeline.yaml (before)",
                                "config/pipeline.yaml (after)", lineterm="", n=1)
    return "\n".join(diff)


def apply(incident_id: str, approved_by: str) -> str:
    approver = (approved_by or "").strip()
    if approver.lower() in NOT_HUMAN:
        raise SystemExit("Refused: a fix needs a named HUMAN approver (--approved-by <name>).")
    inc = tools.show_incident(incident_id)
    if inc["status"] != "open":
        raise SystemExit(f"Refused: {incident_id} is '{inc['status']}', not open.")
    fix_type, params = inc["fix_type"], inc.get("fix_params") or {}
    if fix_type == "none":
        raise SystemExit(f"{incident_id} has no automatic fix; it needs a human action.")
    tools.validate_fix(fix_type, params)

    out = [f"Applying {fix_type} {params or ''} for {incident_id}, approved by {approver}"]
    _audit("fix_approved", incident_id, approver, fix_type=fix_type, fix_params=params)
    diff = _config_change(fix_type, params)
    out.append(diff)
    _audit("config_changed", incident_id, "nightowl-agent", diff=diff)

    out.append(f"Re-running {inc['pipeline']} for dt={inc['dt']} ...")
    summary = pipeline.run(inc["dt"], trigger=f"nightowl-fix:{incident_id}")
    _audit("rerun", incident_id, "nightowl-agent", run_id=summary["run_id"], status=summary["status"])

    if summary["status"] != "SUCCESS":
        out.append(f"Re-run {summary['run_id']} FAILED again: {summary.get('error_class')} "
                   f"{tools._short(summary.get('error_message'), 300)}\nIncident stays open.")
        return "\n".join(out)

    resolved = settings.now()
    failed_at = datetime.fromisoformat(inc["failed_at"])
    mttr = (resolved - failed_at).total_seconds()
    extra = f", quarantined {summary['rows_quarantined']} rows" if summary.get("rows_quarantined") else ""
    inc.update({
        "status": "resolved", "resolved_at": settings.iso(resolved), "mttr_s": round(mttr), "approved_by": approver,
        "resolution": f"Applied {fix_type} {params or ''} (approved by {approver}); re-run {summary['run_id']} "
                      f"succeeded with rows_out={summary.get('rows_out')}{extra}.",
    })
    inc["memory_text"] = " | ".join(str(inc[k]) for k in ("title", "error_signature", "root_cause",
                                                          "resolution", "prevention") if inc.get(k))
    for k in ("_id", "_score"):
        inc.pop(k, None)
    # full re-index (not a partial update) so the ingest pipeline re-embeds the postmortem
    os_client.index_doc(settings.IDX_INCIDENTS, inc, doc_id=incident_id, refresh=True)
    _audit("incident_resolved", incident_id, "nightowl-agent", mttr_s=round(mttr))
    out.append(f"✅ Re-run {summary['run_id']} SUCCEEDED (rows_out={summary.get('rows_out')}{extra}).")
    out.append(f"{incident_id} resolved. MTTR {int(mttr // 60)}m {int(mttr % 60)}s from failure to green.")
    out.append("Postmortem saved to incident memory; next time a similar failure appears it will be found.")
    return "\n".join(out)
