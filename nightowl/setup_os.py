"""One-time OpenSearch setup: indices, ML Commons embedding model, ingest + hybrid
search pipelines, seeded history (past runs, past incidents, lineage), Dashboards index patterns."""
from __future__ import annotations

import os
import random
import time
from datetime import timedelta

from . import os_client, settings
from .seed_data import INCIDENTS, LINEAGE, CLEAN_SCHEMA

KW = {"type": "keyword"}
TEXT_KW = {"type": "text", "fields": {"raw": {"type": "keyword", "ignore_above": 512}}}
DATE = {"type": "date"}
OPAQUE = {"type": "object", "enabled": False}


def _mappings(with_vectors: bool) -> dict:
    incident_props = {
        "incident_id": KW, "title": TEXT_KW, "pipeline": KW, "status": KW, "severity": KW, "run_id": KW,
        "opened_at": DATE, "resolved_at": DATE, "failed_at": DATE, "mttr_s": {"type": "float"},
        "error_class": KW, "error_signature": {"type": "text"}, "root_cause": {"type": "text"},
        "evidence": {"type": "text"}, "resolution": {"type": "text"}, "prevention": {"type": "text"},
        "fix_type": KW, "fix_params": OPAQUE, "proposed_by": KW, "approved_by": KW,
        "memory_text": {"type": "text"},
    }
    incident_settings: dict = {}
    if with_vectors:
        incident_props["memory_embedding"] = {
            "type": "knn_vector", "dimension": settings.EMBED_DIM,
            "method": {"name": "hnsw", "space_type": "l2", "engine": "lucene"}}
        incident_settings = {"index.knn": True, "default_pipeline": settings.EMBED_PIPELINE}
    return {
        settings.IDX_LOGS: ({}, {"@timestamp": DATE, "service": KW, "pipeline": KW, "run_id": KW, "dt": KW,
                                 "level": KW, "stage": KW, "message": TEXT_KW, "error_message": {"type": "text"},
                                 "error_class": KW, "exception_type": KW, "dq_details": OPAQUE,
                                 "input_schema": KW, "missing_columns": KW, "available_columns": KW}),
        settings.IDX_RUNS: ({}, {"run_id": KW, "pipeline": KW, "dt": KW, "status": KW, "trigger": KW,
                                 "started_at": DATE, "ended_at": DATE, "duration_s": {"type": "float"},
                                 "rows_in": {"type": "long"}, "rows_out": {"type": "long"},
                                 "rows_quarantined": {"type": "long"},
                                 "input_columns": KW, "input_schema": KW, "config_version": KW,
                                 "column_aliases": OPAQUE, "error_class": KW, "error_message": {"type": "text"},
                                 "exception_type": KW, "failed_stage": KW, "dq_details": OPAQUE, "host": KW}),
        settings.IDX_CHANGES: ({}, {"@timestamp": DATE, "service": KW, "change_type": KW, "version": KW,
                                    "summary": {"type": "text"}, "author": KW, "pr": KW}),
        settings.IDX_INCIDENTS: (incident_settings, incident_props),
        settings.IDX_LINEAGE: ({}, {"pipeline": KW, "owner": KW, "downstream": OPAQUE, "upstream": OPAQUE}),
        settings.IDX_AUDIT: ({}, {"@timestamp": DATE, "actor": KW, "action": KW, "incident_id": KW,
                                  "details": OPAQUE}),
    }


def _req(method: str, path: str, body=None, params=None):
    return os_client.client().transport.perform_request(method, path, body=body, params=params)


# ---------------------------------------------------------------- ML Commons
def _wait_task(task_id: str, timeout_s: int) -> dict:
    """Polls an ML Commons task until it leaves CREATED/RUNNING (or our timeout expires)."""
    deadline = time.time() + timeout_s
    while True:
        t = _req("GET", f"/_plugins/_ml/tasks/{task_id}")
        if t.get("state") not in ("CREATED", "RUNNING") or time.time() > deadline:
            return t
        time.sleep(3)


def _find_registered_model(version: str) -> str | None:
    """An already-registered copy of this exact artifact (matched by SHA-256), so re-runs don't
    download 90MB again. Prefers a DEPLOYED copy."""
    want = settings.EMBED_MODEL_HASHES.get(version)
    if not want:
        return None
    try:
        res = _req("POST", "/_plugins/_ml/models/_search", body={"size": 20, "query": {"bool": {
            "must": [{"term": {"model_content_hash_value": want}}],
            "must_not": [{"exists": {"field": "chunk_number"}}]}}})
    except Exception:
        return None
    usable = [h for h in res.get("hits", {}).get("hits", [])
              if h["_source"].get("model_state") not in (None, "REGISTERING")]
    usable.sort(key=lambda h: h["_source"].get("model_state") != "DEPLOYED")
    return usable[0]["_id"] if usable else None


def _deploy(mid: str, attempts: int = 3) -> bool:
    """Explicit deploy, waiting on the deploy *task* (model_state can be stale from a previous attempt).
    Bounded retries: a hash mismatch / circuit breaker under heap pressure is transient, an unknown
    TorchScript op is not."""
    for attempt in range(1, attempts + 1):
        if _req("GET", f"/_plugins/_ml/models/{mid}").get("model_state") == "DEPLOYED":
            return True
        task = _req("POST", f"/_plugins/_ml/models/{mid}/_deploy")
        t = _wait_task(task["task_id"], timeout_s=300)
        if t.get("state") == "COMPLETED":
            return True
        err = str(t.get("error") or t.get("state"))
        print(f"    deploy attempt {attempt}/{attempts} failed: {err[:160]}")
        if "Unknown builtin op" in err:
            print("    this model artifact needs a newer PyTorch than this OpenSearch ships; try another version")
            return False
        time.sleep(10)
    return False


def setup_embedding_model() -> str | None:
    """Registers + deploys a pretrained sentence-transformer inside OpenSearch (ML Commons)."""
    state = settings.load_state()
    mid = state.get("model_id")
    if mid:
        try:
            if _req("GET", f"/_plugins/_ml/models/{mid}").get("model_state") == "DEPLOYED":
                print(f"  embedding model already deployed: {mid}")
                return mid
        except Exception:
            pass

    _req("PUT", "/_cluster/settings", body={"persistent": {
        "plugins.ml_commons.only_run_on_ml_node": False,
        "plugins.ml_commons.native_memory_threshold": 99,
        "plugins.ml_commons.model_access_control_enabled": False,
    }})
    version = os.environ.get("EMBED_MODEL_VERSION", settings.EMBED_MODEL_VERSION)
    mid = _find_registered_model(version)
    if mid:
        print(f"  reusing registered model {mid}")
    else:
        print(f"  registering {settings.EMBED_MODEL} v{version} (first time downloads ~90MB, be patient)")
        try:
            # No deploy=true: auto-deploy races the end of registration and, on a 1GB heap, has
            # failed with "model content changed". Register first, then deploy explicitly.
            task = _req("POST", "/_plugins/_ml/models/_register", body={
                "name": settings.EMBED_MODEL, "version": version, "model_format": "TORCH_SCRIPT"})
            t = _wait_task(task["task_id"], timeout_s=600)
            mid = t.get("model_id") if t.get("state") == "COMPLETED" else None
            if not mid:
                print(f"    register failed: {t.get('state')} {t.get('error', '')}")
        except Exception as exc:
            print(f"    register call failed: {exc}")
    if mid and _deploy(mid):
        settings.save_state(model_id=mid)
        print(f"  embedding model deployed: {mid}")
        return mid
    print("  !! could not deploy the embedding model. Similar-incident search will fall back to BM25.")
    return None


def setup_pipelines(model_id: str) -> None:
    _req("PUT", f"/_ingest/pipeline/{settings.EMBED_PIPELINE}", body={
        "description": "Embed incident memory text with the in-cluster MiniLM model",
        "processors": [{"text_embedding": {"model_id": model_id,
                                           "field_map": {"memory_text": "memory_embedding"}}}]})
    _req("PUT", f"/_search/pipeline/{settings.HYBRID_PIPELINE}", body={
        "description": "Normalize + combine BM25 and neural scores",
        "phase_results_processors": [{"normalization-processor": {
            "normalization": {"technique": "min_max"},
            "combination": {"technique": "arithmetic_mean", "parameters": {"weights": [0.3, 0.7]}}}}]})
    print("  ingest pipeline + hybrid search pipeline ready")


# ---------------------------------------------------------------- indices + seed
def create_indices(with_vectors: bool, reset: bool) -> None:
    c = os_client.client()
    for name, (idx_settings, props) in _mappings(with_vectors).items():
        if c.indices.exists(index=name):
            if not reset:
                continue
            c.indices.delete(index=name)
        body = {"mappings": {"properties": props}}
        if idx_settings:
            body["settings"] = idx_settings
        c.indices.create(index=name, body=body)
        print(f"  created index {name}")


def memory_text(inc: dict) -> str:
    return " | ".join(str(inc.get(k, "")) for k in
                      ("title", "error_signature", "root_cause", "resolution", "prevention") if inc.get(k))


def seed(reset: bool) -> None:
    c = os_client.client()
    if not reset and c.count(index=settings.IDX_RUNS)["count"] > 0:
        print("  seed data already present (use --reset to rebuild)")
        return
    rng = random.Random(42)
    now = settings.now()
    runs = []
    for d in range(10, 0, -1):
        day = (now - timedelta(days=d)).date().isoformat()
        start = now.replace(hour=20, minute=30, second=0, microsecond=0) - timedelta(days=d + 1)
        dur = round(rng.uniform(38, 61), 1)
        rows = rng.randint(38500, 41500)
        runs.append({"run_id": f"{settings.PIPELINE}-{day}-hist{d:02d}", "pipeline": settings.PIPELINE, "dt": day,
                     "trigger": "scheduler", "status": "SUCCESS", "started_at": settings.iso(start),
                     "ended_at": settings.iso(start + timedelta(seconds=dur)), "duration_s": dur,
                     "rows_in": rows, "rows_out": 48, "input_columns": [s.split(":")[0] for s in CLEAN_SCHEMA],
                     "input_schema": CLEAN_SCHEMA, "config_version": "3f9a1c2e", "column_aliases": {},
                     "host": "etl-worker-02"})
    os_client.bulk(settings.IDX_RUNS, runs, id_field="run_id")

    incidents = []
    for inc in INCIDENTS:
        doc = dict(inc)
        opened = now - timedelta(days=doc.pop("days_ago"))
        doc["opened_at"] = settings.iso(opened)
        doc["failed_at"] = settings.iso(opened - timedelta(minutes=4))
        doc["resolved_at"] = settings.iso(opened + timedelta(seconds=doc["mttr_s"]))
        doc["memory_text"] = memory_text(doc)
        incidents.append(doc)
    os_client.bulk(settings.IDX_INCIDENTS, incidents, id_field="incident_id")
    os_client.bulk(settings.IDX_LINEAGE, LINEAGE)

    changes = [
        {"@timestamp": settings.iso(now - timedelta(hours=h)), "service": s, "change_type": t, "version": v,
         "summary": m, "author": a}
        for h, s, t, v, m, a in [
            (30, "orders-service", "deploy", "v4.11.3", "Add idempotency key to order create API", "checkout-platform-team"),
            (52, "store-app", "rollout", "7.1.4", "Rollout 7.1.4 to 100% of darkstores", "store-experience-team"),
            (60, "data-platform", "config", "spark-3.5->4.0", "Upgrade ETL workers to Spark 4.0", "data-platform-team"),
        ]]
    os_client.bulk(settings.IDX_CHANGES, changes)
    print(f"  seeded {len(runs)} historical runs, {len(incidents)} past incidents, lineage, change history")


def dashboards_index_patterns() -> None:
    import requests

    patterns = {"nightowl-logs": "@timestamp", "nightowl-runs": "started_at",
                "nightowl-changes": "@timestamp", "nightowl-incidents": "opened_at"}
    for title, tf in patterns.items():
        try:
            requests.post(f"{settings.DASHBOARDS_URL}/api/saved_objects/index-pattern/{title}",
                          params={"overwrite": "true"}, headers={"osd-xsrf": "true"},
                          json={"attributes": {"title": title, "timeFieldName": tf}}, timeout=10)
        except Exception:
            print("  (Dashboards not reachable yet; index patterns skipped. Re-run `nightowl setup` later.)")
            return
    print("  Dashboards index patterns created")


def run_setup(reset: bool = False, no_ml: bool = False) -> None:
    print(f"Connecting to OpenSearch at {settings.OPENSEARCH_URL} ...")
    if not os_client.wait_until_up(180):
        raise SystemExit("OpenSearch is not reachable. Is `docker compose up -d` running?")
    info = os_client.client().info()
    print(f"  OpenSearch {info['version']['number']} is up")
    model_id = None if no_ml else setup_embedding_model()
    if model_id:
        setup_pipelines(model_id)
    else:
        settings.save_state(model_id=None)
    create_indices(with_vectors=bool(model_id), reset=reset)
    seed(reset)
    dashboards_index_patterns()
    print("Setup complete.")
