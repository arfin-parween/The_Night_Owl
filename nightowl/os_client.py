"""Thin OpenSearch helpers. Everything the agent knows comes through here."""
from __future__ import annotations

import json
import time
import warnings

from . import settings

warnings.filterwarnings("ignore", message=".*verify_certs.*")

_client = None


def client():
    global _client
    if _client is None:
        from opensearchpy import OpenSearch

        _client = OpenSearch(
            hosts=[settings.OPENSEARCH_URL],
            use_ssl=settings.OPENSEARCH_URL.startswith("https"),
            verify_certs=False,
            ssl_show_warn=False,
            timeout=60,
        )
    return _client


_avail_cache: tuple[float, bool] | None = None


def available(fresh: bool = False) -> bool:
    global _avail_cache
    if settings.OS_DISABLED:
        return False
    if not fresh and _avail_cache and time.time() - _avail_cache[0] < 20:
        return _avail_cache[1]
    try:
        client().info()
        ok = True
    except Exception:
        ok = False
    _avail_cache = (time.time(), ok)
    return ok


def wait_until_up(seconds: int = 180) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if available(fresh=True):
            return True
        time.sleep(3)
    return False


def index_doc(index: str, doc: dict, doc_id: str | None = None, refresh: bool = False) -> None:
    client().index(index=index, body=doc, id=doc_id, refresh="true" if refresh else "false")


def bulk(index: str, docs: list[dict], id_field: str | None = None) -> int:
    from opensearchpy import helpers

    actions = []
    for d in docs:
        action = {"_index": index, "_source": d}
        if id_field and d.get(id_field):
            action["_id"] = d[id_field]
        actions.append(action)
    ok, _errors = helpers.bulk(client(), actions, raise_on_error=False, refresh="true")
    return ok


def search(index: str, body: dict, **params) -> dict:
    return client().search(index=index, body=body, params=params or None)


def hits(resp: dict) -> list[dict]:
    return [h["_source"] | {"_id": h["_id"], "_score": h.get("_score")} for h in resp["hits"]["hits"]]


def pretty(obj) -> str:
    return json.dumps(obj, indent=2, default=str)
