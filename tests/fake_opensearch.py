"""Tiny in-memory stand-in for OpenSearch, only for offline tests of NightOwl's Python logic.
Supports the query subset NightOwl uses: term, terms, range (incl. now-Nh), bool, multi_match,
hybrid (first sub-query), sort, terms/filter aggs. NOT a substitute for testing on real OpenSearch."""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta, timezone


def _parse_time(v):
    if isinstance(v, (int, float)):
        return v
    m = re.fullmatch(r"now(?:-(\d+)([hmd]))?", str(v))
    if m:
        n = datetime.now(timezone.utc)
        if m.group(1):
            n -= timedelta(**{{"h": "hours", "m": "minutes", "d": "days"}[m.group(2)]: int(m.group(1))})
        return n
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return v


def _vals(doc, field):
    v = doc.get(field)
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def _match(doc, q) -> float:
    if not q:
        return 1.0
    (kind, spec), = q.items()
    if kind == "match_all":
        return 1.0
    if kind == "term":
        (f, val), = spec.items()
        val = val["value"] if isinstance(val, dict) else val
        return 1.0 if val in _vals(doc, f) else 0.0
    if kind == "terms":
        (f, vals), = spec.items()
        return 1.0 if any(v in vals for v in _vals(doc, f)) else 0.0
    if kind == "range":
        (f, cond), = spec.items()
        vs = _vals(doc, f)
        if not vs:
            return 0.0
        x = _parse_time(vs[0])
        for op, bound in cond.items():
            b = _parse_time(bound)
            if op == "gte" and not x >= b: return 0.0
            if op == "gt" and not x > b: return 0.0
            if op == "lte" and not x <= b: return 0.0
            if op == "lt" and not x < b: return 0.0
        return 1.0
    if kind == "bool":
        score = 1.0
        for sub in spec.get("filter", []) + spec.get("must", []):
            s = _match(doc, sub)
            if not s:
                return 0.0
            score += s
        for sub in spec.get("must_not", []):
            if _match(doc, sub):
                return 0.0
        return score
    if kind == "multi_match":
        tokens = re.findall(r"\w+", spec["query"].lower())
        fields = [f.split("^")[0] for f in spec["fields"]]
        text = " ".join(str(doc.get(f, "")) for f in fields).lower()
        return float(sum(1 for t in tokens if t in text))
    if kind == "hybrid":
        return _match(doc, spec["queries"][0])
    if kind == "neural":
        return 0.5
    if kind == "prefix":
        (f, val), = spec.items()
        return 1.0 if any(str(v).startswith(val) for v in _vals(doc, f)) else 0.0
    raise NotImplementedError(kind)


def _aggs(docs, aggs):
    out = {}
    for name, spec in aggs.items():
        sub = spec.get("aggs")
        if "terms" in spec:
            counts = {}
            for d in docs:
                for v in _vals(d, spec["terms"]["field"]):
                    counts.setdefault(v, []).append(d)
            buckets = sorted(counts.items(), key=lambda kv: -len(kv[1]))[: spec["terms"].get("size", 10)]
            out[name] = {"buckets": [{"key": k, "doc_count": len(v), **(_aggs(v, sub) if sub else {})}
                                     for k, v in buckets]}
        elif "filter" in spec:
            sel = [d for d in docs if _match(d, spec["filter"])]
            out[name] = {"doc_count": len(sel), **(_aggs(sel, sub) if sub else {})}
    return out


class _Indices:
    def __init__(self, store):
        self.store = store

    def exists(self, index):
        return index in self.store

    def create(self, index, body=None):
        self.store[index] = {}

    def delete(self, index):
        self.store.pop(index, None)


class _Transport:
    def perform_request(self, *a, **k):
        return {}


class FakeOpenSearch:
    def __init__(self):
        self.store: dict[str, dict] = {}
        self.indices = _Indices(self.store)
        self.transport = _Transport()

    def info(self):
        return {"version": {"number": "fake"}}

    def index(self, index, body, id=None, refresh=None):
        self.store.setdefault(index, {})[id or uuid.uuid4().hex] = dict(body)

    def count(self, index):
        return {"count": len(self.store.get(index, {}))}

    def search(self, index, body, params=None):
        docs = [(i, d) for i, d in self.store.get(index, {}).items()]
        q = body.get("query")
        scored = [(i, d, _match(d, q)) for i, d in docs]
        scored = [x for x in scored if x[2]]
        for s in reversed(body.get("sort", [])):
            (f, o), = s.items()
            order = o if isinstance(o, str) else o.get("order", "asc")
            scored.sort(key=lambda x: str(x[1].get(f, "")), reverse=(order == "desc"))
        if not body.get("sort"):
            scored.sort(key=lambda x: -x[2])
        size = body.get("size", 10)
        resp = {"hits": {"hits": [{"_id": i, "_score": s, "_source": d} for i, d, s in scored[:size]]}}
        if body.get("aggs"):
            resp["aggregations"] = _aggs([d for _, d, _ in scored], body["aggs"])
        return resp


def install():
    """Patch nightowl.os_client to use the fake. Returns the fake instance."""
    from nightowl import os_client

    fake = FakeOpenSearch()
    os_client._client = fake
    os_client.available = lambda fresh=False: True

    def bulk(index, docs, id_field=None):
        for d in docs:
            fake.index(index, d, id=d.get(id_field) if id_field else None)
        return len(docs)

    os_client.bulk = bulk
    return fake
