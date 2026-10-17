"""A partitioned PostHog run asks the API for exactly its window."""
import importlib.util
import sys
from pathlib import Path

import dagster as dg

ROOT = Path(__file__).resolve().parent.parent


class _Resp:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {"results": [], "next": None}


def _asset(monkeypatch, **extra):
    folder = ROOT / "assets/ingestion/posthog_events_ingestion"
    spec = importlib.util.spec_from_file_location("ph", folder / "component.py")
    mod = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    monkeypatch.setenv("POSTHOG_API_KEY", "k")
    attrs = dict(asset_name="events", project_id="1", **extra)
    return mod, next(a for a in mod.PostHogEventsIngestionComponent(**attrs).build_defs(None).assets)


def _params(monkeypatch, partition_key=None, **extra):
    import requests

    calls = []
    monkeypatch.setattr(requests, "get", lambda url, **kw: (calls.append(kw.get("params")), _Resp())[1])
    mod, asset = _asset(monkeypatch, **extra)
    r = dg.materialize([asset], partition_key=partition_key, raise_on_error=False)
    return r, calls


def test_partitioned_run_fetches_its_window(monkeypatch):
    r, calls = _params(monkeypatch, partition_key="2026-01-03", partition_type="daily", partition_start="2026-01-01")
    assert r.success, r
    assert calls[0]["after"] == "2026-01-03T00:00:00Z" and calls[0]["before"] == "2026-01-04T00:00:00Z"


def test_unpartitioned_run_still_uses_since_days(monkeypatch):
    r, calls = _params(monkeypatch, since_days=3)
    assert r.success, r
    assert "before" not in calls[0] and calls[0]["after"].endswith("Z")
