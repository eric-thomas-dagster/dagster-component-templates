"""MLflow Workspace Component.

Auto-enumerates MLflow experiments and registered models via the Tracking
REST API. Emits one Dagster asset per experiment (runs metadata) and one
per registered model (versions metadata).
"""
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None  # type: ignore
    DefsStateConfig = None  # type: ignore
    DefsStateConfigArgs = None  # type: ignore
    ResolvedDefsStateConfig = Any  # type: ignore
    _HAS_STATE_BACKED = False


@dataclass
class MLflowSelector(dg.Resolvable):
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, name: str) -> bool:
        import fnmatch
        if self.exclude_by_name and name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(name, p) for p in self.exclude_by_pattern):
            return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(name, p) for p in self.by_pattern):
            return True
        return False


def _enumerate_mlflow(tracking_uri, verify_ssl, auth=None) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    if auth:
        session.auth = auth

    out: dict = {"experiments": [], "models": []}
    try:
        r = session.get(
            f"{tracking_uri.rstrip('/')}/api/2.0/mlflow/experiments/search",
            params={"max_results": 1000},
            timeout=60,
        )
        r.raise_for_status()
        for e in (r.json() or {}).get("experiments", []):
            eid = e.get("experiment_id")
            ename = e.get("name")
            if eid and ename:
                out["experiments"].append({"id": eid, "name": ename})
    except Exception:  # noqa: BLE001
        pass
    try:
        r = session.get(
            f"{tracking_uri.rstrip('/')}/api/2.0/mlflow/registered-models/search",
            params={"max_results": 1000},
            timeout=60,
        )
        r.raise_for_status()
        for m in (r.json() or {}).get("registered_models", []):
            mname = m.get("name")
            if mname:
                out["models"].append({"name": mname})
    except Exception:  # noqa: BLE001
        pass
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class MLflowWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        tracking_uri_env_var: str
        username_env_var: Optional[str] = None
        password_env_var: Optional[str] = None
        verify_ssl: bool = True

        experiment_selector: Optional[MLflowSelector] = None
        model_selector: Optional[MLflowSelector] = None
        runs_limit: int = 100

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["mlflow"])
        compute_kind: str = "mlflow"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"MLflowWorkspace[{hashlib.sha256(self.tracking_uri_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            uri = os.environ.get(self.tracking_uri_env_var, "")
            auth = None
            if self.username_env_var and self.password_env_var:
                auth = (os.environ.get(self.username_env_var, ""), os.environ.get(self.password_env_var, ""))
            snapshot = _enumerate_mlflow(uri, self.verify_ssl, auth)
            if self.experiment_selector is not None:
                snapshot["experiments"] = [e for e in snapshot["experiments"] if self.experiment_selector.matches(e["name"])]
            if self.model_selector is not None:
                snapshot["models"] = [m for m in snapshot["models"] if self.model_selector.matches(m["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for e in state.get("experiments", []):
                assets.append(self._build_experiment_asset(e["id"], e["name"]))
            for m in state.get("models", []):
                assets.append(self._build_model_asset(m["name"]))
            return dg.Definitions(assets=assets)

        def _build_experiment_asset(self, exp_id: str, exp_name: str):
            _self = self
            safe = "".join(c if c.isalnum() or c == "_" else "_" for c in exp_name)[:40] or exp_id
            key = dg.AssetKey([*self.asset_key_prefix, "experiment", safe])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={"mlflow_experiment_id": dg.MetadataValue.text(exp_id),
                          "mlflow_experiment_name": dg.MetadataValue.text(exp_name)},
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e
                uri = os.environ.get(_self.tracking_uri_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                if _self.username_env_var and _self.password_env_var:
                    session.auth = (os.environ.get(_self.username_env_var, ""), os.environ.get(_self.password_env_var, ""))

                r = session.post(
                    f"{uri.rstrip('/')}/api/2.0/mlflow/runs/search",
                    json={"experiment_ids": [exp_id], "max_results": _self.runs_limit},
                    timeout=60,
                )
                r.raise_for_status()
                runs = (r.json() or {}).get("runs", [])
                rows = []
                for run in runs:
                    info = run.get("info") or {}
                    data = run.get("data") or {}
                    row = {
                        "run_id": info.get("run_id"),
                        "status": info.get("status"),
                        "start_time": info.get("start_time"),
                        "end_time": info.get("end_time"),
                        "artifact_uri": info.get("artifact_uri"),
                    }
                    for m in data.get("metrics", []):
                        row[f"metric_{m.get('key')}"] = m.get("value")
                    for p in data.get("params", []):
                        row[f"param_{p.get('key')}"] = p.get("value")
                    rows.append(row)
                df = pd.DataFrame(rows)
                context.add_output_metadata({"row_count": len(df), "experiment": exp_name})
                return df

            return _asset

        def _build_model_asset(self, model_name: str):
            _self = self
            safe = "".join(c if c.isalnum() or c == "_" else "_" for c in model_name)[:60] or "model"
            key = dg.AssetKey([*self.asset_key_prefix, "model", safe])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={"mlflow_model_name": dg.MetadataValue.text(model_name)},
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e
                uri = os.environ.get(_self.tracking_uri_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                if _self.username_env_var and _self.password_env_var:
                    session.auth = (os.environ.get(_self.username_env_var, ""), os.environ.get(_self.password_env_var, ""))
                r = session.get(
                    f"{uri.rstrip('/')}/api/2.0/mlflow/model-versions/search",
                    params={"filter": f"name = '{model_name}'", "max_results": 1000},
                    timeout=60,
                )
                r.raise_for_status()
                versions = (r.json() or {}).get("model_versions", [])
                df = pd.DataFrame(versions)
                context.add_output_metadata({"row_count": len(df), "model": model_name})
                return df

            return _asset

else:
    class MLflowWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("MLflowWorkspaceComponent requires Dagster with StateBackedComponent support.")
