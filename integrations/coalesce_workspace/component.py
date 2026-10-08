"""Coalesce Workspace Component.

Connects to a Coalesce environment, discovers its nodes (SQL transforms),
and builds one Dagster asset per node with upstream dependencies mirroring
the Coalesce DAG. Materializing an asset triggers a Coalesce run for its
node via the Coalesce Scheduler API and polls until the run reaches a
terminal state.

Extends StateBackedComponent so the Coalesce API is called once at prepare
time (write_state_to_path) and cached on disk. build_defs_from_state builds
asset specs from the cached node list with zero network calls, keeping
code-server reloads fast.

On first load (state_path is None) returns empty Definitions -- run
`dg utils refresh-defs-state` or `dagster dev` to populate the cache.

Real Coalesce API facts this file's behavior is built on (confirmed against
the live OpenAPI spec at https://docs.coalesce.io/api/coalesce-api.json and
https://docs.coalesce.io/llms-full.txt on 2026-10-08, not assumed):

- Run triggering/polling (`/scheduler/startRun`, `/scheduler/runStatus`) is
  Coalesce's separate, older "Scheduler API" -- it isn't in the public v1
  OpenAPI spec (that spec only documents `/api/v1/runs*`), but it's real and
  documented at https://docs.coalesce.io/docs/api/runs/run-status and friends.
  Its terminal run-status values are `completed`, `failed`, `canceled`
  (one L) -- NOT `succeeded`/`cancelled`/`error`, which is what this file
  used to check for. That meant the polling loop here never broke out on a
  genuinely successful run (`"succeeded"` never arrives) -- every
  materialization silently burned the full `timeout_seconds` before falling
  through to report success regardless of actual status, and a canceled run
  (`"canceled"`, one L) was never recognized as a failure. Fixed below.
- Per-run, per-node test results come from `GET /api/v1/runs/{runID}/results`
  (runID-only path -- no `/environments/{id}/` prefix despite the second-hand
  summary this change started from). Each `RunResult` has `nodeID` and an
  optional `hasTestFailures: bool` that is only PRESENT when `true` (per the
  spec's own field description) -- its absence means no test failed on that
  node, not "unknown".
- Test-failure gating is NOT automatic/architectural: Coalesce's own docs
  (data-quality-testing page) say Node-level tests can be configured to
  "halt the pipeline execution upon test failure" -- halting is opt-in per
  test, not a universal rule. A real "List Run Results With Failed Test"
  example response in Coalesce's docs shows a node with
  `"runState": "complete"` AND `"hasTestFailures": true` in the same object
  -- i.e. the node (and the overall run's `runStatus`) can complete
  successfully while still carrying a test failure, surfaced only through
  the separate `hasTestFailures` flag (shown as a "yellow" status in the
  Coalesce UI, distinct from green/success and red/failure). This is why
  `fail_run_on_test_failure` below defaults to False: Coalesce's own
  halt-on-failure setting (configured per test, in Coalesce) is already the
  authoritative decision for whether a test failure should stop the
  pipeline -- if a test was configured to halt, the run already comes back
  with a `failed`/`canceled` status and this file's existing exception path
  already catches that. This field exists only for teams that want Dagster
  itself to enforce failure regardless of each test's Coalesce-side config.
- Column-level tests run after the node's transform (confirmed empirically
  in the same example: the failing `N_REGIONKEY: Unique` test's query runs
  after the node's `Insert STG_NATION` stage) and do not have a halt option
  in the docs -- node-level tests are the ones with the optional before/after
  + halt toggle.

Optional Catalog (Castor) metadata enrichment -- `include_catalog_metadata`:
Coalesce's Catalog product (formerly Castor, still served from
api.castordoc.com with its own `Token` auth, separate from the Scheduler/v1
REST API's Bearer token) exposes a GraphQL API with table/column
descriptions, owners, tags, and column-level lineage
(https://docs.coalesce.io/llms-catalog-api.txt, confirmed 2026-10-08). This
is a materially different product surface -- different base URL, different
auth scheme, keyed by Catalog's own table/column UUIDs rather than Coalesce
nodeIDs (resolved here by exact, case-insensitive name match) -- and not
every Coalesce customer has it provisioned. So it's wired up as strictly
additive, best-effort, and off by default: a failed or unavailable Catalog
call logs a warning and leaves the asset's spec exactly as it would have
been without this feature, it never raises. When enabled, the lookup runs
during `write_state_to_path` (i.e. once, at state-refresh time) so that
`build_defs_from_state`'s own "zero network calls on reload" guarantee
holds for the Catalog calls too, not just the node-discovery call.
"""
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import dagster as dg
import requests
from pydantic import Field

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None
    _HAS_STATE_BACKED = False


# ── Resource ──────────────────────────────────────────────────────────────────

class CoalesceResource(dg.ConfigurableResource):
    """Shared Coalesce connection config.

    Example:
        ```python
        resources = {
            "coalesce": CoalesceResource(
                api_token_env_var="COALESCE_API_TOKEN",
                environment_id="env_abc123",
            )
        }
        ```
    """
    api_token_env_var: str = Field(description="Env var with Coalesce API token")
    environment_id: str = Field(description="Coalesce environment ID")
    api_base_url: str = Field(
        default="https://app.coalescesoftware.io",
        description="Coalesce API base URL",
    )
    catalog_api_token_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Env var with a Coalesce Catalog (Castor) API token. Catalog is a "
            "separately-licensed product; leave unset to skip catalog enrichment."
        ),
    )
    catalog_base_url: str = Field(
        default="https://api.castordoc.com/public/graphql",
        description="Coalesce Catalog GraphQL API base URL (EU default; US tenants use api.us.castordoc.com).",
    )

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {dg.EnvVar(self.api_token_env_var).get_value()}"}

    def list_nodes(self) -> list[dict]:
        url = f"{self.api_base_url}/api/v1/environments/{self.environment_id}/nodes"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data) if isinstance(data, dict) else data

    def start_run(self, node_ids: list[str], parameters: Optional[dict] = None) -> str:
        url = f"{self.api_base_url}/scheduler/startRun"
        payload: dict = {
            "environmentID": self.environment_id,
            "parameterOverride": {"nodeIds": node_ids},
        }
        if parameters:
            payload["parameterOverride"].update(parameters)
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()["runCounter"]

    def get_run_status(self, run_counter: str) -> str:
        url = f"{self.api_base_url}/scheduler/runStatus"
        resp = requests.get(
            url, headers=self._headers(),
            params={"runCounter": run_counter}, timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("runStatus", "")

    def get_run_results(self, run_counter: str) -> list[dict]:
        """GET /api/v1/runs/{runID}/results -- per-node RunResult objects.

        Each item has `nodeID` and an `hasTestFailures` key that's only
        present (and true) when a Node or column test failed on that node.
        """
        url = f"{self.api_base_url}/api/v1/runs/{run_counter}/results"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", data) if isinstance(data, dict) else data

    # ── Catalog (Castor) enrichment -- separate product/auth, best-effort ──

    def _catalog_headers(self) -> Optional[dict]:
        if not self.catalog_api_token_env_var:
            return None
        token = os.environ.get(self.catalog_api_token_env_var)
        if not token:
            return None
        return {"Authorization": f"Token {token}", "Content-Type": "application/json"}

    def _catalog_call(self, op_name: str, query: str, variables: dict) -> Optional[dict]:
        headers = self._catalog_headers()
        if headers is None:
            return None
        resp = requests.post(
            f"{self.catalog_base_url}?op={op_name}",
            headers=headers,
            json={"query": query, "variables": variables},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        if body.get("errors"):
            raise RuntimeError(f"Catalog API {op_name!r} returned errors: {body['errors']}")
        return (body.get("data") or {}).get(op_name)

    def get_catalog_table_metadata(self, table_name: str) -> Optional[dict]:
        """Best-effort Catalog lookup for a table's description/owners/tags.

        `getTables(scope: {nameContains})` is a substring match server-side,
        so results are filtered here to an exact, case-insensitive name
        match (a substring search for "ORDERS" would also return
        "STG_ORDERS"). Returns None if Catalog isn't configured, the call
        fails, or no exact match is found -- callers treat None as "skip
        enrichment for this node", never as an error.

        `owners` prefers each Catalog user's email over their display name:
        Dagster's `AssetSpec.owners` only accepts an email address or a
        `team:`-prefixed team name (confirmed empirically -- it raises
        `DagsterInvalidDefinitionError` on a bare display name like "Jane
        Doe"), so email is the only one of Catalog's owner fields that's
        actually usable there.
        """
        query = """
        query GetTables($scope: GetTablesScope) {
          getTables(scope: $scope) {
            data {
              id
              name
              descriptionMarkdown
              ownerEntities { user { fullName email } }
              tagEntities { tag { label } }
            }
          }
        }
        """
        result = self._catalog_call("getTables", query, {"scope": {"nameContains": table_name}})
        rows = (result or {}).get("data") or []
        for row in rows:
            if (row.get("name") or "").lower() == table_name.lower():
                owners = [
                    (u.get("email") or u.get("fullName"))
                    for e in (row.get("ownerEntities") or [])
                    for u in [e.get("user") or {}]
                    if (u.get("email") or u.get("fullName"))
                ]
                tags = [
                    t.get("label")
                    for e in (row.get("tagEntities") or [])
                    for t in [e.get("tag") or {}]
                    if t.get("label")
                ]
                return {
                    "table_id": row.get("id"),
                    "description": row.get("descriptionMarkdown"),
                    "owners": owners,
                    "tags": tags,
                }
        return None

    def get_catalog_column_lineage(self, table_id: str) -> dict[str, list[str]]:
        """Best-effort column -> [upstream "table.column" names] map.

        Two GraphQL hops: `getColumns(scope: {tableId})` lists this table's
        columns, then for each one `getFieldLineages(scope: {childColumnId})`
        returns parent column IDs, resolved back to names via a second
        `getColumns(scope: {ids})` call (lineage edges are ID-keyed only,
        with no inline name/table fields). Any failure at any hop returns
        {} -- this is enrichment, never load-bearing.
        """
        headers = self._catalog_headers()
        if headers is None or not table_id:
            return {}
        try:
            cols = self._catalog_call(
                "getColumns",
                "query GetColumns($scope: GetColumnsScope) { "
                "getColumns(scope: $scope) { data { id name } } }",
                {"scope": {"tableId": table_id}},
            )
            columns = (cols or {}).get("data") or []
            if not columns:
                return {}

            lineage: dict[str, list[str]] = {}
            for col in columns:
                col_id, col_name = col.get("id"), col.get("name")
                if not col_id or not col_name:
                    continue
                fl = self._catalog_call(
                    "getFieldLineages",
                    "query GetFieldLineages($scope: GetFieldLineagesScope!) { "
                    "getFieldLineages(scope: $scope) { data { parentColumnId } } }",
                    {"scope": {"childColumnId": col_id}},
                )
                parent_ids = [
                    row.get("parentColumnId")
                    for row in (fl or {}).get("data") or []
                    if row.get("parentColumnId")
                ]
                if not parent_ids:
                    continue
                parents = self._catalog_call(
                    "getColumns",
                    "query GetColumns($scope: GetColumnsScope) { "
                    "getColumns(scope: $scope) { data { id name table { name } } } }",
                    {"scope": {"ids": parent_ids}},
                )
                names = [
                    (f"{(p.get('table') or {}).get('name')}.{p['name']}"
                     if (p.get("table") or {}).get("name") else p["name"])
                    for p in (parents or {}).get("data") or []
                    if p.get("name")
                ]
                if names:
                    lineage[col_name] = names
            return lineage
        except Exception:
            return {}


# ── Component ─────────────────────────────────────────────────────────────────

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _valid_dagster_owners(owners: Optional[list]) -> list[str]:
    """Dagster's AssetSpec.owners only accepts an email address or a
    `team:`-prefixed team name -- drop anything else (e.g. Catalog owners
    with no email on file, only a display name) rather than let a real
    AssetSpec construction crash on enrichment data we don't fully control."""
    return [o for o in (owners or []) if o and (o.startswith("team:") or _EMAIL_RE.match(o))]


def _merge_spec(base: dg.AssetSpec, ov: dict) -> dg.AssetSpec:
    """Merge an override dict into a base AssetSpec."""
    extra_deps = [dg.AssetKey.from_user_string(d) for d in ov.get("deps", [])]
    return dg.AssetSpec(
        key=dg.AssetKey.from_user_string(ov["key"]) if "key" in ov else base.key,
        description=ov.get("description", base.description),
        group_name=ov.get("group_name", base.group_name),
        owners=ov.get("owners", base.owners),
        metadata={**(base.metadata or {}), **(ov.get("metadata") or {})},
        tags={**(base.tags or {}), **(ov.get("tags") or {})},
        kinds=set(ov["kinds"]) if "kinds" in ov else base.kinds,
        deps=list(base.deps or []) + extra_deps,
    )


def _apply_node_overrides(
    default_spec: dg.AssetSpec,
    node_name: str,
    overrides: Optional[dict],
) -> list[dg.AssetSpec]:
    """Apply assets_by_node_name overrides. Returns list (usually 1, but >1 if one node → multiple assets)."""
    if not overrides or node_name not in overrides:
        return [default_spec]
    ov = overrides[node_name]
    if isinstance(ov, list):
        return [_merge_spec(default_spec, o) for o in ov]
    return [_merge_spec(default_spec, ov)]


def _enrich_nodes_with_catalog_metadata(nodes: list[dict], resource: CoalesceResource) -> None:
    """Mutates `nodes` in place, attaching a `_catalog` dict per node.

    Best-effort: any per-node failure is logged (via `print` -- this runs
    outside a Dagster execution context, at prepare/state-refresh time) and
    that node is simply left without a `_catalog` key.
    """
    for node in nodes:
        name = node.get("name", node.get("nodeId", node.get("id", "")))
        try:
            meta = resource.get_catalog_table_metadata(name)
        except Exception as e:
            print(f"CoalesceWorkspaceComponent: Catalog lookup failed for node {name!r}: {e}")
            continue
        if meta is None:
            continue
        lineage: dict[str, list[str]] = {}
        if meta.get("table_id"):
            try:
                lineage = resource.get_catalog_column_lineage(meta["table_id"])
            except Exception as e:
                print(f"CoalesceWorkspaceComponent: Catalog column lineage failed for node {name!r}: {e}")
        node["_catalog"] = {**meta, "column_lineage": lineage}


def _build_coalesce_defs(
    nodes: list[dict],
    environment_id: str,
    api_token_env_var: str,
    api_base_url: str,
    asset_name_prefix: Optional[str],
    group_name: Optional[str],
    poll_interval: int,
    timeout: int,
    assets_by_node_name: Optional[dict] = None,
    emit_test_checks: bool = True,
    fail_run_on_test_failure: bool = False,
) -> dg.Definitions:
    """Build Definitions from a list of Coalesce node dicts (no network calls)."""
    from dagster import AssetExecutionContext

    # Build a flat mapping of nodeId → AssetKey for dep resolution
    node_key_map: dict[str, dg.AssetKey] = {}
    for node in nodes:
        node_id = node.get("nodeId", node.get("id", ""))
        name = node.get("name", node_id)
        parts = [asset_name_prefix, name] if asset_name_prefix else [name]
        node_key_map[node_id] = dg.AssetKey([p for p in parts if p])

    specs: list[dg.AssetSpec] = []
    spec_key_to_node_id: dict[tuple, str] = {}  # AssetKey.path tuple → node_id

    for node in nodes:
        node_id = node.get("nodeId", node.get("id", ""))
        name = node.get("name", node_id)
        source_ids = node.get("sourceNodeIds", [])
        dep_keys = [node_key_map[sid] for sid in source_ids if sid in node_key_map]
        catalog: dict[str, Any] = node.get("_catalog") or {}

        metadata: dict[str, Any] = {
            "coalesce/node_id": dg.MetadataValue.text(node_id),
            "coalesce/node_type": dg.MetadataValue.text(node.get("type", "")),
        }
        if catalog.get("tags"):
            metadata["coalesce/catalog_tags"] = dg.MetadataValue.json(catalog["tags"])
        if catalog.get("column_lineage"):
            metadata["coalesce/column_lineage"] = dg.MetadataValue.json(catalog["column_lineage"])

        default_spec = dg.AssetSpec(
            key=node_key_map[node_id],
            description=node.get("description") or catalog.get("description"),
            group_name=group_name or "coalesce",
            deps=dep_keys,
            kinds={"coalesce", "sql"},
            owners=_valid_dagster_owners(catalog.get("owners")) or None,
            metadata=metadata,
        )

        expanded = _apply_node_overrides(default_spec, name, assets_by_node_name)
        for spec in expanded:
            specs.append(spec)
            spec_key_to_node_id[tuple(spec.key.path)] = node_id

    if not specs:
        return dg.Definitions()

    check_specs: list[dg.AssetCheckSpec] = []
    if emit_test_checks:
        for spec in specs:
            check_specs.append(
                dg.AssetCheckSpec(
                    name="coalesce_node_tests",
                    asset=spec.key,
                    description=(
                        "Whether this node's Coalesce Node-level and column-level "
                        "tests passed on its most recent run (Coalesce's "
                        "hasTestFailures flag from GET /api/v1/runs/{runID}/results)."
                    ),
                )
            )

    @dg.multi_asset(specs=specs, check_specs=check_specs or None)
    def coalesce_project(context: AssetExecutionContext):
        import time

        token = os.environ[api_token_env_var]
        headers = {"Authorization": f"Bearer {token}"}

        # Group selected asset keys by node_id (one node may map to multiple assets)
        node_id_to_keys: dict[str, list[dg.AssetKey]] = {}
        for key in context.selected_asset_keys:
            nid = spec_key_to_node_id.get(tuple(key.path))
            if nid:
                node_id_to_keys.setdefault(nid, []).append(key)

        context.log.info(f"Starting Coalesce run for {len(node_id_to_keys)} nodes")

        for node_id, keys in node_id_to_keys.items():
            run_url = f"{api_base_url}/scheduler/startRun"
            run_resp = requests.post(run_url, headers=headers, json={
                "environmentID": environment_id,
                "parameterOverride": {"nodeIds": [node_id]},
            }, timeout=30)
            run_resp.raise_for_status()
            run_counter = run_resp.json()["runCounter"]
            context.log.info(f"Coalesce run started: {run_counter} for node {node_id}")

            elapsed = 0
            status = None
            while elapsed < timeout:
                time.sleep(poll_interval)
                elapsed += poll_interval
                status_resp = requests.get(
                    f"{api_base_url}/scheduler/runStatus",
                    headers=headers,
                    params={"runCounter": run_counter},
                    timeout=30,
                )
                status_resp.raise_for_status()
                status = status_resp.json().get("runStatus", "")
                context.log.info(f"Coalesce run {run_counter} status: {status}")
                # Real terminal values (confirmed via Coalesce's OpenAPI spec and
                # docs): "completed" for success, "failed"/"canceled" (one L)
                # for failure. "cancelled"/"error" are kept as defensive
                # fallbacks in case the legacy Scheduler endpoint's wording
                # ever drifts from the newer v1 REST API's RunStatus enum.
                if status == "completed":
                    break
                if status in ("failed", "canceled", "cancelled", "error"):
                    raise Exception(f"Coalesce run {run_counter} {status}")
            else:
                raise Exception(
                    f"Coalesce run {run_counter} did not reach a terminal status "
                    f"within timeout_seconds={timeout} (last status: {status!r})"
                )

            # Test failures don't change `runStatus` -- Coalesce surfaces them
            # as a separate `hasTestFailures` flag on the run's per-node
            # results, which only the /results endpoint carries.
            node_has_test_failures = False
            if emit_test_checks or fail_run_on_test_failure:
                try:
                    results_resp = requests.get(
                        f"{api_base_url}/api/v1/runs/{run_counter}/results",
                        headers=headers, timeout=30,
                    )
                    results_resp.raise_for_status()
                    results_data = results_resp.json()
                    results_list = results_data.get("data", results_data)
                    node_result = next(
                        (r for r in results_list if r.get("nodeID") == node_id), None
                    )
                    node_has_test_failures = bool(
                        (node_result or {}).get("hasTestFailures", False)
                    )
                except Exception as e:
                    context.log.warning(
                        f"Could not fetch Coalesce run results for node {node_id} "
                        f"(run {run_counter}): {e}. Treating as no test failures."
                    )

            if fail_run_on_test_failure and node_has_test_failures:
                raise dg.Failure(
                    f"Coalesce run {run_counter}: node {node_id} has one or more "
                    f"failing Node/column tests (hasTestFailures=true) and "
                    f"fail_run_on_test_failure=True"
                )

            for key in keys:
                yield dg.MaterializeResult(
                    asset_key=key,
                    metadata={
                        "run_counter": dg.MetadataValue.text(run_counter),
                        "node_id": dg.MetadataValue.text(node_id),
                    },
                )
                if emit_test_checks:
                    yield dg.AssetCheckResult(
                        check_name="coalesce_node_tests",
                        asset_key=key,
                        passed=not node_has_test_failures,
                        metadata={
                            "hasTestFailures": dg.MetadataValue.bool(node_has_test_failures),
                            "run_counter": dg.MetadataValue.text(run_counter),
                        },
                    )

    return dg.Definitions(assets=[coalesce_project])


if _HAS_STATE_BACKED:
    @dataclass
    class CoalesceWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        """Coalesce workspace component — one Dagster asset per Coalesce node.

        Uses StateBackedComponent to cache the node list from the Coalesce API,
        so code-server reloads are fast. Populate the cache with:
          dagster dev   (automatic in dev)
          dg utils refresh-defs-state   (CI/CD/image build)

        Example:
            ```yaml
            type: dagster_community_components.CoalesceWorkspaceComponent
            attributes:
              environment_id: env_abc123
              api_token_env_var: COALESCE_API_TOKEN
            ```
        """

        environment_id: str
        api_token_env_var: str
        api_base_url: str = "https://app.coalescesoftware.io"
        asset_name_prefix: Optional[str] = None
        group_name: Optional[str] = "coalesce"
        poll_interval_seconds: int = 10
        timeout_seconds: int = 1800
        assets_by_node_name: Optional[dict] = None
        emit_test_checks: bool = True
        fail_run_on_test_failure: bool = False
        include_catalog_metadata: bool = False
        catalog_api_token_env_var: Optional[str] = None
        catalog_base_url: str = "https://api.castordoc.com/public/graphql"
        defs_state: ResolvedDefsStateConfig = field(
            default_factory=DefsStateConfigArgs.local_filesystem
        )

        @property
        def defs_state_config(self) -> DefsStateConfig:
            return DefsStateConfig.from_args(
                self.defs_state,
                default_key=f"CoalesceWorkspaceComponent[{self.environment_id}]",
            )

        def write_state_to_path(self, state_path: Path) -> None:
            """Fetch all nodes from Coalesce API and cache to disk.

            When `include_catalog_metadata` is set, also runs the (separate,
            best-effort) Catalog enrichment lookups here -- once, at state
            refresh time -- so `build_defs_from_state` never needs to make
            network calls of any kind on a plain code-server reload.
            """
            token = dg.EnvVar(self.api_token_env_var).get_value()
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{self.api_base_url}/api/v1/environments/{self.environment_id}/nodes"
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            nodes = data.get("data", data) if isinstance(data, dict) else data

            if self.include_catalog_metadata:
                resource = CoalesceResource(
                    api_token_env_var=self.api_token_env_var,
                    environment_id=self.environment_id,
                    api_base_url=self.api_base_url,
                    catalog_api_token_env_var=self.catalog_api_token_env_var,
                    catalog_base_url=self.catalog_base_url,
                )
                _enrich_nodes_with_catalog_metadata(nodes, resource)

            state_path.write_text(json.dumps(nodes))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path]
        ) -> dg.Definitions:
            """Build asset specs from cached node list — no network calls."""
            if state_path is None or not state_path.exists():
                context.log.warning(  # type: ignore
                    "CoalesceWorkspaceComponent: no cached state found. "
                    "Run `dg utils refresh-defs-state` or `dagster dev` to populate."
                ) if hasattr(context, "log") else None
                return dg.Definitions()

            nodes = json.loads(state_path.read_text())
            return _build_coalesce_defs(
                nodes=nodes,
                environment_id=self.environment_id,
                api_token_env_var=self.api_token_env_var,
                api_base_url=self.api_base_url,
                asset_name_prefix=self.asset_name_prefix,
                group_name=self.group_name,
                poll_interval=self.poll_interval_seconds,
                timeout=self.timeout_seconds,
                assets_by_node_name=self.assets_by_node_name,
                emit_test_checks=self.emit_test_checks,
                fail_run_on_test_failure=self.fail_run_on_test_failure,
            )

else:
    # Fallback: StateBackedComponent not available in this dagster version.
    # Falls back to calling the API on every build_defs (original behaviour).
    class CoalesceWorkspaceComponent(dg.Component, dg.Model, dg.Resolvable):  # type: ignore[no-redef]
        """Coalesce workspace component (fallback: no state caching).

        Upgrade to dagster>=1.8 to get StateBackedComponent caching.
        """
        environment_id: str = Field(description="Coalesce environment ID")
        api_token_env_var: str = Field(description="Env var with Coalesce API token")
        api_base_url: str = Field(default="https://app.coalescesoftware.io")
        asset_name_prefix: Optional[str] = Field(default=None)
        group_name: Optional[str] = Field(default="coalesce")
        poll_interval_seconds: int = Field(default=10)
        timeout_seconds: int = Field(default=1800)
        assets_by_node_name: Optional[dict] = Field(default=None)
        emit_test_checks: bool = Field(default=True)
        fail_run_on_test_failure: bool = Field(default=False)
        include_catalog_metadata: bool = Field(default=False)
        catalog_api_token_env_var: Optional[str] = Field(default=None)
        catalog_base_url: str = Field(default="https://api.castordoc.com/public/graphql")

        def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
            token = dg.EnvVar(self.api_token_env_var).get_value()
            headers = {"Authorization": f"Bearer {token}"}
            url = f"{self.api_base_url}/api/v1/environments/{self.environment_id}/nodes"
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            nodes = data.get("data", data) if isinstance(data, dict) else data

            if self.include_catalog_metadata:
                resource = CoalesceResource(
                    api_token_env_var=self.api_token_env_var,
                    environment_id=self.environment_id,
                    api_base_url=self.api_base_url,
                    catalog_api_token_env_var=self.catalog_api_token_env_var,
                    catalog_base_url=self.catalog_base_url,
                )
                _enrich_nodes_with_catalog_metadata(nodes, resource)

            return _build_coalesce_defs(
                nodes=nodes,
                environment_id=self.environment_id,
                api_token_env_var=self.api_token_env_var,
                api_base_url=self.api_base_url,
                asset_name_prefix=self.asset_name_prefix,
                group_name=self.group_name,
                poll_interval=self.poll_interval_seconds,
                timeout=self.timeout_seconds,
                assets_by_node_name=self.assets_by_node_name,
                emit_test_checks=self.emit_test_checks,
                fail_run_on_test_failure=self.fail_run_on_test_failure,
            )
