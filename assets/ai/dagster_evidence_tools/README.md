# `DagsterEvidenceToolsComponent`

Read-only deployment-HEALTH evidence tools for an "improvement advisor" agent (one that reviews a Dagster
deployment and suggests fixes), over the **Dagster+ GraphQL API**, **GitHub**, **PyPI** and **OSV.dev**.
Ported from `ops_tools.py` in an internal engineering-agents reference sandbox -- the ~2,000 lines of
hand-written Python a real production deployment runs three agents from today -- through this repo's own
conventions.

## Why this exists

[`dagster_discovery_tools`](../dagster_discovery_tools) answers "what exists in this deployment". This
component answers "is it healthy, and what changed" -- the evidence an improvement-advisor agent actually
needs before proposing a fix, not a guess at what might be wrong.

| Tool | What it answers |
|---|---|
| `project_overview(location)` | Everything about ONE code location -- see below |
| `deprecation_report(max_files?)` | Deprecated/superseded/removed Dagster API usage across the repo's Python files |
| `latest_dagster_release()` | The newest published `dagster` version + its GitHub release notes |

### `project_overview(location)`

| Section | Source |
|---|---|
| `assets[]` | `assetKey`, `group`, `freshness_policy`, `automation_condition`, `scheduled_by` (cross-referenced against this location's schedules), `code_references` (the repository file(s) that define it, via `CodeReferencesMetadataEntry`) |
| `schedules[]` | Every schedule in this location (name, cron, job, status) |
| `running_dagster_version` / `latest_dagster_version` / `dagster_dependency` | What's running (`dagsterLibraryVersions`), the newest release (PyPI), and how `dagster` is pinned in `pyproject.toml` |
| `retries` | Steps that retried in the last `retry_window_days` (default 7): how often, the deepest error message, and whether the run ultimately succeeded (`final_run_status`) |
| `partition_and_check_health` | Partitioned assets with failed/missing partitions; asset-check pass/fail/stale counts across the location; which assets have failing checks |
| `runtime_changes` | Runs of the same asset selection that got meaningfully slower in the last `runtime_change_window_days` (default 14): the recent half's median duration against the earlier half's |
| `dependencies` | Declared dependencies from `pyproject.toml` (pinned/range/unconstrained), lock files, and OSV.dev vulnerabilities (with a fixed-in version when OSV has one) for the ones this tool can confirm are actually running |
| `incidents` | Runs that failed in the last `incident_window_hours` (default 24): job, assets, the failing step, and its error |
| `repo_notes` | Passed through from this component's own config, verbatim -- see below |

Refuses (no HTTP calls at all) when `location` is in `exclude_locations` -- so an improvement-advisor agent
can never review, and so never reacts to, its own code location.

## Telling the agent what's deliberate (`repo_notes`)

Same idea as the `context_notes` the `*_agent` components take (see
[`litellm_agent/README.md`](../litellm_agent/README.md#telling-the-agent-whats-deliberate-context_notes)):
free text, passed through `project_overview`'s output under `repo_notes`, for a demo's deliberate
instability, a known limitation that isn't a bug, or anything else worth saying explicitly so the advisor
doesn't file it as a finding.

```yaml
repo_notes: |
  This is a demo repository. The flakiness in retry_handler.py is deliberate (it's the point of the demo)
  -- do not propose fixing it.
```

## Honesty about what's verified vs. best-effort

- `assetNodes` (with `freshnessPolicy`, `automationCondition`, `hasAssetChecks`, `partitionStats`,
  `assetChecksOrError { ... on AssetChecks { checks { executionForLatestMaterialization { status
  timestamp } } } }`, `metadataEntries { ... on CodeReferencesMetadataEntry { codeReferences { ... on
  LocalFileCodeReference { filePath lineNumber } ... on UrlCodeReference { url } } } }`), `workspaceOrError
  { ... on Workspace { locationEntries { locationOrLoadError { ... on RepositoryLocation {
  dagsterLibraryVersions, repositories { schedules } } } } } }`, `runsOrError(limit)` with
  `Run.repositoryOrigin.repositoryLocationName`, and `logsForRun` with `ExecutionStepUpForRetryEvent`/
  `ExecutionStepFailureEvent` are confirmed **real, OSS GraphQL fields** -- verified directly against the
  installed `dagster_graphql` package's own schema classes while building this component (not merely
  assumed from the reference code, and not guessed). Every query is **also validated against a subset of the real
  Dagster+ schema** (`tests/fixtures/dagster_plus_schema_subset.graphql`, generated from dagster-cloud's own
  `schema.graphql`), because Dagster+ is stricter than OSS: it rejects `... on PythonError` under
  `assetChecksOrError` (an earlier version sent it and every `project_overview` call got a Dagster+ HTTP 400).
  The Dagster+-only `alertPoliciesForCodeLocation` is checked the same way. Run against a live Dagster+
  deployment: `project_overview` on two real locations (checked by the Hooli team, asset counts matched).
- `RunsFilter` has **no** `repositoryOrigin` filter argument in the OSS schema (confirmed absent from
  `GrapheneRunsFilter`) -- so, like the reference code, this component fetches `runsOrError(limit)`
  **unfiltered** and filters client-side on each run's own `repositoryOrigin.repositoryLocationName`,
  rather than relying on an unconfirmed filter extension.
- PyPI's `https://pypi.org/pypi/dagster/json` and OSV.dev's `POST https://api.osv.dev/v1/query`
  (`{"package": {"name", "ecosystem": "PyPI"}, "version"}` -> `{"vulns": [{"id", "affected": [{"ranges":
  [{"events": [{"introduced"|"fixed": "..."}]}]}]}]}`) were both confirmed live against the real APIs while
  building this component. The reference code's own `dependency_report` only surfaces vulnerability ids --
  this component additionally extracts the `fixed` event's version, since OSV's real response already
  carries it.
- **Not implemented**: a historical, over-time diff of installed package versions ("package version changes
  in the last N days"). Neither the reference code nor Dagster+'s GraphQL schema exposes a historical
  snapshot of `dagsterLibraryVersions` to diff against. `project_overview` instead reports the *current*
  running version per declared dependency alongside the latest published `dagster` release -- the one
  time-comparison that's actually possible.
- `dependencies.running_packages_with_known_vulnerabilities` only covers **dagster-family** packages.
  `dagsterLibraryVersions` (the only source this tool has for "what version is actually running") covers
  `dagster`/`dagster-aws`/etc., not arbitrary third-party packages like `pandas` or `boto3` -- Dagster+'s
  GraphQL schema does not expose the full installed site-packages list. A declared dependency this tool
  can't confirm is running gets `running_version: null`, never a guessed version.
- "Incidents in the last 24h" is `runsOrError(status=FAILURE)` in the window + the failing step's error --
  exactly what the reference code's own `incidents_last_24h` does. It is **not** alert-policy-fired events.
  The alert policies that apply to the location are reported separately as `alert_policies` /
  `alert_policy_count` (`alertPoliciesForCodeLocation`; if the token can't read them the overview still
  returns, with `alert_policies: null` and an `alert_policies_error`).
- `deprecation_report` needs `repo` configured (reads Python files from GitHub via `repo`/`base_branch`,
  the same auth/traversal-safety conventions [`github_pr_tools`](../github_pr_tools) already established in
  this repo) and scans them with `dagster._annotations.{is_deprecated,get_deprecated_info,is_superseded,
  get_superseded_info,has_deprecated_params,get_deprecated_params,get_deprecated_param_info}` -- all
  confirmed present on the installed `dagster` package. Without `repo`, it refuses with a clear error
  rather than silently returning nothing; `project_overview`'s dependency section degrades the same way.

## Auth

- Dagster+: same mechanism [`dagster_discovery_tools`](../dagster_discovery_tools)/
  `insights_custom_metrics_sync` already use -- `deployment_url: auto` (default) derives
  `https://<org>[.<region>].dagster.cloud/<deployment>` from `DAGSTER_CLOUD_DEPLOYMENT_NAME` + the
  instance's Dagster+ organization (plain-resource path only; set it explicitly for
  `mcp_server_resource_key`). `token_env_var` (default `DAGSTER_CLOUD_API_TOKEN`) is sent as the
  `Dagster-Cloud-Api-Token` header.
- GitHub: `github_token_env_var` (default `GITHUB_TOKEN`) is sent as a Bearer token, same as
  `github_pr_tools`'s `token_env_var`.
- PyPI/OSV.dev: unauthenticated public APIs, no token needed.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"dagster_evidence_tools"` | Resource key the toolset is registered under. |
| `token_env_var` | `str` | `"DAGSTER_CLOUD_API_TOKEN"` | Env var holding a Dagster+ API token with read access to this deployment. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout_seconds` | `float` | `30.0` | HTTP timeout per GraphQL call. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_window_days` | `int` | `7` | project_overview's retried-steps lookback window, in days. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `deployment_url` | `str` | `_AUTO` | Full Dagster+ deployment URL, e.g. https://acme.dagster.cloud/prod. 'auto' derives it at run time from DAGSTER_CLOUD_DEPLOYMENT_NAME + the instance's Dagster+ organization -- resolvable for the plain resource (it sees th… _(full docs in schema.json + component README)_ |
| `repo` | `str` | — | GitHub repository 'owner/repo' this deployment's code lives in -- required for deprecation_report and for project_overview's dependency/package-risk scan (pyproject.toml, lock files). Without it, deprecation_report refus… _(full docs in schema.json + component README)_ |
| `github_token_env_var` | `str` | `"GITHUB_TOKEN"` | Env var holding a GitHub token with read access to `repo`. |
| `github_api_base_url` | `str` | `"https://api.github.com"` | GitHub API base URL. Change for GitHub Enterprise Server. |
| `base_branch` | `str` | `"main"` | Branch deprecation_report/the dependency scan read `repo` from. |
| `exclude_paths` | `List[str]` | — | Path prefixes deprecation_report's file scan and the dependency scan never read -- e.g. vendored code or generated files. Same directory-like ('src/')/filename-like ('Dockerfile') matching convention as github_pr_tools'… _(full docs in schema.json + component README)_ |
| `exclude_locations` | `List[str]` | — | Code location names project_overview refuses to review -- typically the improvement-advisor agent's OWN code location, so it never reviews or reacts to itself. Same name/convention as dagster_discovery_tools' own exclude_locations. |
| `repo_notes` | `str` | — | Free text passed through project_overview's output verbatim, under `repo_notes` -- for what's deliberate in this deployment (a demo's artificial flakiness, a known limitation that isn't a bug). Same idea as the *_agent c… _(full docs in schema.json + component README)_ |
| `allow` | `List[str]` | — | `f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}."` |
| `max_result_chars` | `int` | `12000` | Upper bound on the serialized size of any tool result. |
| `runtime_change_window_days` | `int` | `14` | project_overview's slower-than-before-runs lookback window, in days. |
| `incident_window_hours` | `int` | `24` | project_overview's recent-incidents lookback window, in hours. |
| `max_runs_scanned` | `int` | `100` | Upper bound on runs fetched per project_overview call (retries/incidents/runtime-changes all derive from this one fetch). |
| `max_files_scanned` | `int` | `40` | Default cap on .py files deprecation_report scans (override per call with max_files). |
| `osv_max_packages` | `int` | `25` | Upper bound on packages checked against OSV.dev per project_overview call. |
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this module, with DAGSTER_EVIDENCE_TOOLS_* env vars set from this component's own config). Any mcp_servers-aware component can then refe… _(full docs in schema.json + component README)_ |
| `mcp_extra_env_vars` | `List[str]` | — | mcp_server_resource_key only. Extra env var NAMES to copy into the spawned MCP server's env. |

[//]: # (FIELDS:END)

## MCP server

Any `mcp_servers`-aware component (`mcp_tool_call`, `mcp_tool_picker`, the agent components, ...) can
reference this toolset via `server_resource_key` once `mcp_server_resource_key` is set:

```yaml
type: dagster_community_components.DagsterEvidenceToolsComponent
attributes:
  deployment_url: https://acme.dagster.cloud/prod   # required (not auto) when mcp_server_resource_key is set
  repo: acme/hooli-data-eng-pipelines
  mcp_server_resource_key: evidence_tools_mcp
```

Or run it standalone for a local agent / Claude Code:
`python -m dagster_community_components.assets.ai.dagster_evidence_tools.component`, with
`DAGSTER_EVIDENCE_TOOLS_DEPLOYMENT_URL` and `DAGSTER_EVIDENCE_TOOLS_TOKEN_ENV_VAR` (and the env var it
names) set in the environment.

## See also

- [`dagster_discovery_tools`](../dagster_discovery_tools) -- "what exists in this deployment" (code
  locations, assets, UI-created components, agents, MCP servers, schedules/sensors, runs, alert policies);
  this component's health-and-change complement
- [`dagster_instance_tools`](../dagster_instance_tools) -- read-only tools over the LOCAL instance
- [`github_pr_tools`](../github_pr_tools) -- the GitHub auth/traversal-safety conventions this component
  reuses for `deprecation_report` and the dependency scan
- [`litellm_agent`](../litellm_agent) -- `context_notes`, the same idea as this component's `repo_notes`
