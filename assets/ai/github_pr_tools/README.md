# github_pr_tools

Repo-scoped GitHub pull-request tools for engineering agents (a bug fixer, a PR reviewer), with the review rules enforced in code, not in the prompt -- a model can describe a PR's risk, but the deterministic rules below decide whether it's actually low risk, and `review_pull_request` refuses an APPROVE that disagrees.

| Tool | Answers / does |
|---|---|
| `get_pull_request(number)` | title, body, author, state, changed files + patches, CI check state |
| `list_open_pull_requests()` | number, head sha, branch, title, author, url -- so an agent can avoid proposing a change twice |
| `assess_pull_request(number)` | the FIXED risk assessment (`level`: `'low'`/`'needs_human'`, plus `reasons`), from changed paths, size and checks |
| `propose_fix(branch, title, body, files, note="")` | opens a pull request. `files` items are `{path, old, new}` (replaces one exact piece of text -- it must appear once -- leaving the rest of the file untouched) or `{path, content}` (new files only). Refuses files under a protected path, or a fix touching more than `max_files_per_fix` files, or a file an already-open PR already touches |
| `review_pull_request(number, decision, body)` | `decision` is `COMMENT` or `APPROVE`. `COMMENT` always works. `APPROVE` is refused unless `assess_pull_request` says `'low'`, and is posted under a SECOND GitHub identity -- GitHub blocks a PR's own author approving it |

Deliberately **not** a tool: merging. Only `sensors/slack_pr_approval_watcher` can merge (on a human's Slack reaction, gated by branch prefix + reviewer approval + passing checks) -- an agent is never handed a merge button.

## Why a second identity for APPROVE

GitHub rejects a review with `event: APPROVE` when the reviewer is the PR's own author -- and a single bot token that opens PRs (`propose_fix`) is exactly that author. `reviewer_token_env_var` names a SECOND GitHub token (a distinct user or bot account with write access to the repo) used only for the APPROVE call; `COMMENT` still goes out under the primary identity. Leave `reviewer_token_env_var` unset and APPROVE always fails loudly (`"APPROVE requires a second GitHub identity..."`) rather than silently commenting instead.

## Using from Claude Code (`claude mcp add`)

```bash
export GITHUB_PR_TOOLS_REPO=your-org/your-repo
export GITHUB_TOKEN=ghp_...
claude mcp add github-pr-tools -- python -m dagster_community_components.assets.ai.github_pr_tools.component
```

See `main()`'s docstring in `component.py` for the full list of `GITHUB_PR_TOOLS_*` env vars the standalone server reads.

## As a Dagster resource

```yaml
type: dagster_community_components.GithubPrToolsComponent
attributes:
  repo: your-org/your-repo
  token_env_var: GITHUB_TOKEN
  reviewer_token_env_var: GITHUB_REVIEWER_TOKEN
```
`context.resources.github_pr_tools.call("assess_pull_request", {"number": 42})`.

## Wiring it into an agent

Same dual-mode shape as `agentic_pipeline_authoring_tools`/`dagster_instance_tools`/`dagster_workflow_tools`: `component.py` is itself a stdio MCP server (`pip install mcp`; verified with mcp 1.30.0 and 2.3.0).

```yaml
- id: review_pr
  op: tool_use_loop
  model: gpt-4o-mini
  api_key_env_var: OPENAI_API_KEY
  prompt_template: "Review PR #{pr_number}. Call assess_pull_request first; only APPROVE if it says low risk."
  mcp_servers:
    - name: github
      type: stdio
      command: [python, -m, dagster_community_components.assets.ai.github_pr_tools.component]
      env: {GITHUB_PR_TOOLS_REPO: your-org/your-repo}
```

Tools are namespaced by the loop as `github__assess_pull_request`, etc.

## Referencing it by key (`mcp_server_resource_key`)

Same convention as the other dual-mode tool components -- set `mcp_server_resource_key` to register a second, [`MCPServerResource`](../../../resources/mcp_server_resource)-shaped resource under that key, built from this component's own settings, with both token env vars passed through live (the actual secret value is read fresh from this process's environment each time a consumer resolves the resource -- never baked into the spawned command).

```yaml
type: dagster_community_components.GithubPrToolsComponent
attributes:
  repo: your-org/your-repo
  mcp_server_resource_key: github_pr_tools_mcp
```
Then, in any MCP-aware component: `server_resource_key: github_pr_tools_mcp`.

## Risk rules (`assess_pull_request` / enforced by `review_pull_request`)

A PR is `'low'` risk only if ALL of: it changes at most `low_risk_max_files` files (default 2), at most `low_risk_max_lines` total additions+deletions (default 30), every changed path starts with one of `low_risk_paths` (default `src/`, `tests/`, `docs/`, `README`), none of it touches a `protected_paths` prefix (default `.github/`, `pyproject.toml`, `requirements`, `Dockerfile`, `setup.`), and CI checks are passing. Anything else is `'needs_human'` -- a person decides, `review_pull_request` refuses APPROVE, and only `COMMENT` is possible.

## Safety

Every GitHub call is pinned to the configured `repo` -- a prompt cannot point the tools at another repository. `propose_fix` never touches a protected path and never changes more files than `max_files_per_fix`. `review_pull_request` never approves above the configured risk limits, no matter what the model argues. Failures come back as `{"error": "..."}`, never an exception.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `repo` | `str` | GitHub repository the tools are scoped to, 'owner/repo'. Every call is pinned to this repo. |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"github_pr_tools"` | Resource key the toolset is registered under. |
| `token_env_var` | `str` | `"GITHUB_TOKEN"` | Env var holding the primary GitHub token (reads, COMMENT reviews, propose_fix, merge). |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `reviewer_token_env_var` | `str` | — | Env var holding a SECOND GitHub token (a distinct bot/user identity with write access) -- required for an APPROVE review. GitHub blocks a PR's own author (or a single bot identity that opened it) from approving it, so AP… _(full docs in schema.json + component README)_ |
| `api_base_url` | `str` | `"https://api.github.com"` | API base URL. Change for GitHub Enterprise Server. |
| `base_branch` | `str` | `"main"` | Base branch for propose_fix's pull requests. |
| `branch_prefix` | `str` | `"agent/"` | Prefix propose_fix uses for the branches it creates. |
| `protected_paths` | `List[str]` | `lambda: ['.github/', 'pyproject.toml', 'requirements', 'Dockerfile', 'setup.']()` | Path prefixes propose_fix refuses to touch (CI, dependencies, packaging). |
| `low_risk_paths` | `List[str]` | `lambda: ['src/', 'tests/', 'docs/', 'README']()` | Path prefixes assess_pull_request/review_pull_request treat as low-risk when nothing else disqualifies the PR. |
| `low_risk_max_files` | `int` | `2` | assess_pull_request: max changed files for a 'low' risk level. |
| `low_risk_max_lines` | `int` | `30` | assess_pull_request: max changed lines (additions + deletions) for a 'low' risk level. |
| `max_files_per_fix` | `int` | `5` | propose_fix: max files a single fix may change. |
| `allow` | `List[str]` | — | `f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}."` |
| `max_result_chars` | `int` | `20000` | Upper bound on the serialized size of any tool result. |
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this module, with GITHUB_PR_TOOLS_* env vars set from this component's own settings, and the real token(s) passed through via live env p… _(full docs in schema.json + component README)_ |
| `mcp_extra_env_vars` | `List[str]` | — | mcp_server_resource_key only. Extra env var NAMES (beyond the token env vars, which are always passed through) to copy into the spawned MCP server's env. |

[//]: # (FIELDS:END)
