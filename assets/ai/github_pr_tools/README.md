# github_pr_tools

Repo-scoped GitHub pull-request tools for engineering agents (a bug fixer, a PR reviewer), with the review rules enforced in code, not in the prompt -- a model can describe a PR's risk, but the deterministic rules below decide whether it's actually low risk, and `review_pull_request` refuses an APPROVE that disagrees.

| Tool | Answers / does |
|---|---|
| `get_pull_request(number)` | title, body, author, state, changed files + patches, CI check state |
| `list_open_pull_requests()` | number, head sha, branch, title, author, url -- so an agent can avoid proposing a change twice |
| `assess_pull_request(number)` | the FIXED risk assessment (`level`: `'low'`/`'needs_human'`, plus `reasons`), from changed paths, size and checks |
| `propose_fix(branch, title, body, files, note="")` | opens a pull request. `files` items are `{path, old, new}` (replaces one exact piece of text -- it must appear once -- leaving the rest of the file untouched) or `{path, content}` (new files only). Refuses files under a protected path, or a fix touching more than `max_files_per_fix` files, or a file an already-open PR already touches |
| `review_pull_request(number, decision, body)` | `decision` is `COMMENT` or `APPROVE`. `COMMENT` always works. `APPROVE` is refused unless `assess_pull_request` says `'low'`, and is posted under a SECOND GitHub identity -- GitHub blocks a PR's own author approving it |
| `refresh_open_agent_prs()` | brings every open agent-branch PR up to date with `base_branch` -- call after a merge so CI on the other open agent PRs runs against the just-merged code |
| `find_issues(query)` | open issues the agents already filed whose title matches `query`'s main words (generic project words dropped first) -- a duplicate-check, call before `open_issue` |
| `open_issue(title, body, labels=None)` | files a GitHub issue labeled `engineering-agent` |

`get_pull_request`'s result also includes `branch_deployment_url` -- the Dagster+ branch deployment link, read from the PR's comments/checks/statuses (see below).

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

A PR is `'low'` risk only if ALL of: it changes at most `low_risk_max_files` files (default 2), at most `low_risk_max_lines` total additions+deletions (default 30), every changed path starts with one of `low_risk_paths` (default `src/`, `tests/`, `docs/`, `README`), none of it touches a `protected_paths` prefix (default `.github/`, `pyproject.toml`, `requirements`, `Dockerfile`, `setup.`), CI checks are passing, and the PR's own body doesn't contain `Kind: improvement` (an improvement proposal changes behavior, not just fixes something, so it always needs a person regardless of size). Anything else is `'needs_human'` -- a person decides, `review_pull_request` refuses APPROVE, and only `COMMENT` is possible.

## `branch_deployment_url`

`get_pull_request` includes `branch_deployment_url`: the Dagster+ branch deployment link, read from the PR's own comments, check runs, and commit statuses (wherever the Dagster Cloud GitHub Action posts it) -- `null` when nothing was found. No separate "post the link" tool: an agent that sees it in `get_pull_request`'s result can already include it in its own `review_pull_request` COMMENT for a person to click.

## `refresh_open_agent_prs`

Brings every open agent-branch PR (one whose head ref starts with `branch_prefix`) up to date with `base_branch` via GitHub's update-branch API -- call this after a merge so CI on the OTHER open agent PRs runs against the just-merged code instead of a stale base. Never touches a human's own branch.

## Issue filing with a duplicate-check

`find_issues(query)` searches open `engineering-agent`-labeled issues for `query`'s main words (5+
character tokens, a handful of generic project words like `dagster`/`asset`/`improve`/`reliability`
dropped first) via GitHub's search API -- call it before `open_issue` so the same problem isn't
filed twice. `open_issue` labels every issue it creates `engineering-agent` (creating the label on
first use if it doesn't exist) and appends a `Filed by an engineering agent.` footer. Also governed
by `practice_mode_env_var`.

## Practice mode (`practice_mode_env_var`)

Set `practice_mode_env_var` to the NAME of an env var (not a value templated into this field) that your deployment can scope per-environment -- e.g. set it in a Dagster+ **branch deployment** and leave it unset in the full deployment, so the same component YAML practices in a PR's own branch deployment and acts for real once merged. When that named env var holds anything other than `''`/`'0'`/`'false'`/`'no'`/`'off'` (case-insensitive -- same vocabulary as the engineering-agents reference's `settings.shadow()`), `propose_fix` runs every validation (protected paths, duplicate-PR-file check, file count) but returns `{"practice_mode": true, "would_open": "pull_request", "title": ..., "files": [...]}` instead of actually creating a branch/commit/PR.

The env var is read **fresh, by name, inside the tool call** -- not templated into a typed component field at YAML-resolve time. That matters: the engineering-agents reference hit a real bug where an env value that looked numeric (e.g. `"1"`) got stored as the Python `int` `1` by a typed field, which broke the whole code location's load. Reading it as a plain string at call time avoids that failure mode entirely.

## Restricting review scope to agent branches (`restrict_to_branch_prefix`)

This toolset works as a general-purpose PR reviewer by default -- `get_pull_request`/`list_open_pull_requests`/`assess_pull_request`/`review_pull_request` operate on any open PR in `repo`, human-authored or not. That's intentional for a standalone "review every PR" agent, but it's the wrong default when this toolset is paired with `sensors/github_pull_request_agent_sensor` (which already only ever launches a review run for `branch_prefix` PRs): a manually-invoked chat agent, or a hallucinated PR number, could still have this toolset read, comment on, or approve someone else's PR.

Set `restrict_to_branch_prefix: true` to close that gap -- every one of those four tools then refuses (`GithubPrScopeError`) a PR whose head branch doesn't start with `branch_prefix`, matching the branch-scoping `propose_fix`/`refresh_open_agent_prs`/`sensors/slack_pr_approval_watcher`'s merge already enforce unconditionally.

## Safety

Every GitHub call is pinned to the configured `repo` -- a prompt cannot point the tools at another repository. `propose_fix` never touches a protected path and never changes more files than `max_files_per_fix`. `review_pull_request` never approves above the configured risk limits, no matter what the model argues. Issue filing and PR creation are deliberately separate GitHub label-and-search mechanisms -- `find_issues`/`open_issue` only ever read or create issues, never pull requests. Failures come back as `{"error": "..."}`, never an exception.

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
| `restrict_to_branch_prefix` | `bool` | `false` | When true, get_pull_request/list_open_pull_requests/assess_pull_request/review_pull_request all refuse (GithubPrScopeError) to operate on a PR whose head branch doesn't start with branch_prefix -- never a human's or a bo… _(full docs in schema.json + component README)_ |
| `protected_paths` | `List[str]` | `lambda: ['.github/', 'pyproject.toml', 'requirements', 'Dockerfile', 'setup.']()` | Path prefixes propose_fix refuses to touch (CI, dependencies, packaging). |
| `low_risk_paths` | `List[str]` | `lambda: ['src/', 'tests/', 'docs/', 'README']()` | Path prefixes assess_pull_request/review_pull_request treat as low-risk when nothing else disqualifies the PR. |
| `low_risk_max_files` | `int` | `2` | assess_pull_request: max changed files for a 'low' risk level. |
| `low_risk_max_lines` | `int` | `30` | assess_pull_request: max changed lines (additions + deletions) for a 'low' risk level. |
| `max_files_per_fix` | `int` | `5` | propose_fix: max files a single fix may change. |
| `practice_mode_env_var` | `str` | — | Env var NAME (not a value templated into this field) whose value, read fresh at call time, switches propose_fix into practice mode: every validation (protected paths, duplicate-PR-file check, file count) still runs, but… _(full docs in schema.json + component README)_ |
| `ledger_path` | `str` | — | Directory this tool writes one JSON-lines file per Dagster run_id into, recording every consequential action (propose_fix, open_issue, an APPROVE review) as it happens -- filed, would_file (practice mode), refused (a gua… _(full docs in schema.json + component README)_ |
| `allow` | `List[str]` | — | `f"Hard allow-list of tools (default: all). Valid: {', '.join(ALL_TOOLS)}."` |
| `max_result_chars` | `int` | `20000` | Upper bound on the serialized size of any tool result. |
| `mcp_server_resource_key` | `str` | — | If set, ALSO registers a stdio MCP-server-shaped resource under this key (python -m this module, with GITHUB_PR_TOOLS_* env vars set from this component's own settings, and the real token(s) passed through via live env p… _(full docs in schema.json + component README)_ |
| `mcp_extra_env_vars` | `List[str]` | — | mcp_server_resource_key only. Extra env var NAMES (beyond the token env vars, which are always passed through) to copy into the spawned MCP server's env. |

[//]: # (FIELDS:END)
