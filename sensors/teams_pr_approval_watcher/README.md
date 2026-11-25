# teams_pr_approval_watcher

The Microsoft Teams analog of [`slack_pr_approval_watcher`](../slack_pr_approval_watcher) -- watches the Teams channel messages [`github_pull_request_agent_sensor`](../github_pull_request_agent_sensor) (`platform: teams`) posts, and when an allowlisted person reacts:

1. **Records a GitHub review through a SECOND identity** -- GitHub blocks a PR's own author approving it, so recording a human's approve/reject always needs a genuinely different token (`reviewer_token_env_var`) from whatever opened the PR.
2. **Refuses to approve while CI checks are failing** -- a reaction does not override a red build.
3. **Optionally merges**, once the reviewer identity has approved and checks pass, and **only for branches under `branch_prefix`** (default `agent/`) -- a human's own branch is never touched.

## What ships

One sensor. Per tick: scans the Teams channel for still-open PR-review messages (an Adaptive Card attachment carries the correlation metadata -- a plain Teams `chatMessage` has no hidden-metadata field the way Slack's `chat.postMessage` `metadata` param does, confirmed against Graph's own `chatMessage` resource reference, whose property list is closed), and for each:

- If the PR is no longer open (merged or closed elsewhere), marks the message resolved and moves on.
- If the reviewer identity hasn't approved yet, reads the message's reactions (first allowlisted vote wins -- reactions come back inline on the message itself, no separate `reactions.get` call the way Slack needs) and calls the GitHub reviews API under the reviewer identity -- refusing while checks are failing, posting the result back as a thread reply.
- If approved and `auto_merge` is set, squash-merges once checks pass, deletes the branch, and marks the message resolved.

Already-resolved messages and already-shown warnings are tracked in this sensor's own Dagster-native sensor cursor -- not a shared document two sensors could race on rewriting.

## No reaction seeding, no bot-identity exclusion

Unlike the Slack version, this sensor never pre-reacts to its own posted message, and does not try to exclude "the bot's own vote" the way Slack's `bot_user_id` does: a delegated Teams token represents the consenting human behind the app registration, not a distinct bot identity (no equivalent of Slack's `auth.test` self-lookup), so there is no reliable id to exclude. Instead, set `teams_approver_allowlist` on [`github_pull_request_agent_sensor`](../github_pull_request_agent_sensor) (`platform: teams`) to the exact Azure AD object IDs allowed to vote -- the same posture [`teams_approval_gate`](../../assets/ai/teams_approval_gate) already takes.

## Verifying the fix actually worked (`verify_after_merge`)

Merging a PR doesn't prove the fix works -- it proves CI passed, which may not even cover the failure the PR claims to fix. With `verify_after_merge: true`, once `auto_merge` merges a PR, this sensor looks for `Fixes failed run <uuid>` or `Run ID: <uuid>` in the PR body (the convention a fixer asset's PR description should use) and re-executes that exact failed run (Dagster's own `launchRunReexecution`, `ALL_STEPS` strategy) against the just-merged code:

```yaml
auto_merge: true
verify_after_merge: true
dagster_url: "https://my-org.dagster.cloud/prod"
dagster_cloud_api_token_env_var: DAGSTER_CLOUD_API_TOKEN
post_merge_command: "./deploy.sh"   # optional -- run before the re-run, e.g. a redeploy step
```

The message stays open (not marked resolved) until the re-run finishes -- a later tick polls it and posts a pass/fail/timeout reply into the SAME message. Tracked in the cursor (`verifying`), same no-shared-state-file discipline as everything else here. If the PR body never names a failed run, or `dagster_url` isn't set, the message is simply marked resolved with a note instead of silently hanging forever.

## `reviewer_token_env_var` / `reviewer_login`

Set up a second GitHub account or bot with write access to the repo, and export its token under a different env var than the one that opens PRs. `reviewer_login` is that identity's username -- checked before merging, to confirm the SPECIFIC reviewer identity approved (not just anyone). Leave `reviewer_token_env_var` unset and every approve attempt fails loudly with an explanatory message instead of silently only commenting.

## `auto_merge`

Off by default. When on, a merge still requires ALL of: the reviewer identity's approval, passing checks, and the PR's branch starting with `branch_prefix`. Turning this on is a real decision about how much autonomy to grant -- review the branch-prefix convention your `propose_fix`-opened PRs actually use before enabling it.

## Teams auth

Same delegated OAuth2 refresh-token Azure AD app registration as [`teams_agent_request_sensor`](../teams_agent_request_sensor) and [`teams_approval_gate`](../../assets/ai/teams_approval_gate) -- `teams_tenant_id_env_var` / `teams_client_id_env_var` / `teams_client_secret_env_var` / `teams_refresh_token_env_var`, needing delegated `ChannelMessage.Send` + `ChannelMessage.Read.All`.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `repo` | `str` | GitHub repository to act on, 'owner/repo'. Must match the repo sensors/github_pull_request_agent_sensor watches. |
| `teams_team_id` | `str` | The SAME Teams team id sensors/github_pull_request_agent_sensor (platform='teams') posts to. |
| `teams_channel_id` | `str` | The SAME Teams channel id sensors/github_pull_request_agent_sensor (platform='teams') posts to. |

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `token_env_var` | `str` | `"GITHUB_TOKEN"` | Env var holding the primary GitHub token (reads, checks CI). |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `sensor_name` | `str` | `"teams_pr_approval_watcher"` | Unique sensor name. |
| `minimum_interval_seconds` | `int` | `30` | Sensor cadence -- how often to poll Teams + GitHub. |
| `default_status` | `str` | `"RUNNING"` | 'RUNNING' or 'STOPPED'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `reviewer_token_env_var` | `str` | — | Env var holding a SECOND GitHub token (a distinct bot/user identity with write access) -- required to actually record an approval. GitHub blocks a PR's own author approving it. |
| `reviewer_login` | `str` | — | The reviewer (second) identity's GitHub username -- checked before merging, to confirm THAT identity (not just anyone) approved. |
| `api_base_url` | `str` | `"https://api.github.com"` | API base URL. Change for GitHub Enterprise Server. |
| `branch_prefix` | `str` | `"agent/"` | Only branches with this prefix are ever merged automatically. |
| `teams_tenant_id_env_var` | `str` | `"TEAMS_TENANT_ID"` | Env var holding the Azure AD tenant id. |
| `teams_client_id_env_var` | `str` | `"TEAMS_CLIENT_ID"` | Env var holding the Azure AD app registration client id. |
| `teams_client_secret_env_var` | `str` | `"TEAMS_CLIENT_SECRET"` | Env var holding the Azure AD app registration client secret. |
| `teams_refresh_token_env_var` | `str` | `"TEAMS_REFRESH_TOKEN"` | Env var holding the OAuth refresh token. |
| `history_lookback_hours` | `float` | `168.0` | How far back (hours) to scan the channel for still-open PR-review messages. |
| `post_branch_deployment_link` | `bool` | `true` | Post 'A branch deployment of this change is ready to test: <link>' once in a PR's Teams thread, when the PR has a Dagster+ branch deployment link and is NOT low risk (per protected_paths / low_risk_paths / low_risk_max_f… _(full docs in schema.json + component README)_ |
| `protected_paths` | `List[str]` | `lambda: ['.github/', 'pyproject.toml', 'requirements', 'Dockerfile', 'setup.']()` | Risk assessment for post_branch_deployment_link: a PR touching one of these paths is not low risk. Same matching convention and defaults as github_pr_tools' protected_paths. |
| `low_risk_paths` | `List[str]` | `lambda: ['src/', 'tests/', 'docs/', 'README']()` | Risk assessment for post_branch_deployment_link: a PR changing only these paths (within the size limits, checks passing) is low risk and gets no link. Same convention and defaults as github_pr_tools' low_risk_paths. |
| `low_risk_max_files` | `int` | `2` | Risk assessment for post_branch_deployment_link: max changed files for a low-risk PR. |
| `low_risk_max_lines` | `int` | `30` | Risk assessment for post_branch_deployment_link: max changed lines (additions + deletions) for a low-risk PR. |
| `auto_merge` | `bool` | `false` | When True, squash-merge automatically once the reviewer identity has approved and checks pass (still gated by branch_prefix). |
| `refresh_other_agent_prs_after_merge` | `bool` | `true` | After auto_merge successfully merges a PR, bring every OTHER open branch_prefix PR up to date with the new base_branch (GitHub's update-branch API) so their CI runs against the just-merged code instead of a stale base, t… _(full docs in schema.json + component README)_ |
| `skip_in_branch_deployments` | `bool` | `true` | Skip every tick while running inside a Dagster+ branch deployment (DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT=1). A branch deployment runs its own copy of every sensor alongside the full deployment's -- without this, a branch de… _(full docs in schema.json + component README)_ |
| `verify_after_merge` | `bool` | `false` | After auto_merge merges a PR, look for 'Fixes failed run <uuid>' or 'Run ID: <uuid>' in the PR body (the same convention failed_run_agent_sensor's launched fixer asset should put there) and re-execute that failed run (Da… _(full docs in schema.json + component README)_ |
| `post_merge_command` | `str` | — | Only used when verify_after_merge is true. An optional shell command run once, right before launching the verification re-run -- e.g. a deploy/redeploy step so the re-run actually executes against the just-merged code in… _(full docs in schema.json + component README)_ |
| `dagster_url` | `str` | — | Base Dagster(+) URL (e.g. 'https://org.dagster.cloud/prod'). Required when verify_after_merge is true -- both for the GraphQL endpoint (<dagster_url>/graphql) and for building run links in Teams messages. |
| `dagster_cloud_api_token_env_var` | `str` | `"DAGSTER_CLOUD_API_TOKEN"` | Only used when verify_after_merge is true. Env var holding a Dagster+ API token with permission to launch runs in this deployment. |
| `verify_timeout_minutes` | `int` | `30` | Only used when verify_after_merge is true. Give up waiting for the verification re-run after this many minutes and report a timeout instead of a pass/fail. |

[//]: # (FIELDS:END)

## See also

- [`github_pull_request_agent_sensor`](../github_pull_request_agent_sensor) -- posts the messages this sensor watches (`platform: teams`)
- [`teams_approval_gate`](../../assets/ai/teams_approval_gate) -- the same reactions/reply vote-classification logic, for a generic upstream asset instead of a pull request
- [`github_pr_tools`](../../assets/ai/github_pr_tools) -- the same risk-assessment/review/merge logic, available to an agent directly (minus merging, which only this sensor ever does)
