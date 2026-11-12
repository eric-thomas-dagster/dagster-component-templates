# Skill Resource

Register a skill -- a `SKILL.md` file following Anthropic's Agent Skills convention
(YAML front matter with `name`/`description`, plus a markdown body of instructions) --
once, and reference it by name (`skill_keys`) from the MCP-aware agent components
(`litellm_agent`, `openai_agent`, `anthropic_agent`, `gemini_agent`,
`vercel_ai_gateway_agent`, `snowflake_cortex_agent`, `agentic_pipeline`) instead of
repeating the same source fields inline in every one of them.

## Why

This mirrors `resources/mcp_server_resource`'s own reasoning, applied to skills instead
of MCP servers: a project with several agents that should all have access to the SAME
skill had no shared place to register it, and a UI wanting to show "what skills does
this project have" had nowhere to look except inside each `SKILL.md` file itself.

```yaml
type: dagster_community_components.SkillResourceComponent
attributes:
  resource_key: pdf_report_skill
  git_url: https://github.com/your-org/agent-skills.git
  git_path: skills/pdf-report
  git_ref: main
```

Then, in any skill-aware agent component:

```yaml
skill_keys:
  - pdf_report_skill
```

The listed skill's `name`/`description` go into the agent's context up front (cheap to
show); the full `SKILL.md` body is only fetched when the agent actually chooses to use
it -- the same progressive-disclosure shape Claude Code's own Skill tool uses. Each
skill-aware agent component tags its own asset(s) `dagster-community/skill` (value: the
`skill_keys` in use) -- this resource itself has no assets of its own to tag (same shape
as `MCPServerResourceComponent`), so that tag is stamped by the consuming agent, the same
way `dagster-community/mcp-server` is stamped by `mcp_tool_call` rather than by
`MCPServerResourceComponent`.

## Source: git, local, or a download URL

Exactly one of `git_url`, `local_path`, or `url` is required -- setting more than one,
or none, raises a clear error at component load:

- **`git_url`** (+ optional `git_path`, `git_ref`, `git_token_env_var`): shallow-clones
  the repo (`git clone --depth 1 --branch <git_ref> --single-branch`) and reads
  `<git_path>/SKILL.md` from it (or, with `mode="collection"`, treats `<git_path>` as a
  directory of skill subfolders -- see below). Requires a `git` binary on `PATH`.
  `git_ref` must be a branch or tag name -- shallow-cloning an arbitrary commit SHA isn't
  guaranteed to work (depends on whether the remote's server allows fetching by SHA; most
  don't). `git_token_env_var` names an env var holding a token for a private repo, sent as
  an `Authorization: Bearer <token>` header during clone (`-c http.extraHeader=...`) --
  works for GitHub/GitLab/Bitbucket/Forgejo HTTPS remotes alike; not applicable to
  `ssh://` URLs (use your normal SSH agent/known_hosts setup for those instead).
- **`local_path`**: reads `<local_path>/SKILL.md` directly off the local filesystem (or,
  with `mode="collection"`, treats it as a directory of skill subfolders).
- **`url`** (+ optional `url_headers_env`) -- `mode="single"` only: downloads either a
  raw `SKILL.md` file or a `.zip` archive, for skills distributed by a marketplace
  (e.g. mcpmarket) that doesn't expose a git source. The content is sniffed from the
  response body itself (zip magic bytes), not just the URL's extension. A zip that wraps
  its contents in one top-level folder (common for a single-skill export) is handled --
  if `SKILL.md` isn't at the zip root but is exactly one directory down, that directory
  is treated as the real skill root. `url_headers_env` names an env var holding a JSON
  object of HTTP headers (e.g. `'{"Authorization": "Bearer ..."}'`) for authenticated
  downloads.

## `mode="collection"`: register every skill in a repo/directory at once

Point `git_path` (under `git_url`) or `local_path` at a directory that **contains
multiple skill subfolders** -- e.g. a clone of
[`anthropics/skills`](https://github.com/anthropics/skills), which has ~19 skills as
top-level folders -- and every immediate subfolder with a `SKILL.md` is registered as its
own resource, keyed by its folder name:

```yaml
type: dagster_community_components.SkillResourceComponent
attributes:
  mode: collection
  git_url: https://github.com/anthropics/skills.git
  git_ref: main
```

This registers one resource per skill (`pdf`, `docx`, `xlsx`, `pptx`, ...), so
`skill_keys: [pdf]` works directly in any skill-aware agent component -- without a
separate `SkillResourceComponent` instance (and a separate line in the Resources UI) per
skill. `include`/`exclude` filter by folder name (`exclude` applied after `include`); a
subfolder with no `SKILL.md` at all is silently skipped as "not a skill folder" either
way. `name`/`description`/`url` aren't per-folder, so they're rejected with
`mode="collection"` (each folder's own front matter is the only source for those).  A
folder that DOES have a `SKILL.md` but no `description` is still a real configuration
problem and fails loudly, naming that folder.

## Caching: fetch at component load, not per run

A git checkout is cloned once per `(git_url, git_ref)` pair (shared across every skill in
a collection -- not re-cloned per skill), and a `url` download is fetched once per `url`
-- both cached under the system temp dir and reused by every later component load or run
that asks for the same source. Neither is re-fetched on every code-location reload, and
never re-fetched per run. `load()` (called by an agent once it actually chooses the
skill) just reads from the already-fetched directory on disk.

## Name/description resolution -- fails loudly, not silently

`name`/`description` are read from the skill's own `SKILL.md` front matter at component
load time if left unset -- so the resource's resolved config (visible in Dagster's own
Resources UI) already shows what the skill is, without anyone needing to open the file.

Unlike components that tolerate a transient/unreachable external resource (an API, an
MCP server), a missing `SKILL.md` or a missing `description` (neither in the front
matter nor given explicitly) is treated as a **configuration error**: `build_defs` raises
immediately, naming the exact path that was checked (the local path, or the git
url/ref/path for a git source), rather than silently falling back to a blank resource
a UI would otherwise show as present but empty.

## describe() / load() / read_file()

The registered `SkillResource` exposes:

- **`describe() -> {key, name, description}`** -- cheap, no I/O. This is the part an
  agent sees up front, before choosing whether to use the skill.
- **`load() -> {content, files}`** -- `content` is the full `SKILL.md` text; `files` is
  every other file bundled alongside it in the skill folder (relative paths, recursive).
  Only called once an agent has actually chosen the skill -- never inlined into every
  prompt up front (progressive disclosure, the same shape Claude Code's own Skill tool
  uses).
- **`read_file(relative_path) -> str`** -- reads one bundled file by its path relative
  to the skill folder. Rejects any path that resolves outside the folder (e.g.
  `../../etc/passwd` or an absolute path).

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `"skill"` | mode='single' only. Dagster resource key other components reference via skill_keys. |
| `url` | `str` | — | mode='single' only. URL to download the skill from -- either a raw SKILL.md file, or a .zip archive containing a skill folder (SKILL.md + any bundled files). Exactly one of git_url / local_path / url is required. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `description` | `str` | — | mode='single' only. Skill description override. If unset, read from the SKILL.md front matter at component load time. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `mode` | `Literal['single', 'collection']` | `"single"` | 'single': register one skill under resource_key. 'collection': treat git_path/local_path as a directory of MULTIPLE skill subfolders (e.g. a cloned anthropics/skills repo) and register one resource per subfolder, keyed by folder name. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | — | mode='single' only. Skill name override. If unset, read from the SKILL.md front matter at component load time. |
| `git_url` | `str` | — | Git remote URL to fetch the skill(s) from (e.g. https://github.com/org/repo.git). Exactly one of git_url / local_path / url is required. |
| `git_path` | `str` | — | Path within the git repo to the skill directory (mode='single') or the directory of skill subfolders (mode='collection'). Defaults to the repo root. |
| `git_ref` | `str` | `"main"` | Git branch or tag to check out. An arbitrary commit SHA isn't guaranteed to work (depends on whether the remote allows fetching by SHA). |
| `git_token_env_var` | `str` | — | Env var holding a token for private-repo git auth (sent as an Authorization: Bearer header during clone). |
| `local_path` | `str` | — | Local filesystem directory containing SKILL.md (mode='single') or a directory of skill subfolders (mode='collection'). Exactly one of git_url / local_path / url is required. |
| `url_headers_env` | `str` | — | Env var holding a JSON object of HTTP headers to send with the download request (e.g. '{"Authorization": "Bearer ..."}'), for authenticated downloads. |
| `include` | `List[str]` | — | mode='collection' only: allow-list of subfolder names to register. Leave unset to register every subfolder that contains a SKILL.md. |
| `exclude` | `List[str]` | — | mode='collection' only: subfolder names to skip, applied after include. |

[//]: # (FIELDS:END)
