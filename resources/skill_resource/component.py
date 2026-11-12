"""Shared skill registration.

A "skill" here follows Anthropic's Agent Skills convention: a SKILL.md file with YAML
front matter (`name`, `description`) plus a markdown body of instructions, optionally
alongside other bundled files (references, scripts, templates) in the same folder. The
short name/description is cheap to show an agent up front ("here's what's available");
the full body (and any bundled file) is only fetched when the agent actually chooses to
use that skill -- the same progressive-disclosure shape Claude Code's own Skill tool
uses: don't inline the full text into every prompt.

This resource lets a project register a skill's SOURCE once (a git repo + path + ref, a
local directory, or a direct download URL) and have litellm_agent/openai_agent/
anthropic_agent/gemini_agent/vercel_ai_gateway_agent/snowflake_cortex_agent/
agentic_pipeline reference it by `skill_keys` instead of repeating the source fields
inline in every one of them -- and, since a Dagster resource is independently
inspectable, lets a UI see a skill's resolved name/description without opening the file,
the same way MCPServerResource lets a UI list MCP servers as their own thing (see
resources/mcp_server_resource).

`mode="collection"` registers every skill subfolder in a repo/directory at once (e.g.
anthropics/skills' ~19 skills) instead of needing one component instance per skill.
"""
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import dagster as dg
from pydantic import Field, model_validator


def _require_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise ValueError(f"Env var {var_name!r} is not set.")
    return value


def _split_frontmatter(raw: str) -> Tuple[dict, str]:
    """Splits a SKILL.md file into its YAML front matter dict and markdown body. Returns
    ({}, raw) if there's no '---'-delimited front matter block at the top of the file."""
    if not raw.startswith("---"):
        return {}, raw
    parts = raw.split("---", 2)
    if len(parts) < 3:
        return {}, raw
    import yaml

    frontmatter = yaml.safe_load(parts[1]) or {}
    body = parts[2].lstrip("\n")
    return frontmatter, body


_CACHE_ROOT = Path(tempfile.gettempdir()) / "dagster_community_components_skill_cache"


def _git_checkout_cache_dir(git_url: str, git_ref: str) -> Path:
    key = hashlib.sha256(f"{git_url}@{git_ref}".encode()).hexdigest()[:24]
    return _CACHE_ROOT / "git" / key


def _ensure_git_checkout(git_url: str, git_ref: str, git_token_env_var: Optional[str]) -> Path:
    """Shallow-clones git_url at git_ref into a per-(url, ref) cache dir under the system
    temp dir, reused by every later call for the same (git_url, git_ref) -- the clone only
    happens once (at component load / whenever the cache dir is missing), never per run.
    Requires a `git` binary on PATH. git_ref must be a branch or tag name -- shallow-cloning
    an arbitrary commit SHA isn't guaranteed to work (depends on whether the remote's server
    allows fetching by SHA; most don't)."""
    cache_dir = _git_checkout_cache_dir(git_url, git_ref)
    if cache_dir.is_dir() and any(cache_dir.iterdir()):
        return cache_dir

    cache_dir.parent.mkdir(parents=True, exist_ok=True)
    extra_args: List[str] = []
    if git_token_env_var:
        token = _require_env(git_token_env_var)
        extra_args = ["-c", f"http.extraHeader=Authorization: Bearer {token}"]

    partial_dir = f"{cache_dir}.partial-{os.getpid()}"
    shutil.rmtree(partial_dir, ignore_errors=True)
    result = subprocess.run(
        ["git", *extra_args, "clone", "--depth", "1", "--branch", git_ref, "--single-branch", git_url, partial_dir],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        shutil.rmtree(partial_dir, ignore_errors=True)
        raise RuntimeError(f"git clone of {git_url!r} at ref {git_ref!r} failed: {result.stderr.strip()[:500]}")
    os.replace(partial_dir, cache_dir)
    return cache_dir


def _url_cache_dir(url: str) -> Path:
    key = hashlib.sha256(url.encode()).hexdigest()[:24]
    return _CACHE_ROOT / "url" / key


def _parse_url_headers(url_headers_env: Optional[str]) -> Optional[Dict[str, str]]:
    if not url_headers_env:
        return None
    raw = _require_env(url_headers_env)
    try:
        headers = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"url_headers_env {url_headers_env!r} must hold a JSON object of HTTP headers, "
            f"e.g. '{{\"Authorization\": \"Bearer ...\"}}': {e}"
        ) from e
    if not isinstance(headers, dict):
        raise ValueError(f"url_headers_env {url_headers_env!r} must hold a JSON *object* of HTTP headers, got {type(headers).__name__}.")
    return {str(k): str(v) for k, v in headers.items()}


def _ensure_url_download(url: str, url_headers_env: Optional[str]) -> Path:
    """Downloads `url` once and caches it under a hash of the URL -- not re-downloaded on
    every component load, and never per run (same caching convention as
    `_ensure_git_checkout`). Accepts either a raw SKILL.md file or a .zip archive
    containing a skill folder (SKILL.md + any bundled files); detected from the response
    body's magic bytes, not just the URL's extension (some download endpoints don't put
    .zip in the URL at all)."""
    cache_dir = _url_cache_dir(url)
    if cache_dir.is_dir() and any(cache_dir.iterdir()):
        return cache_dir

    cache_dir.parent.mkdir(parents=True, exist_ok=True)
    headers = _parse_url_headers(url_headers_env)

    import requests
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    content = resp.content

    partial_dir = f"{cache_dir}.partial-{os.getpid()}"
    shutil.rmtree(partial_dir, ignore_errors=True)
    partial_path = Path(partial_dir)
    partial_path.mkdir(parents=True)

    is_zip = content[:4] == b"PK\x03\x04"
    if is_zip:
        import io
        import zipfile

        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            zf.extractall(partial_path)
        root = partial_path
        if not (root / "SKILL.md").is_file():
            # A zip of a single repo folder (e.g. a GitHub codeload download) typically
            # wraps everything in one top-level directory -- descend into it if that's
            # the only thing here and it actually holds a SKILL.md.
            subdirs = [p for p in root.iterdir() if p.is_dir()]
            if len(subdirs) == 1 and (subdirs[0] / "SKILL.md").is_file():
                root = subdirs[0]
        if not (root / "SKILL.md").is_file():
            shutil.rmtree(partial_dir, ignore_errors=True)
            raise ValueError(f"No SKILL.md found in the zip downloaded from {url!r}.")
        if root != partial_path:
            for item in root.iterdir():
                shutil.move(str(item), str(partial_path / item.name))
    else:
        (partial_path / "SKILL.md").write_bytes(content)

    os.replace(partial_dir, cache_dir)
    return cache_dir


class SkillResource(dg.ConfigurableResource):
    """Fetches and parses a SKILL.md (Anthropic Agent Skills format: YAML front matter +
    markdown body, plus optional bundled files) from a git repo, a local directory, or a
    download URL. `name`/`description` are the resolved values (front matter, or an
    explicit override) -- already known at construction time, so a consumer never needs
    to fetch anything just to show "what is this skill" (`describe()`). The full
    instructions body + bundled file listing are only fetched when `load()` is actually
    called."""

    key: str = Field(description="The resource_key this resource was registered under.")
    name: str = Field(description="Skill name (shown to the agent before it decides whether to load the skill).")
    description: str = Field(description="Skill description (shown to the agent before it decides whether to load the skill).")
    git_url: Optional[str] = Field(default=None, description="Git remote URL the skill was fetched from.")
    git_path: str = Field(default="", description="Path within the git repo to the skill directory containing SKILL.md.")
    git_ref: str = Field(default="main", description="Git branch or tag checked out.")
    git_token_env_var: Optional[str] = Field(default=None, description="Env var holding a token for private-repo git auth, if the checkout needs to be re-fetched.")
    local_path: Optional[str] = Field(default=None, description="Local filesystem directory containing SKILL.md.")
    url: Optional[str] = Field(default=None, description="URL the skill was downloaded from (a raw SKILL.md file or a .zip archive).")
    url_headers_env: Optional[str] = Field(default=None, description="Env var holding a JSON object of HTTP headers, if the download needs to be re-fetched.")

    def _skill_dir(self) -> Path:
        if self.git_url:
            checkout = _ensure_git_checkout(self.git_url, self.git_ref, self.git_token_env_var)
            return (checkout / self.git_path) if self.git_path else checkout
        if self.url:
            return _ensure_url_download(self.url, self.url_headers_env)
        return Path(self.local_path)

    def describe(self) -> dict:
        """Cheap, no I/O -- the part of a skill an agent sees before choosing to load it."""
        return {"key": self.key, "name": self.name, "description": self.description}

    def load(self) -> dict:
        """Returns {"content": <full SKILL.md text>, "files": [<other bundled files in the
        skill folder, as paths relative to it>]}. This is the part an agent only fetches
        once it has actually chosen to use the skill -- never inlined up front."""
        skill_dir = self._skill_dir()
        skill_md = skill_dir / "SKILL.md"
        content = skill_md.read_text()
        files = sorted(
            str(p.relative_to(skill_dir)) for p in skill_dir.rglob("*")
            if p.is_file() and p.name != "SKILL.md"
        )
        return {"content": content, "files": files}

    def read_file(self, relative_path: str) -> str:
        """Reads one bundled file by its path relative to the skill folder. Rejects any
        path that escapes the folder (e.g. '../../etc/passwd' or an absolute path)."""
        skill_dir = self._skill_dir().resolve()
        target = (skill_dir / relative_path).resolve()
        if not target.is_relative_to(skill_dir):
            raise ValueError(f"{relative_path!r} escapes the skill folder {skill_dir}")
        if not target.is_file():
            raise FileNotFoundError(f"No such file {relative_path!r} in skill folder {skill_dir}")
        return target.read_text()


def _read_skill_md_or_none(folder: Path) -> Optional[str]:
    skill_md = folder / "SKILL.md"
    return skill_md.read_text() if skill_md.is_file() else None


def _sanitize_skill_key(folder_name: str) -> str:
    """mode='collection' registers each skill under its folder name as a Dagster
    resource_key -- but a folder name (e.g. 'web-artifacts-builder', with hyphens, or one
    starting with a digit) isn't guaranteed to already be a valid resource_key shape.
    Replaces any run of characters outside [A-Za-z0-9_] with a single '_', and prefixes
    '_' if the result would start with a digit (so agents list this skill by the
    sanitized key, e.g. 'web_artifacts_builder', not the raw folder name)."""
    key = re.sub(r"[^A-Za-z0-9_]+", "_", folder_name)
    if key and key[0].isdigit():
        key = f"_{key}"
    return key


class SkillResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a skill (or, with `mode="collection"`, every skill in a repo/directory at
    once) for use by the MCP-aware agent components (litellm_agent/openai_agent/
    anthropic_agent/gemini_agent/vercel_ai_gateway_agent/snowflake_cortex_agent/
    agentic_pipeline) via their own `skill_keys` field.

    `name`/`description` are read from each skill's own SKILL.md front matter at
    component load time if left unset -- so the resource's resolved config (visible in
    Dagster's own Resources UI) already shows what the skill is, without anyone needing
    to open the file. A git checkout or download is fetched once (per `(git_url,
    git_ref)` or per `url`) and cached -- not re-fetched on every component load, and
    never per run. A missing SKILL.md, or a missing description with none given, fails
    loudly at component load (naming the exact path checked) rather than silently
    degrading -- this is a configuration error, not a transient one.

    ## Source: git, local, or a download URL

    Exactly one of `git_url`, `local_path`, or `url` is required -- setting more than
    one, or none, raises a clear error at component load:

    - **`git_url`** (+ optional `git_path`, `git_ref`, `git_token_env_var`): shallow-clones
      the repo and reads `<git_path>/SKILL.md` from it (or, in `mode="collection"`, treats
      `<git_path>` as a directory of skill subfolders). `git_token_env_var` names an env
      var holding a token for a private repo, sent as an `Authorization: Bearer <token>`
      header during clone.
    - **`local_path`**: reads `<local_path>/SKILL.md` directly off the local filesystem
      (or, in `mode="collection"`, treats it as a directory of skill subfolders).
    - **`url`** (+ optional `url_headers_env`): downloads either a raw `SKILL.md` file or
      a `.zip` archive containing a skill folder -- for skills distributed by a
      marketplace/registry that doesn't expose a git source. `url_headers_env` names an
      env var holding a JSON object of HTTP headers (e.g. `'{"Authorization": "Bearer
      ..."}'`) for authenticated downloads. Single-skill mode only -- not usable with
      `mode="collection"`.

    ## `mode="collection"`: register every skill in a repo/directory at once

    Instead of one component instance per skill, point `git_url`/`git_path` (or
    `local_path`) at a directory that CONTAINS multiple skill subfolders (e.g. a clone of
    `anthropics/skills`, which has ~19 skills as top-level folders) and every immediate
    subfolder containing a `SKILL.md` is registered as its own resource, keyed by its
    *sanitized* folder name -- any run of characters outside `[A-Za-z0-9_]` becomes a
    single `_` (e.g. a folder named `web-artifacts-builder` is keyed
    `web_artifacts_builder`; a leading digit gets a `_` prefix) -- so `skill_keys:
    [web_artifacts_builder]` works directly, without a separate `SkillResourceComponent`
    instance per skill. Agents list and reference skills by this *sanitized* key, not the
    raw folder name. Two folders that sanitize to the same key raise a clear error naming
    both; a folder name with no `[A-Za-z0-9_]` characters at all (nothing to sanitize)
    logs a warning and is skipped rather than taking down the whole collection.
    `name`/`description`/`url` aren't per-folder, so they aren't usable with
    `mode="collection"`; `include`/`exclude` filter by the raw folder name (a subfolder
    with no `SKILL.md` at all is silently skipped as "not a skill folder" either way; one
    that HAS a `SKILL.md` but no description is a real config problem and still fails
    loudly, naming that folder).

    Example (single skill):
        ```yaml
        type: dagster_community_components.SkillResourceComponent
        attributes:
          resource_key: pdf_report_skill
          git_url: https://github.com/your-org/agent-skills.git
          git_path: skills/pdf-report
          git_ref: main
        ```

    Example (a whole repo of skills at once):
        ```yaml
        type: dagster_community_components.SkillResourceComponent
        attributes:
          mode: collection
          git_url: https://github.com/anthropics/skills.git
          git_ref: main
          exclude: [".github"]
        ```

        Then, in any skill-aware agent component:
        ```yaml
        skill_keys:
          - pdf
        ```

    Example (a marketplace download, no git source):
        ```yaml
        type: dagster_community_components.SkillResourceComponent
        attributes:
          resource_key: some_marketplace_skill
          url: https://mcpmarket.example/skills/some-skill.zip
          url_headers_env: MCPMARKET_DOWNLOAD_HEADERS
        ```
    """

    resource_key: str = Field(default="skill", description="mode='single' only. Dagster resource key other components reference via skill_keys.")
    name: Optional[str] = Field(default=None, description="mode='single' only. Skill name override. If unset, read from the SKILL.md front matter at component load time.")
    description: Optional[str] = Field(default=None, description="mode='single' only. Skill description override. If unset, read from the SKILL.md front matter at component load time.")
    git_url: Optional[str] = Field(default=None, description="Git remote URL to fetch the skill(s) from (e.g. https://github.com/org/repo.git). Exactly one of git_url / local_path / url is required.")
    git_path: str = Field(default="", description="Path within the git repo to the skill directory (mode='single') or the directory of skill subfolders (mode='collection'). Defaults to the repo root.")
    git_ref: str = Field(default="main", description="Git branch or tag to check out. An arbitrary commit SHA isn't guaranteed to work (depends on whether the remote allows fetching by SHA).")
    git_token_env_var: Optional[str] = Field(default=None, description="Env var holding a token for private-repo git auth (sent as an Authorization: Bearer header during clone).")
    local_path: Optional[str] = Field(default=None, description="Local filesystem directory containing SKILL.md (mode='single') or a directory of skill subfolders (mode='collection'). Exactly one of git_url / local_path / url is required.")
    url: Optional[str] = Field(default=None, description="mode='single' only. URL to download the skill from -- either a raw SKILL.md file, or a .zip archive containing a skill folder (SKILL.md + any bundled files). Exactly one of git_url / local_path / url is required.")
    url_headers_env: Optional[str] = Field(default=None, description="Env var holding a JSON object of HTTP headers to send with the download request (e.g. '{\"Authorization\": \"Bearer ...\"}'), for authenticated downloads.")
    mode: Literal["single", "collection"] = Field(
        default="single",
        description="'single': register one skill under resource_key. 'collection': treat git_path/local_path as a directory of MULTIPLE skill subfolders (e.g. a cloned anthropics/skills repo) and register one resource per subfolder, keyed by folder name.",
    )
    include: Optional[List[str]] = Field(default=None, description="mode='collection' only: allow-list of subfolder names to register. Leave unset to register every subfolder that contains a SKILL.md.")
    exclude: Optional[List[str]] = Field(default=None, description="mode='collection' only: subfolder names to skip, applied after include.")

    @model_validator(mode="after")
    def _exactly_one_source(self):
        if sum(bool(x) for x in (self.git_url, self.local_path, self.url)) != 1:
            raise ValueError(
                "SkillResourceComponent requires exactly one of `git_url`, `local_path`, or `url`."
            )
        if self.mode == "collection":
            if self.url:
                raise ValueError("SkillResourceComponent: `url` isn't usable with mode='collection' (a collection needs a directory of skill subfolders, not a single download).")
            if self.name or self.description:
                raise ValueError("SkillResourceComponent: `name`/`description` aren't usable with mode='collection' (they're per-skill, read from each subfolder's own SKILL.md instead).")
        elif self.include or self.exclude:
            raise ValueError("SkillResourceComponent: `include`/`exclude` are only usable with mode='collection'.")
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Skill Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.mode == "collection":
            return self._build_collection_defs()
        return self._build_single_defs()

    def _resolve_source_root(self) -> Tuple[Path, str]:
        """Returns (root_dir, a human-readable description of the source, for error
        messages). Shared by single- and collection-mode."""
        if self.git_url:
            checkout = _ensure_git_checkout(self.git_url, self.git_ref, self.git_token_env_var)
            root = (checkout / self.git_path) if self.git_path else checkout
            desc = f"{self.git_url!r} (ref {self.git_ref!r}, path {self.git_path or '.'!r})"
            return root, desc
        if self.url:
            return _ensure_url_download(self.url, self.url_headers_env), repr(self.url)
        return Path(self.local_path), str(self.local_path)

    def _build_single_defs(self) -> dg.Definitions:
        skill_dir, checked_path_desc = self._resolve_source_root()

        skill_md = skill_dir / "SKILL.md"
        if not skill_md.is_file():
            raise ValueError(f"SkillResourceComponent {self.resource_key!r}: no SKILL.md found at {skill_md} (source: {checked_path_desc}).")

        frontmatter, _ = _split_frontmatter(skill_md.read_text())

        resolved_name = self.name or frontmatter.get("name") or self.resource_key
        resolved_description = self.description or frontmatter.get("description")
        if not resolved_description:
            raise ValueError(
                f"SkillResourceComponent {self.resource_key!r}: SKILL.md at {skill_md} has no "
                f"`description` in its front matter and none was given explicitly (source: "
                f"{checked_path_desc})."
            )

        resource = SkillResource(
            key=self.resource_key,
            name=resolved_name,
            description=resolved_description,
            git_url=self.git_url,
            git_path=self.git_path,
            git_ref=self.git_ref,
            git_token_env_var=self.git_token_env_var,
            local_path=self.local_path,
            url=self.url,
            url_headers_env=self.url_headers_env,
        )
        return dg.Definitions(resources={self.resource_key: resource})

    def _build_collection_defs(self) -> dg.Definitions:
        collection_root, checked_path_desc = self._resolve_source_root()
        if not collection_root.is_dir():
            raise ValueError(f"SkillResourceComponent collection: {collection_root} is not a directory (source: {checked_path_desc}).")

        include = set(self.include) if self.include else None
        exclude = set(self.exclude or [])

        resources: Dict[str, Any] = {}
        key_sources: Dict[str, str] = {}  # sanitized key -> the folder_name that claimed it
        for entry in sorted(collection_root.iterdir()):
            if not entry.is_dir():
                continue
            folder_name = entry.name
            if include is not None and folder_name not in include:
                continue
            if folder_name in exclude:
                continue
            raw = _read_skill_md_or_none(entry)
            if raw is None:
                continue  # not a skill folder -- no SKILL.md here, silently skip

            if not any(c.isalnum() or c == "_" for c in folder_name):
                # Every character in the folder name is outside [A-Za-z0-9_] (e.g. "!!!")
                # -- sanitizing would collapse it to a single, meaningless "_" rather than
                # anything derived from the real name. One unusable folder name shouldn't
                # take down the whole collection; skip it with a warning instead.
                import logging
                logging.getLogger("skill_resource").warning(
                    f"SkillResourceComponent collection ({checked_path_desc}): skill folder "
                    f"{folder_name!r} has no usable characters for a resource_key "
                    "([A-Za-z0-9_]) -- skipped."
                )
                continue
            key = _sanitize_skill_key(folder_name)
            if key in key_sources:
                raise ValueError(
                    f"SkillResourceComponent collection ({checked_path_desc}): skill folders "
                    f"{key_sources[key]!r} and {folder_name!r} both sanitize to the same "
                    f"resource_key {key!r} -- rename one of them, or use include/exclude to "
                    "drop one."
                )
            key_sources[key] = folder_name

            frontmatter, _ = _split_frontmatter(raw)
            resolved_name = frontmatter.get("name") or folder_name
            resolved_description = frontmatter.get("description")
            if not resolved_description:
                raise ValueError(
                    f"SkillResourceComponent collection ({checked_path_desc}): skill folder "
                    f"{folder_name!r} (SKILL.md at {entry / 'SKILL.md'}) has no `description` "
                    "in its front matter."
                )

            if self.git_url:
                member_git_path = f"{self.git_path}/{folder_name}" if self.git_path else folder_name
                resource: Any = SkillResource(
                    key=key, name=resolved_name, description=resolved_description,
                    git_url=self.git_url, git_path=member_git_path, git_ref=self.git_ref,
                    git_token_env_var=self.git_token_env_var,
                )
            else:
                resource = SkillResource(
                    key=key, name=resolved_name, description=resolved_description,
                    local_path=str(entry),
                )
            resources[key] = resource

        if not resources:
            raise ValueError(
                f"SkillResourceComponent collection ({checked_path_desc}): found no skill "
                f"subfolders (an immediate subdirectory containing a SKILL.md) under "
                f"{collection_root} -- check git_path/local_path and include/exclude."
            )
        return dg.Definitions(resources=resources)
