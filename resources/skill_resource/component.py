"""Shared skill registration.

A "skill" here follows Anthropic's Agent Skills convention: a SKILL.md file with YAML
front matter (`name`, `description`) plus a markdown body of instructions, optionally
alongside other bundled files (references, scripts, templates) in the same folder. The
short name/description is cheap to show an agent up front ("here's what's available");
the full body (and any bundled file) is only fetched when the agent actually chooses to
use that skill -- the same progressive-disclosure shape Claude Code's own Skill tool
uses: don't inline the full text into every prompt.

This resource lets a project register a skill's SOURCE once (a git repo + path + ref, or
a local directory) and have litellm_agent/openai_agent/anthropic_agent/gemini_agent/
vercel_ai_gateway_agent/snowflake_cortex_agent/agentic_pipeline reference it by
`skill_keys` instead of repeating the source fields inline in every one of them -- and,
since a Dagster resource is independently inspectable, lets a UI see a skill's resolved
name/description without opening the file, the same way MCPServerResource lets a UI list
MCP servers as their own thing (see resources/mcp_server_resource).
"""
import hashlib
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

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
    return _CACHE_ROOT / key


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


class SkillResource(dg.ConfigurableResource):
    """Fetches and parses a SKILL.md (Anthropic Agent Skills format: YAML front matter +
    markdown body, plus optional bundled files) from either a git repo or a local
    directory. `name`/`description` are the resolved values (front matter, or an explicit
    override) -- already known at construction time, so a consumer never needs to fetch
    anything just to show "what is this skill" (`describe()`). The full instructions body
    + bundled file listing are only fetched when `load()` is actually called."""

    key: str = Field(description="The resource_key this resource was registered under.")
    name: str = Field(description="Skill name (shown to the agent before it decides whether to load the skill).")
    description: str = Field(description="Skill description (shown to the agent before it decides whether to load the skill).")
    git_url: Optional[str] = Field(default=None, description="Git remote URL the skill was fetched from.")
    git_path: str = Field(default="", description="Path within the git repo to the skill directory containing SKILL.md.")
    git_ref: str = Field(default="main", description="Git branch or tag checked out.")
    git_token_env_var: Optional[str] = Field(default=None, description="Env var holding a token for private-repo git auth, if the checkout needs to be re-fetched.")
    local_path: Optional[str] = Field(default=None, description="Local filesystem directory containing SKILL.md.")

    def _skill_dir(self) -> Path:
        if self.git_url:
            checkout = _ensure_git_checkout(self.git_url, self.git_ref, self.git_token_env_var)
            return (checkout / self.git_path) if self.git_path else checkout
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


class SkillResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a skill for use by the MCP-aware agent components (litellm_agent/
    openai_agent/anthropic_agent/gemini_agent/vercel_ai_gateway_agent/
    snowflake_cortex_agent/agentic_pipeline) via their own `skill_keys` field.

    `name`/`description` are read from the skill's own SKILL.md front matter at component
    load time if left unset -- so the resource's resolved config (visible in Dagster's own
    Resources UI) already shows what the skill is, without anyone needing to open the
    file. A git checkout is fetched once per (git_url, git_ref) and cached -- not re-fetched
    on every component load, and never per run. A missing SKILL.md, or a missing
    description with none given, fails loudly at component load (naming the exact path
    checked) rather than silently degrading -- this is a configuration error, not a
    transient one.

    Example:
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
    """

    resource_key: str = Field(default="skill", description="Dagster resource key other components reference via skill_keys.")
    name: Optional[str] = Field(default=None, description="Skill name override. If unset, read from the SKILL.md front matter at component load time.")
    description: Optional[str] = Field(default=None, description="Skill description override. If unset, read from the SKILL.md front matter at component load time.")
    git_url: Optional[str] = Field(default=None, description="Git remote URL to fetch the skill from (e.g. https://github.com/org/repo.git). Exactly one of git_url / local_path is required.")
    git_path: str = Field(default="", description="Path within the git repo to the skill directory containing SKILL.md. Defaults to the repo root.")
    git_ref: str = Field(default="main", description="Git branch or tag to check out. An arbitrary commit SHA isn't guaranteed to work (depends on whether the remote allows fetching by SHA).")
    git_token_env_var: Optional[str] = Field(default=None, description="Env var holding a token for private-repo git auth (sent as an Authorization: Bearer header during clone).")
    local_path: Optional[str] = Field(default=None, description="Local filesystem directory containing SKILL.md. Exactly one of git_url / local_path is required.")

    @model_validator(mode="after")
    def _exactly_one_source(self):
        if bool(self.git_url) == bool(self.local_path):
            raise ValueError(
                "SkillResourceComponent requires exactly one of `git_url` or `local_path`."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Skill Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.git_url:
            skill_dir = _ensure_git_checkout(self.git_url, self.git_ref, self.git_token_env_var)
            if self.git_path:
                skill_dir = skill_dir / self.git_path
            checked_path_desc = f"{self.git_url!r} (ref {self.git_ref!r}, path {self.git_path or '.'!r})"
        else:
            skill_dir = Path(self.local_path)
            checked_path_desc = str(skill_dir)

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
        )
        return dg.Definitions(resources={self.resource_key: resource})
