"""AgentFixtureCheckComponent -- grade an agent's structured outcome against owner-written fixtures.

An asset check (not a separate asset) attached to an agent's own outcome asset: it reads the
outcome's reported findings (filed, or practice-mode "would file" findings -- see the
engineering-agents feedback's practice-mode design), matches them against a fixtures file, and
reports recall/precision as an `AssetCheckResult` -- a human grade (e.g. a Slack 👍/👎) is a
separate signal this check doesn't attempt to replace.

Generalized from (not hardcoded to) a single internal reference implementation
(`defs/grades.py`'s `improvement_advisor_matches_fixtures`): `asset_key`, `fixtures_file`, and
which keys of the outcome dict hold findings are all configurable, so the same component grades
ANY agent whose outcome asset returns a dict shaped `{findings_key: [{"title": ..., "body": ...},
...], ...}` plus a fallback text field for when no structured findings exist at all (practice mode
with nothing matched, or a tool-less agent whose only record is its own final note).

Fixture file shape (JSON): `{"fixtures": [{"id": "...", "any": [["word", "word"], [...]]}]}` -- a
finding matches a fixture when it contains every word of AT LEAST ONE of its `any` groups
(case-insensitive substring match). Confirmed via the reference's own `_matches`/`grade` functions,
not reinvented.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


def _agent_fixture_matches(text: str, fixture: Dict[str, Any]) -> bool:
    low = text.lower()
    return any(all(word.lower() in low for word in group) for group in fixture["any"])


def agent_fixture_grade(
    outcome: Dict[str, Any], fixtures: List[Dict[str, Any]], *, findings_keys: List[str], fallback_text_key: str,
) -> Dict[str, Any]:
    """Match an outcome's findings (or, failing that, its fallback text) against fixtures.
    Pure/standalone so it's directly unit-testable without building a whole check."""
    findings: List[Dict[str, Any]] = []
    for key in findings_keys:
        findings.extend(outcome.get(key) or [])
    texts = [f"{f.get('title', '')} {f.get('body', '')}" for f in findings]
    # No structured findings at all (practice mode matched nothing, or a tool-less agent) -- the
    # fallback text (e.g. the agent's own final note/summary) is the only record of what it found.
    pool = texts or [str(outcome.get(fallback_text_key, ""))]
    matched = [fx["id"] for fx in fixtures if any(_agent_fixture_matches(t, fx) for t in pool)]
    useful = sum(1 for t in pool if any(_agent_fixture_matches(t, fx) for fx in fixtures))
    return {
        "matched": matched,
        "missed": [fx["id"] for fx in fixtures if fx["id"] not in matched],
        "findings_count": len(pool) if texts else None,
        "findings_matching_a_fixture": useful if texts else None,
        "recall": round(len(matched) / len(fixtures), 2) if fixtures else None,
        "precision": round(useful / len(texts), 2) if texts else None,
    }


class AgentFixtureCheckComponent(dg.Component, dg.Model, dg.Resolvable):
    """Attach a fixture-grading asset check to an agent's outcome asset.

    Example:

    ```yaml
    type: dagster_community_components.AgentFixtureCheckComponent
    attributes:
      asset_key: improvement_advisor_outcome
      fixtures_file: eval/improvement_fixtures.json
      min_recall: 0.5
      severity: WARN
    ```
    """

    asset_key: str = Field(description="The agent's outcome asset to attach this check to (e.g. 'improvement_advisor_outcome').")
    fixtures_file: str = Field(
        description=(
            "Path to a JSON fixtures file: {\"fixtures\": [{\"id\": \"...\", \"any\": [[\"word\", \"word\"], "
            "[...]]}]}. A finding matches a fixture when it contains every word of AT LEAST ONE of its "
            "`any` groups (case-insensitive substring match). Relative paths resolve against this "
            "component's own defs.yaml directory."
        ),
    )
    min_recall: float = Field(default=0.5, description="Minimum recall (matched fixtures / total fixtures) to pass.")
    severity: Literal["WARN", "ERROR"] = Field(default="WARN", description="Check severity when recall is below min_recall.")
    check_name: str = Field(default="matches_fixtures", description="Name of the emitted asset check.")
    findings_keys: List[str] = Field(
        default_factory=lambda: ["filed", "would_file"],
        description=(
            "Top-level keys of the outcome dict holding lists of {title, body} (or similarly-shaped) "
            "findings dicts -- default covers both a real filing and a practice-mode 'would file' "
            "record. Every listed key's entries are pooled together."
        ),
    )
    fallback_text_key: str = Field(
        default="text",
        description="Top-level key of the outcome dict used as the only evidence when no findings_keys entries exist at all (e.g. practice mode matched nothing, or a tool-less agent's final note).",
    )
    description: Optional[str] = Field(default=None, description="Check description. Defaults to a generated one naming the fixtures file and threshold.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("findings_keys",), csv=(), tags=())

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Agent Fixture Check", editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        asset_key = self.asset_key
        min_recall = self.min_recall
        severity = dg.AssetCheckSeverity.WARN if self.severity.upper() == "WARN" else dg.AssetCheckSeverity.ERROR
        findings_keys = list(self.findings_keys)
        fallback_text_key = self.fallback_text_key
        check_name = self.check_name

        fixtures_path = Path(self.fixtures_file)
        if not fixtures_path.is_absolute():
            base = getattr(context, "path", None)
            base_dir = base.parent if base is not None and hasattr(base, "parent") else Path.cwd()
            fixtures_path = base_dir / fixtures_path

        description = self.description or (
            f"Whether {asset_key} found enough of the problems seeded in {self.fixtures_file} "
            f"(needs recall >= {min_recall})."
        )

        @dg.asset_check(
            asset=dg.AssetKey.from_user_string(asset_key),
            name=check_name,
            description=description,
        )
        def _agent_fixture_check(context: dg.AssetCheckExecutionContext, outcome: Dict[str, Any]) -> dg.AssetCheckResult:
            fixtures = json.loads(fixtures_path.read_text())["fixtures"]
            result = agent_fixture_grade(outcome, fixtures, findings_keys=findings_keys, fallback_text_key=fallback_text_key)
            return dg.AssetCheckResult(
                passed=bool(result["recall"] is not None and result["recall"] >= min_recall),
                severity=severity,
                description=f"Found {len(result['matched'])} of {len(fixtures)} seeded problems (needs recall >= {min_recall}).",
                metadata={
                    "recall": result["recall"] if result["recall"] is not None else dg.MetadataValue.null(),
                    "precision": result["precision"] if result["precision"] is not None else dg.MetadataValue.null(),
                    "matched_fixture": dg.MetadataValue.json(result["matched"]),
                    "missed_fixture": dg.MetadataValue.json(result["missed"]),
                },
            )

        return dg.Definitions(asset_checks=[_agent_fixture_check])
