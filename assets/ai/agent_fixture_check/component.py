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

A second mode, `match_mode: "fields"`, grades structured extraction output instead of free text --
found while auditing this registry against a document-intelligence recipe list: a document-extraction
agent's outcome is a flat `{field_name: value}` dict (e.g. `invoice_total: 1042.50`), and the original
whole-word-substring matcher can't grade that at all (it only pools `{title} {body}` free text). In
`fields` mode, the fixtures file instead lists `{"id": "...", "field": "...", "expected": ...}`
entries; each named field is compared against its golden value with a type-appropriate fuzzy match
(string: `difflib.SequenceMatcher` token-sort ratio; number: `math.isclose` near-equality) instead of
substring containment. `text` mode (the default) is completely unchanged -- `fields` mode is an
additional capability, not a replacement.
"""

import json
import math
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
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
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


def _agent_fixture_matches(text: str, fixture: Dict[str, Any]) -> bool:
    low = text.lower()
    return any(all(word.lower() in low for word in group) for group in fixture["any"])


# ── fields mode: fuzzy-compare structured extraction output against golden values ────────────────


def _as_number(value: Any) -> Optional[float]:
    """Coerce a value to float for near-equality comparison, or None if it isn't numeric.
    Booleans are excluded even though `isinstance(True, int)` is true in Python -- a bool field
    should be graded as text ("true"/"false"), not as 0/1."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "").strip())
        except ValueError:
            return None
    return None


def _field_fuzzy_match(actual: Any, expected: Any, *, fuzzy_field_threshold: float, numeric_tolerance: float) -> bool:
    """Compare one extracted field value against its golden fixture value. Numbers (including
    numeric strings like "1,042.50") are compared via near-equality (`math.isclose`, relative
    tolerance); everything else is compared as text via a token-sort ratio (case-insensitive,
    word order ignored) using stdlib `difflib.SequenceMatcher` -- no new third-party dependency,
    matching this repo's existing precedent for lightweight fuzzy text similarity (see
    `assets/transforms/fuzzy_match` and `assets/ai/text_similarity`, which use the same approach;
    neither `rapidfuzz` nor `python-Levenshtein` is a dependency anywhere in this repo)."""
    if actual is None:
        return False
    a_num, e_num = _as_number(actual), _as_number(expected)
    if a_num is not None and e_num is not None:
        return math.isclose(a_num, e_num, rel_tol=numeric_tolerance, abs_tol=1e-9)
    a_sorted = " ".join(sorted(str(actual).lower().split()))
    e_sorted = " ".join(sorted(str(expected).lower().split()))
    return SequenceMatcher(None, a_sorted, e_sorted).ratio() >= fuzzy_field_threshold


def agent_fixture_grade_fields(
    outcome: Dict[str, Any], fixtures: List[Dict[str, Any]], *, fields_key: str, fuzzy_field_threshold: float, numeric_tolerance: float,
) -> Dict[str, Any]:
    """Field-level counterpart to `agent_fixture_grade()` -- compares a flat `field_name: value`
    dict (e.g. a document-extraction outcome) against per-field golden fixtures shaped
    `{"id": ..., "field": ..., "expected": ...}`, instead of pooling free text. Returns the SAME
    matched/missed/recall/precision shape so both modes feed identical `AssetCheckResult` metadata.
    Pure/standalone so it's directly unit-testable without building a whole check."""
    extracted: Dict[str, Any] = outcome.get(fields_key) or {}
    matched = [
        fx["id"] for fx in fixtures
        if fx["field"] in extracted
        and _field_fuzzy_match(extracted[fx["field"]], fx["expected"], fuzzy_field_threshold=fuzzy_field_threshold, numeric_tolerance=numeric_tolerance)
    ]
    matched_field_names = {fx["field"] for fx in fixtures if fx["id"] in matched}
    graded_field_names = {fx["field"] for fx in fixtures}
    # Precision denominator is extracted fields the fixtures actually grade (not every field the
    # agent happened to extract) -- mirrors text mode's "findings that match a fixture / findings
    # produced", scoped to fields we have a golden value for at all.
    extracted_graded = [name for name in extracted if name in graded_field_names]
    useful = sum(1 for name in extracted_graded if name in matched_field_names)
    return {
        "matched": matched,
        "missed": [fx["id"] for fx in fixtures if fx["id"] not in matched],
        "findings_count": len(extracted_graded),
        "findings_matching_a_fixture": useful,
        "recall": round(len(matched) / len(fixtures), 2) if fixtures else None,
        "precision": round(useful / len(extracted_graded), 2) if extracted_graded else None,
    }


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
    match_mode: Literal["text", "fields"] = Field(
        default="text",
        description=(
            "'text' (default, unchanged): pools {title, body} free-text findings and matches them "
            "against word-group fixtures (see fixtures_file). 'fields': grades structured extraction "
            "output instead -- the outcome's fields_key holds a flat {field_name: value} dict (e.g. "
            "document-extraction output), compared field-by-field against fixtures_file entries shaped "
            "{\"id\": \"...\", \"field\": \"...\", \"expected\": ...} using fuzzy string/numeric matching "
            "(see fuzzy_field_threshold, numeric_tolerance)."
        ),
    )
    fields_key: str = Field(
        default="fields",
        description="Top-level key of the outcome dict holding the extracted field_name -> value dict. Only used when match_mode is 'fields'.",
    )
    fuzzy_field_threshold: float = Field(
        default=0.7,
        description=(
            "Minimum text-similarity ratio (0-1) for a string field to count as matching its fixture's "
            "expected value, in 'fields' mode. Computed with a token-sort ratio (word order ignored, "
            "case-insensitive) over Python's stdlib difflib.SequenceMatcher -- e.g. 'Acme Corp' vs "
            "'Acme Corporation' scores 0.72, above the 0.7 default, while a genuinely different value "
            "scores well below it. Only used when match_mode is 'fields'."
        ),
    )
    numeric_tolerance: float = Field(
        default=0.01,
        description=(
            "Relative tolerance for a numeric field to count as matching its fixture's expected value, "
            "in 'fields' mode -- e.g. 0.01 means within 1% (so 1042.50 matches an expected 1042.51, but "
            "not an expected 950.00). Passed as math.isclose's rel_tol. Only used when match_mode is "
            "'fields'."
        ),
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
        match_mode = self.match_mode
        fields_key = self.fields_key
        fuzzy_field_threshold = self.fuzzy_field_threshold
        numeric_tolerance = self.numeric_tolerance

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
            if match_mode == "fields":
                result = agent_fixture_grade_fields(
                    outcome, fixtures, fields_key=fields_key,
                    fuzzy_field_threshold=fuzzy_field_threshold, numeric_tolerance=numeric_tolerance,
                )
            else:
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
