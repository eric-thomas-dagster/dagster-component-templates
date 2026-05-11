"""FhirResourceNormalizerComponent — flatten FHIR R4/R5 resources to a DataFrame.

Takes an upstream DataFrame whose values are FHIR JSON resources (either as
parsed dicts or JSON strings) and emits one flat row per resource. Supports
the common resource types out of the box (`Patient`, `Observation`,
`Encounter`, `Condition`, `MedicationRequest`) with sane field extraction;
falls back to a generic field-walker for everything else.

Inspired by the `hris_normalizer` pattern: messy vendor data → canonical
flat schema. Same `value_maps` + case-insensitive matching shape.

Common use:
  - EHR ingest: hospital sends FHIR Bundles → flatten to BQ
  - Research data prep: pull FHIR resources from an API → flatten to parquet
  - Compliance reporting: extract specific fields across thousands of
    Patient resources without writing per-vendor SQL
"""

import json
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


# Per-resource extractors. Each returns a flat dict of canonical fields.
def _extract_patient(r: Dict[str, Any]) -> Dict[str, Any]:
    name0 = (r.get("name") or [{}])[0]
    given = " ".join(name0.get("given") or [])
    family = name0.get("family", "")
    addr0 = (r.get("address") or [{}])[0]
    return {
        "resource_type":   r.get("resourceType"),
        "id":              r.get("id"),
        "first_name":      given.strip() or None,
        "last_name":       family or None,
        "gender":          r.get("gender"),
        "birth_date":      r.get("birthDate"),
        "deceased":        bool(r.get("deceasedBoolean") or r.get("deceasedDateTime")),
        "city":            addr0.get("city"),
        "state":           addr0.get("state"),
        "country":         addr0.get("country"),
        "postal_code":     addr0.get("postalCode"),
    }


def _extract_observation(r: Dict[str, Any]) -> Dict[str, Any]:
    code = r.get("code", {})
    coding0 = (code.get("coding") or [{}])[0]
    vq = r.get("valueQuantity") or {}
    return {
        "resource_type":   r.get("resourceType"),
        "id":              r.get("id"),
        "patient_id":      (r.get("subject") or {}).get("reference", "").replace("Patient/", ""),
        "status":          r.get("status"),
        "code_system":     coding0.get("system"),
        "code":            coding0.get("code"),
        "display":         coding0.get("display") or code.get("text"),
        "effective_dt":    r.get("effectiveDateTime"),
        "value":           vq.get("value"),
        "unit":            vq.get("unit"),
    }


def _extract_encounter(r: Dict[str, Any]) -> Dict[str, Any]:
    period = r.get("period") or {}
    cls = r.get("class") or {}
    return {
        "resource_type":   r.get("resourceType"),
        "id":              r.get("id"),
        "patient_id":      (r.get("subject") or {}).get("reference", "").replace("Patient/", ""),
        "status":          r.get("status"),
        "class_code":      cls.get("code"),
        "class_display":   cls.get("display"),
        "start":           period.get("start"),
        "end":             period.get("end"),
        "reason_text":     ((r.get("reasonCode") or [{}])[0].get("text") if r.get("reasonCode") else None),
    }


def _extract_condition(r: Dict[str, Any]) -> Dict[str, Any]:
    code = r.get("code", {})
    coding0 = (code.get("coding") or [{}])[0]
    clinical = (r.get("clinicalStatus") or {}).get("coding") or [{}]
    return {
        "resource_type":   r.get("resourceType"),
        "id":              r.get("id"),
        "patient_id":      (r.get("subject") or {}).get("reference", "").replace("Patient/", ""),
        "code_system":     coding0.get("system"),
        "code":            coding0.get("code"),
        "display":         coding0.get("display") or code.get("text"),
        "clinical_status": (clinical[0].get("code") if clinical else None),
        "onset_dt":        r.get("onsetDateTime"),
        "recorded_dt":     r.get("recordedDate"),
    }


def _extract_medication_request(r: Dict[str, Any]) -> Dict[str, Any]:
    med_cc = r.get("medicationCodeableConcept") or {}
    coding0 = (med_cc.get("coding") or [{}])[0]
    dosage0 = (r.get("dosageInstruction") or [{}])[0]
    return {
        "resource_type":   r.get("resourceType"),
        "id":              r.get("id"),
        "patient_id":      (r.get("subject") or {}).get("reference", "").replace("Patient/", ""),
        "status":          r.get("status"),
        "intent":          r.get("intent"),
        "med_system":      coding0.get("system"),
        "med_code":        coding0.get("code"),
        "med_display":     coding0.get("display") or med_cc.get("text"),
        "authored_on":     r.get("authoredOn"),
        "dosage_text":     dosage0.get("text"),
    }


def _extract_generic(r: Dict[str, Any]) -> Dict[str, Any]:
    """Fallback — pull common top-level fields any FHIR resource may have."""
    return {
        "resource_type":   r.get("resourceType"),
        "id":              r.get("id"),
        "status":          r.get("status"),
        "patient_id":      (r.get("subject") or {}).get("reference", "").replace("Patient/", "")
                            if isinstance(r.get("subject"), dict) else None,
    }


_EXTRACTORS = {
    "Patient":           _extract_patient,
    "Observation":       _extract_observation,
    "Encounter":         _extract_encounter,
    "Condition":         _extract_condition,
    "MedicationRequest": _extract_medication_request,
}


class FhirResourceNormalizerComponent(Component, Model, Resolvable):
    """Flatten FHIR R4/R5 JSON resources into a flat DataFrame."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    resource_column: str = Field(
        default="resource",
        description="Column holding the FHIR resource. Values may be dicts OR JSON strings.",
    )

    resource_types: Optional[List[str]] = Field(
        default=None,
        description=(
            "Optional filter — only emit rows for these resource types. "
            "E.g. ['Patient', 'Observation']. Default: all."
        ),
    )

    value_maps: Optional[Dict[str, Dict[str, str]]] = Field(
        default=None,
        description=(
            "Per-column value normalization, like `hris_normalizer`. "
            "E.g. `{gender: {M: male, F: female}}`. Case-insensitive by default."
        ),
    )
    case_insensitive_map: bool = Field(default=True)

    drop_invalid: bool = Field(
        default=True,
        description="If True, silently drop rows whose resource is missing/un-parseable. "
                    "If False, emit a row with `resource_type=null` and an `_error` column.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        resource_column = self.resource_column
        filter_types = set(self.resource_types) if self.resource_types else None
        value_maps = self.value_maps or {}
        case_insensitive = self.case_insensitive_map
        drop_invalid = self.drop_invalid

        # Pre-lowercase the value_maps for case-insensitive lookup
        prepared_maps: Dict[str, Dict[str, str]] = {}
        if case_insensitive:
            for col, m in value_maps.items():
                prepared_maps[col] = {str(k).lower(): v for k, v in m.items()}
        else:
            prepared_maps = {col: dict(m) for col, m in value_maps.items()}

        @asset(
            name=asset_name,
            description=self.description or "Flatten FHIR resources to a canonical DataFrame.",
            group_name=self.group_name,
            kinds={"fhir", "healthcare", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            if resource_column not in upstream.columns:
                raise ValueError(
                    f"resource_column={resource_column!r} not in upstream: {list(upstream.columns)}"
                )

            rows: List[Dict[str, Any]] = []
            type_counts: Dict[str, int] = {}
            errors = 0
            for _, src in upstream.iterrows():
                v = src[resource_column]
                resource: Optional[Dict[str, Any]] = None
                err: Optional[str] = None
                if isinstance(v, dict):
                    resource = v
                elif isinstance(v, str):
                    try:
                        resource = json.loads(v)
                    except Exception as e:
                        err = f"json parse: {e}"
                else:
                    err = f"unsupported type: {type(v).__name__}"

                if resource is None:
                    errors += 1
                    if not drop_invalid:
                        rows.append({"resource_type": None, "_error": err})
                    continue

                rtype = resource.get("resourceType")
                if filter_types and rtype not in filter_types:
                    continue

                extractor = _EXTRACTORS.get(rtype, _extract_generic)
                flat = extractor(resource)

                # Apply value_maps
                for col, m in prepared_maps.items():
                    val = flat.get(col)
                    if col in flat and val is not None:
                        key = str(val).lower() if case_insensitive else str(val)
                        if key in m:
                            flat[col] = m[key]

                # Carry over any non-resource columns from the upstream row
                for c in upstream.columns:
                    if c != resource_column and c not in flat:
                        flat[c] = src[c]

                rows.append(flat)
                type_counts[rtype or "unknown"] = type_counts.get(rtype or "unknown", 0) + 1

            df = pd.DataFrame(rows)
            preview = df.head(10).to_markdown(index=False) if not df.empty else "(no rows)"
            return Output(
                value=df,
                metadata={
                    "input_rows":     MetadataValue.int(len(upstream)),
                    "output_rows":    MetadataValue.int(len(df)),
                    "invalid_rows":   MetadataValue.int(errors),
                    "by_resource":    MetadataValue.json(type_counts),
                    "preview":        MetadataValue.md(preview or ""),
                },
            )

        return Definitions(assets=[_asset])
