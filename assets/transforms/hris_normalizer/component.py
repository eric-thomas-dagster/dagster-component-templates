"""HrisNormalizerComponent — vendor-agnostic HRIS schema normalization.

Takes any DataFrame of employee data (from Workday, BambooHR, ADP,
Gusto, Rippling, Hibob, an internal HRIS export, a CSV, anything) and
maps it to a canonical employee schema downstream HR analytics
components can rely on.

Canonical schema (one row per employee):
  employee_id, email, first_name, last_name, full_name,
  manager_employee_id, department, job_title, location, country,
  employment_status, employment_type, hire_date, termination_date,
  tenure_days, is_active

The mapping is config-driven — provide a vendor-specific
`column_map: { canonical_field: source_column }` mapping. Optional
status / type / boolean normalization rules cover the most common
vendor differences.
"""

import datetime as _dt
import os
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


_CANONICAL_FIELDS = [
    "employee_id",
    "email",
    "first_name",
    "last_name",
    "full_name",
    "manager_employee_id",
    "department",
    "job_title",
    "location",
    "country",
    "employment_status",
    "employment_type",
    "hire_date",
    "termination_date",
]


_DEFAULT_STATUS_MAP = {
    "active":          "active",
    "a":               "active",
    "current":         "active",
    "inactive":        "inactive",
    "terminated":      "terminated",
    "terminate":       "terminated",
    "term":            "terminated",
    "leave":           "on_leave",
    "leave of absence": "on_leave",
    "loa":             "on_leave",
    "pending":         "pending",
    "future":          "pending",
}

_DEFAULT_TYPE_MAP = {
    "full_time":  "full_time",
    "fulltime":   "full_time",
    "ft":         "full_time",
    "part_time":  "part_time",
    "parttime":   "part_time",
    "pt":         "part_time",
    "contractor": "contractor",
    "contract":   "contractor",
    "intern":     "intern",
    "temp":       "temporary",
    "temporary":  "temporary",
}


class HrisNormalizerComponent(Component, Model, Resolvable):
    """Vendor-agnostic HRIS normalization. Maps any employee DataFrame to
    a canonical schema usable by downstream `hr_metrics` and other
    HR-aware components.
    """

    asset_name: str = Field(description="Output asset name (the normalized employee table).")
    upstream_asset_key: str = Field(
        description="Upstream DataFrame asset — the raw HRIS data.",
    )

    column_map: Dict[str, str] = Field(
        description=(
            "{canonical_field: source_column}. Canonical fields: "
            "employee_id, email, first_name, last_name, full_name, "
            "manager_employee_id, department, job_title, location, country, "
            "employment_status, employment_type, hire_date, termination_date. "
            "Missing canonical fields are filled with None."
        ),
    )

    status_map: Optional[Dict[str, str]] = Field(
        default=None,
        description=(
            "{vendor_value: canonical_value} for employment_status. Defaults to "
            "a sane mapping (active/inactive/terminated/on_leave/pending). "
            "Set explicitly to override."
        ),
    )
    type_map: Optional[Dict[str, str]] = Field(
        default=None,
        description="{vendor_value: canonical_value} for employment_type. Defaults to full_time/part_time/contractor/intern/temporary.",
    )

    derive_full_name: bool = Field(
        default=True,
        description="If full_name isn't mapped, derive it from first_name + last_name.",
    )
    compute_tenure: bool = Field(
        default=True,
        description="Adds a tenure_days column (today - hire_date if active, else termination_date - hire_date).",
    )
    derive_is_active: bool = Field(
        default=True,
        description="Adds an is_active boolean column based on the normalized employment_status.",
    )

    drop_extra_columns: bool = Field(
        default=False,
        description="If True, only the canonical columns are kept. If False (default), source columns are preserved with a vendor_ prefix.",
    )
    case_insensitive_map: bool = Field(
        default=True,
        description="Lowercase + strip vendor values before applying status_map / type_map.",
    )

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        unknown_targets = [k for k in self.column_map.keys() if k not in _CANONICAL_FIELDS]
        if unknown_targets:
            raise ValueError(
                f"HrisNormalizerComponent: column_map keys must be canonical fields. "
                f"Unknown: {unknown_targets}. Allowed: {_CANONICAL_FIELDS}"
            )

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        column_map = dict(self.column_map)
        status_map = {**_DEFAULT_STATUS_MAP, **(self.status_map or {})}
        type_map = {**_DEFAULT_TYPE_MAP, **(self.type_map or {})}
        derive_full_name = self.derive_full_name
        compute_tenure = self.compute_tenure
        derive_is_active = self.derive_is_active
        drop_extra_columns = self.drop_extra_columns
        case_insensitive_map = self.case_insensitive_map

        @asset(
            name=asset_name,
            description=self.description or f"Canonical HRIS schema mapped from {self.upstream_asset_key}.",
            group_name=self.group_name,
            kinds={"hris", "pandas"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            df = upstream.copy()
            if df.empty:
                empty = pd.DataFrame(columns=_CANONICAL_FIELDS)
                return Output(value=empty, metadata={"row_count": MetadataValue.int(0)})

            # Build the canonical-output DataFrame.
            out: Dict[str, Any] = {}
            for canon, src in column_map.items():
                if src not in df.columns:
                    context.log.warning(
                        f"column_map: source column {src!r} → {canon!r} not in upstream. Filling with None."
                    )
                    out[canon] = pd.Series([None] * len(df))
                else:
                    out[canon] = df[src]
            for canon in _CANONICAL_FIELDS:
                if canon not in out:
                    out[canon] = pd.Series([None] * len(df))

            normalized = pd.DataFrame(out)

            # Derive full_name if missing.
            if derive_full_name and (normalized["full_name"].isna().all() or normalized["full_name"].astype(str).str.strip().eq("").all()):
                fn = normalized["first_name"].fillna("").astype(str).str.strip()
                ln = normalized["last_name"].fillna("").astype(str).str.strip()
                derived = (fn + " " + ln).str.strip()
                normalized["full_name"] = derived.where(derived.ne(""), other=None)

            # Apply status_map + type_map. When case_insensitive_map is True we
            # lowercase BOTH the input value AND the map keys before lookup,
            # so user maps like {A: active} match raw values like 'A' / 'a'.
            def _build_lookup(mapping: Dict[str, str]) -> Dict[str, str]:
                if not case_insensitive_map:
                    return dict(mapping)
                return {str(k).strip().lower(): v for k, v in mapping.items()}

            status_lookup = _build_lookup(status_map)
            type_lookup = _build_lookup(type_map)

            def _norm_value(v: Any, mapping: Dict[str, str]) -> Optional[str]:
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return None
                key = str(v).strip()
                if case_insensitive_map:
                    key = key.lower()
                return mapping.get(key, str(v))  # fallback: pass through

            normalized["employment_status"] = normalized["employment_status"].apply(lambda v: _norm_value(v, status_lookup))
            normalized["employment_type"]   = normalized["employment_type"].apply(lambda v: _norm_value(v, type_lookup))

            # Parse dates.
            for col in ("hire_date", "termination_date"):
                normalized[col] = pd.to_datetime(normalized[col], errors="coerce", utc=False).dt.date.astype(object).where(pd.notna(pd.to_datetime(normalized[col], errors='coerce')), None)

            # Compute tenure_days.
            if compute_tenure:
                today = _dt.date.today()
                def _tenure(row):
                    hd = row.get("hire_date")
                    td = row.get("termination_date")
                    if not hd or pd.isna(hd):
                        return None
                    end = td if (td and not pd.isna(td)) else today
                    try:
                        return (end - hd).days
                    except Exception:
                        return None
                normalized["tenure_days"] = normalized.apply(_tenure, axis=1)

            # Derive is_active.
            if derive_is_active:
                normalized["is_active"] = normalized["employment_status"].apply(
                    lambda s: s == "active" if isinstance(s, str) else False
                )

            # Optionally preserve source columns with vendor_ prefix.
            if not drop_extra_columns:
                for col in df.columns:
                    if col not in column_map.values():
                        new_name = f"vendor_{col}"
                        if new_name not in normalized.columns:
                            normalized[new_name] = df[col].values

            preview_md = normalized.head(10).to_markdown(index=False) if not normalized.empty else "(empty)"
            md = {
                "row_count":     MetadataValue.int(len(normalized)),
                "column_count":  MetadataValue.int(len(normalized.columns)),
                "active_count":  MetadataValue.int(int(normalized.get("is_active", pd.Series([], dtype=bool)).sum())),
                "preview":       MetadataValue.md(preview_md or ""),
            }
            if "department" in normalized.columns:
                md["department_count"] = MetadataValue.int(int(normalized["department"].nunique(dropna=True)))
            return Output(value=normalized, metadata=md)

        return Definitions(assets=[_asset])
