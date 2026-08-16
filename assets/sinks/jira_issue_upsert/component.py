"""DataFrame → Jira Issues upsert.

Mirrors an upstream DataFrame into Jira issues in a target project. Rows
are matched to existing issues by a Jira label of the form
`dagsterkey-<value>` where `<value>` comes from `key_column`. JQL filters
by label server-side, so this scales cleanly.

Matches → updated (summary, description, labels, status transition).
Misses → created.

Pairs with:
  - ``jira_resource`` — connection (required)
"""
import re
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


_KEY_LABEL_PREFIX = "dagsterkey-"
# Jira labels: alphanumeric + `_ - . /` (no spaces). Normalize keys to this.
_UNSAFE_LABEL_CHARS = re.compile(r"[^a-zA-Z0-9_./\-]")


def _sanitize_key(raw) -> str:
    """Normalize any value to a Jira-label-safe string."""
    return _UNSAFE_LABEL_CHARS.sub("_", str(raw))


def _extract_key(labels: Optional[list]) -> Optional[str]:
    """Extract the dagster key from an issue's labels list."""
    for label in labels or []:
        if isinstance(label, str) and label.startswith(_KEY_LABEL_PREFIX):
            return label[len(_KEY_LABEL_PREFIX):]
    return None


class JiraIssueUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into Jira issues.

    Example:
        ```yaml
        type: dagster_community_components.JiraIssueUpsertComponent
        attributes:
          asset_name: jira_incidents_mirror
          upstream_asset_key: incidents_seed
          project_key: SCRATCH
          resource_key: jira
          key_column: incident_id
          summary_column: name
          description_column: description
          labels_column: labels
          transition_column: status         # optional: e.g. 'Done', 'In Progress'
          issue_type: Task
          default_labels: [auto-synced]
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    project_key: str = Field(description="Target Jira project key (e.g. 'SCRATCH').")
    resource_key: str = Field(
        default="jira",
        description="Resource key registered by JiraResourceComponent.",
    )

    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key. Written to each issue as "
            "a `dagsterkey-<value>` label and matched server-side via JQL on re-runs."
        ),
    )
    summary_column: str = Field(description="Column holding the issue summary (title).")
    description_column: Optional[str] = Field(
        default=None,
        description="Column holding the issue description (plain text; auto-wrapped as ADF).",
    )
    labels_column: Optional[str] = Field(
        default=None,
        description="Column holding labels. Accepts a list, or a comma-separated string.",
    )
    transition_column: Optional[str] = Field(
        default=None,
        description=(
            "Column holding a workflow transition name to apply after upsert "
            "(e.g. 'Done', 'In Progress'). Ignored if the transition isn't valid "
            "from the issue's current state."
        ),
    )
    priority_column: Optional[str] = Field(
        default=None,
        description="Column holding priority name (e.g. 'Highest', 'High', 'Medium').",
    )

    issue_type: str = Field(default="Task", description="Issue type to use for new issues.")
    default_labels: List[str] = Field(
        default_factory=list,
        description="Labels always applied on top of `labels_column` (plus the auto-added dagsterkey-* marker).",
    )
    batch_size: int = Field(default=100, description="Max upstream rows to process per run (safety cap).")

    group_name: Optional[str] = Field(default="jira", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'jira').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("jira")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            required_resource_keys={_self.resource_key},
            description=_self.description or (
                f"Upsert DataFrame rows into Jira project {_self.project_key}."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream):
            jira = getattr(context.resources, _self.resource_key)

            import pandas as pd
            if not isinstance(upstream, pd.DataFrame):
                df = pd.DataFrame([upstream]) if isinstance(upstream, dict) else pd.DataFrame(upstream)
            else:
                df = upstream

            if len(df) == 0:
                context.log.warning("Upstream DataFrame is empty — nothing to upsert.")
                return dg.MaterializeResult(metadata={"rows_upserted": dg.MetadataValue.int(0)})

            if len(df) > _self.batch_size:
                context.log.warning(
                    f"Upstream has {len(df)} rows; capped at batch_size={_self.batch_size}."
                )
                df = df.head(_self.batch_size)

            # Column existence checks
            required_cols = {_self.key_column, _self.summary_column}
            for c in (_self.description_column, _self.labels_column, _self.transition_column, _self.priority_column):
                if c:
                    required_cols.add(c)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            # Index existing issues by dagsterkey-* label.
            # Jira JQL doesn't support wildcards on `labels =`, so we narrow
            # to "any-labels-set" then filter the prefix client-side.
            existing_by_key: Dict[str, dict] = {}
            jql = f'project = "{_self.project_key}" AND labels is not EMPTY'
            for issue in jira.iter_search_issues(jql, fields=["labels", "summary", "status"]):
                key = _extract_key(issue.get("fields", {}).get("labels"))
                if key:
                    existing_by_key[key] = issue

            def _coerce_list(value) -> List[str]:
                if value is None:
                    return []
                if isinstance(value, float) and pd.isna(value):
                    return []
                if isinstance(value, (list, tuple)):
                    return [str(v) for v in value]
                return [s.strip() for s in str(value).split(",") if s.strip()]

            created = 0
            updated = 0
            transitioned = 0
            for _, row in df.iterrows():
                key_val = row[_self.key_column]
                if isinstance(key_val, float) and pd.isna(key_val):
                    context.log.warning(f"Row has null key ({_self.key_column}) — skipping.")
                    continue
                key_str = _sanitize_key(key_val)
                key_label = f"{_KEY_LABEL_PREFIX}{key_str}"
                summary = str(row[_self.summary_column])
                description = (
                    str(row[_self.description_column])
                    if _self.description_column and not (isinstance(row[_self.description_column], float) and pd.isna(row[_self.description_column]))
                    else ""
                )

                labels = list(_self.default_labels) + [key_label]
                if _self.labels_column:
                    labels += _coerce_list(row[_self.labels_column])
                labels = list(dict.fromkeys(labels))  # de-dupe, preserve order

                priority = str(row[_self.priority_column]) if _self.priority_column else None
                transition = str(row[_self.transition_column]) if _self.transition_column else None

                existing = existing_by_key.get(key_str)
                if existing:
                    issue_key = existing["key"]
                    jira.update_issue(
                        issue_key,
                        summary=summary,
                        description=description if description else None,
                        labels=labels,
                        priority=priority,
                    )
                    updated += 1
                else:
                    issue = jira.create_issue(
                        _self.project_key,
                        summary=summary,
                        description=description,
                        issue_type=_self.issue_type,
                        labels=labels,
                        priority=priority,
                    )
                    existing_by_key[key_str] = issue
                    issue_key = issue["key"]
                    created += 1

                # Transition if requested (best-effort — skip if not valid from current state)
                if transition:
                    try:
                        jira.transition_issue(issue_key, transition)
                        transitioned += 1
                    except Exception as e:  # noqa: BLE001
                        context.log.warning(
                            f"Transition {transition!r} on {issue_key} failed: {e}"
                        )

            context.log.info(
                f"Jira upsert complete: {created} created, {updated} updated, {transitioned} transitioned."
            )
            return dg.MaterializeResult(
                metadata={
                    "jira_project_key": dg.MetadataValue.text(_self.project_key),
                    "rows_created": dg.MetadataValue.int(created),
                    "rows_updated": dg.MetadataValue.int(updated),
                    "rows_transitioned": dg.MetadataValue.int(transitioned),
                    "rows_upserted": dg.MetadataValue.int(created + updated),
                }
            )

        return dg.Definitions(assets=[_asset])
