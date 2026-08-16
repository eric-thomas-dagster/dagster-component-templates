"""DataFrame → GitHub Issues upsert.

Mirrors an upstream DataFrame into GitHub Issues in a target repo. Rows
are matched to existing issues by a stable **key marker** embedded in the
issue body (e.g. `<!-- dagster-key: INC-1001 -->`). This is more robust
than title-matching — the title can be edited by humans without breaking
the sync.

Matches are updated (title, body, labels, state, assignees). Misses are
inserted as new issues. Optional `close_missing: true` closes issues whose
key marker is not in the upstream DataFrame.

Pairs with:
  - ``github_resource`` — connection (required)
"""
import re
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


def _make_body(row_body: str, key: str) -> str:
    """Prepend / update the key marker line in the issue body."""
    marker = f"<!-- dagster-key: {key} -->"
    # Strip any existing marker lines
    body_no_marker = re.sub(r"^<!-- dagster-key: [^>]+ -->\s*\n?", "", row_body or "", flags=re.MULTILINE)
    return f"{marker}\n\n{body_no_marker}".rstrip()


_MARKER_RE = re.compile(r"<!-- dagster-key: ([^>]+?) -->")


def _extract_key(body: Optional[str]) -> Optional[str]:
    if not body:
        return None
    m = _MARKER_RE.search(body)
    return m.group(1).strip() if m else None


class GitHubIssueUpsertComponent(dg.Component, dg.Model, dg.Resolvable):
    """Upsert rows from an upstream DataFrame into GitHub issues.

    Example:
        ```yaml
        type: dagster_community_components.GitHubIssueUpsertComponent
        attributes:
          asset_name: github_incidents_mirror
          upstream_asset_key: incidents_seed
          repo: my-org/incidents-tracker
          resource_key: github
          key_column: incident_id
          title_column: name
          body_column: description
          labels_column: labels          # optional: comma-separated string or list
          state_column: state            # optional: 'open' / 'closed'
          default_labels: [auto-synced]  # optional: always applied
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    upstream_asset_key: str = Field(description="Upstream asset providing the DataFrame.")
    repo: str = Field(description="Target repo in 'owner/name' form.")
    resource_key: str = Field(
        default="github",
        description="Resource key registered by GithubResourceComponent.",
    )

    key_column: str = Field(
        description=(
            "Upstream column holding a stable unique key for each row. Written into "
            "each issue's body as `<!-- dagster-key: <value> -->` and used to match "
            "rows to existing issues on subsequent runs."
        ),
    )
    title_column: str = Field(description="Column holding the issue title.")
    body_column: Optional[str] = Field(
        default=None,
        description="Column holding the issue body (markdown). Leave empty for title-only issues.",
    )
    labels_column: Optional[str] = Field(
        default=None,
        description="Column holding labels. Accepts a list, or a comma-separated string.",
    )
    state_column: Optional[str] = Field(
        default=None,
        description="Column holding state ('open' or 'closed'). Defaults to 'open' if unset.",
    )
    assignees_column: Optional[str] = Field(
        default=None,
        description="Column holding assignee logins. Accepts a list, or a comma-separated string.",
    )
    default_labels: List[str] = Field(
        default_factory=list,
        description="Labels always applied to every synced issue, on top of `labels_column`.",
    )

    close_missing: bool = Field(
        default=False,
        description=(
            "If true, close open issues whose key marker is NOT in the upstream "
            "DataFrame. Off by default."
        ),
    )
    batch_size: int = Field(
        default=100,
        description="Max upstream rows to process per run (safety cap).",
    )

    group_name: Optional[str] = Field(default="github", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    tags: Optional[Dict[str, str]] = Field(default=None, description="Catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'github').")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("github")

        @dg.asset(
            key=dg.AssetKey.from_user_string(_self.asset_name),
            ins={"upstream": dg.AssetIn(key=dg.AssetKey.from_user_string(_self.upstream_asset_key))},
            group_name=_self.group_name,
            kinds=kinds,
            owners=_self.owners,
            tags=_self.tags,
            required_resource_keys={_self.resource_key},
            description=_self.description or (
                f"Upsert DataFrame rows into GitHub issues on {_self.repo}."
            ),
        )
        def _asset(context: dg.AssetExecutionContext, upstream):
            gh = getattr(context.resources, _self.resource_key)

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
            required_cols = {_self.key_column, _self.title_column}
            for c in (_self.body_column, _self.labels_column, _self.state_column, _self.assignees_column):
                if c:
                    required_cols.add(c)
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise dg.Failure(
                    f"Columns not in upstream: {missing_cols}. Available: {list(df.columns)}"
                )

            # Index existing issues by dagster-key marker in body
            existing_by_key: Dict[str, dict] = {}
            for issue in gh.iter_issues(_self.repo, state="all"):
                if issue.get("pull_request"):
                    continue  # /issues endpoint returns PRs too — skip them
                key = _extract_key(issue.get("body"))
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
            for _, row in df.iterrows():
                key_val = row[_self.key_column]
                if isinstance(key_val, float) and pd.isna(key_val):
                    context.log.warning(f"Row has null key ({_self.key_column}) — skipping.")
                    continue
                key_str = str(key_val)
                title = str(row[_self.title_column])
                body = _make_body(str(row[_self.body_column]) if _self.body_column else "", key_str)

                labels = list(_self.default_labels)
                if _self.labels_column:
                    labels += _coerce_list(row[_self.labels_column])
                labels = list(dict.fromkeys(labels))  # de-dupe, preserve order

                state = str(row[_self.state_column]).lower() if _self.state_column else "open"
                if state not in ("open", "closed"):
                    state = "open"

                assignees = _coerce_list(row[_self.assignees_column]) if _self.assignees_column else []

                existing = existing_by_key.get(key_str)
                if existing:
                    gh.update_issue(
                        _self.repo, existing["number"],
                        title=title, body=body, state=state,
                        labels=labels or None,
                        assignees=assignees or None,
                    )
                    updated += 1
                else:
                    issue = gh.create_issue(
                        _self.repo, title=title, body=body,
                        labels=labels or None,
                        assignees=assignees or None,
                    )
                    # Cache the newly-created issue so a retry within the same
                    # run doesn't dupe it (GitHub's list endpoint is eventually
                    # consistent — new issues may not appear via iter_issues
                    # for several seconds).
                    existing_by_key[key_str] = issue
                    if state == "closed":
                        gh.close_issue(_self.repo, issue["number"])
                    created += 1

            closed = 0
            if _self.close_missing:
                upstream_keys = {str(v) for v in df[_self.key_column].dropna().tolist()}
                for key_str, issue in existing_by_key.items():
                    if key_str in upstream_keys:
                        continue
                    if issue.get("state") == "open":
                        gh.close_issue(_self.repo, issue["number"])
                        closed += 1

            context.log.info(
                f"GitHub upsert complete: {created} created, {updated} updated, {closed} closed."
            )
            return dg.MaterializeResult(
                metadata={
                    "github_repo": dg.MetadataValue.text(_self.repo),
                    "rows_created": dg.MetadataValue.int(created),
                    "rows_updated": dg.MetadataValue.int(updated),
                    "rows_closed": dg.MetadataValue.int(closed),
                    "rows_upserted": dg.MetadataValue.int(created + updated),
                }
            )

        return dg.Definitions(assets=[_asset])
