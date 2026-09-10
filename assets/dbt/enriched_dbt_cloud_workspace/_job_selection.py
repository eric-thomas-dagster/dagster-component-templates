"""dbt-style selection DSL for filtering which dbt Cloud jobs get mirrored.

Vendored from ``et/dbt-cloud-mirror-jobs-selection`` PR branch. Delete when
the PR merges + releases; then import
``dagster_dbt.cloud_v2.job_selection.matches_selection`` /
``apply_selection`` directly.

Users often only want SOME Cloud jobs surfaced in Dagster — e.g. only
production deploy jobs, or everything except CI jobs. This module provides
a small selection DSL that mirrors dbt's ``--select`` / ``--exclude``
semantics adapted to what a Cloud job actually has: name, id, and job_type.

## Syntax

A selection string is a space-separated list of selectors. A job matches
the selection if ANY selector matches it (union / OR semantics).

Selector forms:

- ``type:<value>``  — matches ``job.job_type`` exactly (e.g. ``type:ci``,
  ``type:deploy``, ``type:merge``, ``type:scheduled``, ``type:other``)
- ``name:<glob>``   — fnmatch glob against ``job.name`` (case-sensitive)
- ``id:<int>``      — exact ``job.id`` match
- ``<glob>``        — bare token = shorthand for ``name:<glob>``
- ``*`` or empty    — matches every job

Include defaults to "everything", exclude defaults to "nothing." Exclude
runs after include.

## Examples

- ``include: "type:deploy"``               — mirror only deploy jobs
- ``include: "*_prod"``                    — mirror any job named ``*_prod``
- ``include: "type:deploy type:merge"``    — deploy OR merge jobs
- ``exclude: "type:ci"``                   — everything except CI jobs
- ``include: "*", exclude: "*_experimental"`` — everything except experimental
"""

import fnmatch
from collections.abc import Iterable
from typing import Any, Optional


def _match_single_selector(cloud_job: Any, selector: str) -> bool:
    """Match a single selector token against one Cloud job."""
    selector = selector.strip()
    if not selector or selector == "*":
        return True

    if ":" in selector:
        kind, _, value = selector.partition(":")
        kind = kind.strip()
        value = value.strip()
        if kind == "type":
            return (getattr(cloud_job, "job_type", None) or "") == value
        if kind == "name":
            return fnmatch.fnmatchcase(getattr(cloud_job, "name", None) or "", value)
        if kind == "id":
            try:
                return getattr(cloud_job, "id", None) == int(value)
            except ValueError:
                return False
        # Unknown selector kind: no match (safer than silently matching all).
        return False

    # Bare token = name glob shorthand.
    return fnmatch.fnmatchcase(getattr(cloud_job, "name", None) or "", selector)


def matches_selection(cloud_job: Any, selection: Optional[str]) -> bool:
    """Match ``cloud_job`` against a whole selection string.

    Returns True if ``cloud_job`` matches ANY selector in the space-separated
    ``selection`` string. ``None`` or empty string means "match everything."
    """
    if not selection or not selection.strip():
        return True
    tokens = selection.split()
    return any(_match_single_selector(cloud_job, tok) for tok in tokens)


def apply_selection(
    cloud_jobs: Iterable[Any],
    include: Optional[str],
    exclude: Optional[str],
) -> list:
    """Filter ``cloud_jobs`` by ``include`` then ``exclude`` selection strings.

    - ``include=None`` or empty: include every job.
    - ``exclude=None`` or empty: exclude nothing.
    - Exclude wins: a job matching both include and exclude is dropped.
    """
    result: list = []
    for cloud_job in cloud_jobs:
        if not matches_selection(cloud_job, include):
            continue
        if exclude and matches_selection(cloud_job, exclude):
            continue
        result.append(cloud_job)
    return result
