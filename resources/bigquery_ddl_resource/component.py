"""BigQueryDdlResourceComponent.

Reusable BigQuery DDL + value-check actions (dataset/table lifecycle, scalar
query value checks) exposed as a Dagster `ConfigurableResource` -- the
Airflow `BigQueryCreateEmptyDatasetOperator` / `BigQueryDeleteDatasetOperator`
/ `BigQueryCreateEmptyTableOperator` / `BigQueryDeleteTableOperator` /
`BigQueryCheckOperator` / `BigQueryValueCheckOperator` equivalent -- as
callable methods any op or asset can use, rather than a dedicated asset per
operator. An Airflow Operator is reusable action code, not itself a data
asset; this resource models that directly. Distinct from
`resources/bigquery_resource`, which just registers the official
`dagster_gcp.BigQueryResource` (a raw client) -- this resource adds the
higher-level idempotent DDL/check methods on top.
"""
from typing import Any, Dict, List, Literal, Optional

import dagster as dg
from pydantic import Field


_COMPARISONS = {
    "equal": lambda actual, expected, tol: (abs(actual - expected) <= tol) if tol else (actual == expected),
    "not_equal": lambda actual, expected, tol: not ((abs(actual - expected) <= tol) if tol else (actual == expected)),
    "greater_than": lambda actual, expected, tol: actual > expected,
    "greater_than_or_equal": lambda actual, expected, tol: actual >= expected,
    "less_than": lambda actual, expected, tol: actual < expected,
    "less_than_or_equal": lambda actual, expected, tol: actual <= expected,
}


def _bigquery_client(credentials, credentials_path, credentials_env_var, project_id, location):
    import json
    import os

    from google.cloud import bigquery
    from google.oauth2 import service_account

    creds_dict = credentials
    if creds_dict is None and credentials_path:
        with open(credentials_path, "r") as fh:
            creds_dict = json.load(fh)
    if creds_dict is None and credentials_env_var:
        raw = os.environ.get(credentials_env_var)
        if not raw:
            raise ValueError(f"Env var {credentials_env_var!r} (credentials_env_var) is not set.")
        creds_dict = json.loads(raw)
    if creds_dict is None:
        cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if cred_path:
            with open(cred_path, "r") as fh:
                creds_dict = json.load(fh)
    if creds_dict is None:
        raise ValueError("provide credentials, credentials_path, credentials_env_var, or set GOOGLE_APPLICATION_CREDENTIALS.")
    sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
    project = project_id or creds_dict.get("project_id")
    return bigquery.Client(credentials=sa_creds, project=project, location=location), project


def _schema_fields(table_schema):
    from google.cloud import bigquery

    out = []
    for f in table_schema or []:
        out.append(bigquery.SchemaField(
            name=f["name"],
            field_type=f.get("type", "STRING"),
            mode=f.get("mode", "NULLABLE"),
            description=f.get("description"),
        ))
    return out


class BigQueryDdlResource(dg.ConfigurableResource):
    """Reusable BigQuery dataset/table lifecycle + scalar value-check actions. Call from any op or asset."""

    credentials: Optional[Dict[str, Any]] = None
    credentials_path: Optional[str] = None
    credentials_env_var: Optional[str] = None
    project_id: Optional[str] = None
    location: Optional[str] = None

    def _client(self):
        return _bigquery_client(self.credentials, self.credentials_path, self.credentials_env_var, self.project_id, self.location)

    def _qualified_dataset_id(self, dataset_id, project):
        return dataset_id if "." in dataset_id else f"{project}.{dataset_id}"

    # --- dataset lifecycle -------------------------------------------------

    def ensure_dataset(
        self, dataset_id, *, location=None,
        default_table_expiration_ms=None, default_partition_expiration_ms=None,
        labels=None, dataset_description=None,
    ) -> Dict[str, Any]:
        """Idempotent create (`exists_ok=True`). Returns dataset_id/location/labels metadata."""
        from google.cloud import bigquery

        client, project = self._client()
        qualified = self._qualified_dataset_id(dataset_id, project)
        dataset_ref = bigquery.DatasetReference.from_string(qualified, default_project=project)
        ds = bigquery.Dataset(dataset_ref)
        _location = location or self.location
        if _location:
            ds.location = _location
        if default_table_expiration_ms is not None:
            ds.default_table_expiration_ms = default_table_expiration_ms
        if default_partition_expiration_ms is not None:
            ds.default_partition_expiration_ms = default_partition_expiration_ms
        if labels:
            ds.labels = dict(labels)
        if dataset_description:
            ds.description = dataset_description
        result = client.create_dataset(ds, exists_ok=True)
        return {
            "dataset_id": qualified,
            "location": result.location or "",
            "labels": dict(result.labels) if result.labels else {},
        }

    def delete_dataset(self, dataset_id, *, delete_contents=False) -> Dict[str, Any]:
        """Delete a dataset. `delete_contents=False` (default) raises if the dataset still has tables."""
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        client, project = self._client()
        qualified = self._qualified_dataset_id(dataset_id, project)
        dataset_ref = bigquery.DatasetReference.from_string(qualified, default_project=project)
        client.delete_dataset(dataset_ref, delete_contents=delete_contents, not_found_ok=True)
        try:
            client.get_dataset(dataset_ref)
            exists_after = True
        except NotFound:
            exists_after = False
        return {"dataset_id": qualified, "exists_after_delete": exists_after}

    def recreate_dataset(self, dataset_id, *, delete_contents=False, **ensure_kwargs) -> Dict[str, Any]:
        """Delete then re-create a dataset fresh."""
        self.delete_dataset(dataset_id, delete_contents=delete_contents)
        return self.ensure_dataset(dataset_id, **ensure_kwargs)

    # --- table lifecycle -----------------------------------------------------

    def ensure_table(
        self, dataset_id, table_id, table_schema, *,
        table_partition_field=None, table_partition_type="DAY",
        cluster_fields=None, expiration_ms=None, labels=None, table_description=None,
    ) -> Dict[str, Any]:
        """Idempotent create (`exists_ok=True`); never alters an existing table's columns."""
        from google.cloud import bigquery

        client, project = self._client()
        qualified_dataset = self._qualified_dataset_id(dataset_id, project)
        table_ref = f"{qualified_dataset}.{table_id}"
        table = bigquery.Table(table_ref, schema=_schema_fields(table_schema))
        if table_partition_field:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=getattr(bigquery.TimePartitioningType, table_partition_type),
                field=table_partition_field,
            )
        if cluster_fields:
            table.clustering_fields = list(cluster_fields)
        if expiration_ms is not None:
            from datetime import datetime, timedelta, timezone as _tz
            table.expires = datetime.now(_tz.utc) + timedelta(milliseconds=expiration_ms)
        if labels:
            table.labels = dict(labels)
        if table_description:
            table.description = table_description
        result = client.create_table(table, exists_ok=True)
        return {
            "table_ref": table_ref,
            "columns": [s.name for s in result.schema],
            "table_partition_field": result.time_partitioning.field if result.time_partitioning else None,
        }

    def delete_table(self, dataset_id, table_id) -> Dict[str, Any]:
        from google.api_core.exceptions import NotFound

        client, project = self._client()
        qualified_dataset = self._qualified_dataset_id(dataset_id, project)
        table_ref = f"{qualified_dataset}.{table_id}"
        client.delete_table(table_ref, not_found_ok=True)
        try:
            client.get_table(table_ref)
            exists_after = True
        except NotFound:
            exists_after = False
        return {"table_ref": table_ref, "exists_after_delete": exists_after}

    def recreate_table(self, dataset_id, table_id, table_schema, **ensure_kwargs) -> Dict[str, Any]:
        """Drop then re-create a table fresh from `table_schema`."""
        self.delete_table(dataset_id, table_id)
        return self.ensure_table(dataset_id, table_id, table_schema, **ensure_kwargs)

    # --- value check -----------------------------------------------------

    def run_value_check(
        self, query, expected_value, *,
        comparison: str = "equal", tolerance: Optional[float] = None, query_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Run a scalar query, compare its first result column to `expected_value`.

        Returns {actual_value, passed, rendered_query}. Raises `ValueError` for an
        unknown `comparison`, and `dg.Failure` if the query returns no rows --
        the caller decides whether a failed comparison should raise (matching
        `fail_on_mismatch` on the original asset-shaped component).
        """
        client, _project = self._client()
        params = dict(query_params or {})
        try:
            rendered = query.format(**params) if params else query
        except KeyError as e:
            raise ValueError(f"query references missing placeholder {e}; available: {list(params.keys())}")

        job = client.query(rendered)
        rows = list(job.result())
        if not rows:
            raise dg.Failure(description="bigquery_ddl_resource.run_value_check: query returned no rows.")
        actual_value = list(rows[0].values())[0]

        compare = _COMPARISONS.get(comparison)
        if compare is None:
            raise ValueError(f"unknown comparison: {comparison!r}; one of {sorted(_COMPARISONS)}")
        try:
            passed = compare(actual_value, expected_value, tolerance or 0)
        except TypeError:
            # non-numeric comparison, e.g. string equality
            passed = (actual_value == expected_value) if comparison == "equal" else (actual_value != expected_value) if comparison == "not_equal" else False

        return {"actual_value": actual_value, "passed": bool(passed), "rendered_query": rendered}


class BigQueryDdlResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a BigQueryDdlResource (dataset/table lifecycle + value-check actions) for use by other components.

    Distinct from `bigquery_resource` (which registers the official
    `dagster_gcp.BigQueryResource`, a raw client only) -- this resource adds
    idempotent `ensure_dataset`/`delete_dataset`/`ensure_table`/`delete_table`
    and `run_value_check` methods on top, for use from your own ops/assets
    the way an Airflow Operator's logic would be reused across tasks.
    """

    resource_key: str = Field(default="bigquery_ddl_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    credentials: Optional[Dict[str, Any]] = Field(default=None, description="Service-account JSON as a dict.")
    credentials_path: Optional[str] = Field(default=None, description="Path to a service-account JSON file.")
    credentials_env_var: Optional[str] = Field(default=None, description="Env var containing the service-account JSON as a string (same convention as bigquery_resource's/dataproc_resource's gcp_credentials_env_var). Checked after credentials and credentials_path; falls back to GOOGLE_APPLICATION_CREDENTIALS if none of the three are set.")
    project_id: Optional[str] = Field(default=None, description="GCP project. Defaults to the service account's own project.")
    location: Optional[str] = Field(default=None, description="Default BigQuery location, e.g. 'US' or 'EU'. Can be overridden per ensure_dataset() call.")

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Big Query Ddl Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = BigQueryDdlResource(
            credentials=self.credentials,
            credentials_path=self.credentials_path,
            credentials_env_var=self.credentials_env_var,
            project_id=self.project_id,
            location=self.location,
        )
        return dg.Definitions(resources={self.resource_key: resource})
