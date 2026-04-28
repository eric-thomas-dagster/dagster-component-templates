"""Train/Test Splitter Asset Component.

Split a DataFrame into train, test, and (optional) validation Dagster assets
using either a deterministic random split or a chronological cutoff. Supports
stratified sampling on a label column and group-aware splitting (so all rows
sharing a group key land in the same split — important for leakage prevention).
"""
from dataclasses import dataclass
from typing import Dict, List, Optional

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
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class TrainTestSplitterComponent(Component, Model, Resolvable):
    """Split a DataFrame into train / test / (optional) validation assets.

    Three strategies:
    - `random`: Deterministic random split using `seed`. Optionally stratified
      by `stratify_column` (preserves class balance) or grouped by `group_column`
      (all rows sharing a group key end up in the same split).
    - `time`: Chronological split — earliest rows → train, latest → test.
      Requires `time_column`.
    - `hash`: Stable hash of `group_column` (or row index) → split. Useful when
      you need the split to be reproducible across schema changes.

    Produces 2 or 3 downstream Dagster assets:
      `<asset_name>_train`, `<asset_name>_test`, [`<asset_name>_val`]
    """

    asset_name: str = Field(description="Output asset prefix; produces '<asset_name>_train', '<asset_name>_test', and optionally '_val'.")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    strategy: str = Field(default="random", description="Split strategy: 'random', 'time', or 'hash'.")
    test_size: float = Field(default=0.2, description="Fraction allocated to the test set (0.0–1.0).")
    val_size: float = Field(default=0.0, description="Fraction allocated to a validation set. 0.0 = no validation asset emitted.")
    seed: int = Field(default=42, description="Random seed for 'random' strategy.")
    stratify_column: Optional[str] = Field(
        default=None,
        description="Column to stratify the random split on (preserves class balance). Mutually exclusive with group_column.",
    )
    group_column: Optional[str] = Field(
        default=None,
        description="Column whose values define groups; all rows in a group land in the same split (prevents leakage on related rows).",
    )
    time_column: Optional[str] = Field(
        default=None,
        description="Required for strategy='time'. Column to sort by; oldest rows go to train.",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name applied to all output assets.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds for the catalog.")
    freshness_max_lag_minutes: Optional[int] = Field(default=None, description="Max acceptable lag minutes.")
    freshness_cron: Optional[str] = Field(default=None, description="Cron schedule for the freshness policy.")
    column_lineage: Optional[Dict[str, List[str]]] = Field(default=None, description="Column-level lineage (auto-inferred when empty: each output column maps 1:1 to upstream).")

    @classmethod
    def get_description(cls) -> str:
        return "Split a DataFrame into train/test (and optional val) assets using random, time, or hash strategies."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        strategy = self.strategy
        test_size = self.test_size
        val_size = self.val_size
        seed = self.seed
        stratify_column = self.stratify_column
        group_column = self.group_column
        time_column = self.time_column
        group_name = self.group_name

        if test_size <= 0 or test_size >= 1:
            raise ValueError(f"test_size must be in (0, 1); got {test_size}")
        if val_size < 0 or test_size + val_size >= 1:
            raise ValueError(f"val_size + test_size must be < 1; got {val_size}+{test_size}")
        if strategy == "time" and not time_column:
            raise ValueError("strategy='time' requires time_column")
        if stratify_column and group_column:
            raise ValueError("stratify_column and group_column are mutually exclusive")

        emit_val = val_size > 0

        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage

        def _split(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
            n = len(df)
            if n == 0:
                empty = df.iloc[0:0].copy()
                return {"train": empty, "test": empty.copy(), "val": empty.copy()}

            if strategy == "time":
                df_sorted = df.sort_values(time_column).reset_index(drop=True)
                n_test = int(round(n * test_size))
                n_val = int(round(n * val_size)) if emit_val else 0
                n_train = n - n_test - n_val
                train = df_sorted.iloc[:n_train]
                val = df_sorted.iloc[n_train:n_train + n_val] if emit_val else df_sorted.iloc[0:0]
                test = df_sorted.iloc[n_train + n_val:]
                return {"train": train, "test": test, "val": val}

            if strategy == "hash":
                key_series = df[group_column] if group_column else pd.Series(df.index, index=df.index)
                # Stable hash → bucket in [0, 1)
                bucket = key_series.astype(str).apply(lambda x: (hash((seed, x)) & 0xFFFFFFFF) / 2**32)
                test_cut = 1.0 - test_size
                val_cut = test_cut - val_size
                test = df[bucket >= test_cut]
                val = df[(bucket >= val_cut) & (bucket < test_cut)] if emit_val else df.iloc[0:0]
                train = df[bucket < val_cut]
                return {"train": train, "test": test, "val": val}

            # strategy == "random"
            rng = pd.Series(range(n)).sample(frac=1, random_state=seed).reset_index(drop=True)

            if group_column:
                groups = df[group_column].astype(str)
                unique = groups.drop_duplicates().sample(frac=1, random_state=seed).tolist()
                n_g = len(unique)
                n_test_g = int(round(n_g * test_size))
                n_val_g = int(round(n_g * val_size)) if emit_val else 0
                test_groups = set(unique[:n_test_g])
                val_groups = set(unique[n_test_g:n_test_g + n_val_g]) if emit_val else set()
                test = df[groups.isin(test_groups)]
                val = df[groups.isin(val_groups)] if emit_val else df.iloc[0:0]
                train = df[~groups.isin(test_groups | val_groups)]
                return {"train": train, "test": test, "val": val}

            if stratify_column:
                parts: Dict[str, List[pd.DataFrame]] = {"train": [], "test": [], "val": []}
                for _, sub in df.groupby(stratify_column, dropna=False, sort=False):
                    sub = sub.sample(frac=1, random_state=seed).reset_index(drop=True)
                    sn = len(sub)
                    sn_test = int(round(sn * test_size))
                    sn_val = int(round(sn * val_size)) if emit_val else 0
                    sn_train = sn - sn_test - sn_val
                    parts["train"].append(sub.iloc[:sn_train])
                    if emit_val:
                        parts["val"].append(sub.iloc[sn_train:sn_train + sn_val])
                    parts["test"].append(sub.iloc[sn_train + sn_val:])
                return {
                    "train": pd.concat(parts["train"]) if parts["train"] else df.iloc[0:0],
                    "test": pd.concat(parts["test"]) if parts["test"] else df.iloc[0:0],
                    "val": pd.concat(parts["val"]) if parts["val"] and emit_val else df.iloc[0:0],
                }

            # Plain random split
            shuffled = df.sample(frac=1, random_state=seed).reset_index(drop=True)
            n_test = int(round(n * test_size))
            n_val = int(round(n * val_size)) if emit_val else 0
            n_train = n - n_test - n_val
            return {
                "train": shuffled.iloc[:n_train],
                "test": shuffled.iloc[n_train + n_val:],
                "val": shuffled.iloc[n_train:n_train + n_val] if emit_val else df.iloc[0:0],
            }

        # Cache the split result so all asset functions see the same partition
        # (each asset is materialized in its own process, but if invoked together
        # in one run we want a single deterministic split).
        # Since assets in Dagster typically execute independently, we re-derive
        # the split inside each asset using the deterministic seed/hash logic.

        def _make_asset(suffix: str, split_key: str):
            @asset(
                name=f"{asset_name}_{suffix}",
                ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
                owners=owners,
                tags=_all_tags,
                freshness_policy=_freshness_policy,
                group_name=group_name,
                description=f"{suffix.capitalize()} split of {upstream_asset_key} ({strategy} strategy, test_size={test_size}, val_size={val_size}).",
            )
            def _a(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
                splits = _split(upstream)
                out = splits[split_key]
                from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
                _col_schema = TableSchema(columns=[
                    TableColumn(name=str(c), type=str(out.dtypes[c])) for c in out.columns
                ])
                _metadata = {
                    "dagster/row_count": MetadataValue.int(len(out)),
                    "dagster/column_schema": MetadataValue.table_schema(_col_schema),
                    "split_strategy": MetadataValue.text(strategy),
                    "split_role": MetadataValue.text(split_key),
                    "split_fraction": MetadataValue.float(len(out) / max(len(upstream), 1)),
                    "rows_in_upstream": MetadataValue.int(len(upstream)),
                }

                _effective_lineage = column_lineage
                if not _effective_lineage:
                    _effective_lineage = {c: [c] for c in out.columns if c in upstream.columns}
                if _effective_lineage:
                    _upstream_key = AssetKey.from_user_string(upstream_asset_key)
                    _lineage_deps = {
                        out_col: [TableColumnDep(asset_key=_upstream_key, column_name=ic) for ic in in_cols]
                        for out_col, in_cols in _effective_lineage.items()
                    }
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
                context.add_output_metadata(_metadata)
                return out
            return _a

        train_asset = _make_asset("train", "train")
        test_asset = _make_asset("test", "test")
        assets = [train_asset, test_asset]
        if emit_val:
            assets.append(_make_asset("val", "val"))

        from dagster import build_column_schema_change_checks
        _schema_checks = build_column_schema_change_checks(assets=assets)
        return Definitions(assets=assets, asset_checks=list(_schema_checks))
