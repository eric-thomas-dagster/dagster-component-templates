"""DataFrame Transformer Asset Component.

Transform DataFrames from upstream assets using IO managers for automatic data flow.
Works with visual dependency drawing - just connect DataFrame-producing assets!
"""

from typing import Optional, Union
import json

import pandas as pd
from dagster import (
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetKey,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field, field_validator


class DataFrameTransformerComponent(Component, Model, Resolvable):
    """Component for transforming DataFrames from upstream assets.

    This component works with visual dependency drawing in Dagster Designer.
    Simply draw a connection from any DataFrame-producing asset (REST API, Database Query,
    CSV Ingestion) to this transformer, and it will automatically receive the DataFrame.

    **Compatible upstream assets:**
    - REST API Fetcher (with output_format: dataframe)
    - Database Query
    - CSV File Ingestion
    - Other DataFrame Transformers

    Example:
        ```yaml
        # Just configure the transformation - dependencies set by drawing connections!
        type: dagster_component_templates.DataFrameTransformerComponent
        attributes:
          asset_name: cleaned_data
          drop_duplicates: true
          filter_columns: "id,name,amount,date"
          fill_na_value: "0"
        ```
    """

    asset_name: str = Field(
        description="Name of this asset"
    )

    # Column operations
    filter_columns: Optional[str] = Field(
        default=None,
        description="Comma-separated list of columns to keep"
    )

    drop_columns: Optional[str] = Field(
        default=None,
        description="Comma-separated list of columns to drop"
    )

    rename_columns: Optional[str] = Field(
        default=None,
        description="JSON mapping of column renames: '{\"old_name\": \"new_name\"}'"
    )

    # Row operations
    drop_duplicates: bool = Field(
        default=False,
        description="Whether to drop duplicate rows"
    )

    drop_na: bool = Field(
        default=False,
        description="Whether to drop rows with NA values"
    )

    fill_na_value: Optional[str] = Field(
        default=None,
        description="Value to fill NA values with"
    )

    # Filtering
    filter_expression: Optional[str] = Field(
        default=None,
        description="Pandas query expression (e.g., 'amount > 100 and status == \"active\"')"
    )

    # Sorting
    sort_by: Optional[str] = Field(
        default=None,
        description="Comma-separated columns to sort by"
    )

    sort_ascending: bool = Field(
        default=True,
        description="Sort direction"
    )

    # Aggregation
    group_by: Optional[str] = Field(
        default=None,
        description="Comma-separated columns to group by"
    )

    agg_functions: Optional[str] = Field(
        default=None,
        description="JSON mapping of aggregations: '{\"amount\": \"sum\", \"id\": \"count\"}'"
    )

    # Multiple DataFrame handling
    combine_method: str = Field(
        default="concat",
        description="How to combine multiple DataFrames: 'concat', 'merge', or 'first'"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )

    # String operations
    string_operations: Optional[str] = Field(
        default=None,
        description='JSON list of string operations: [{"column": "name", "operation": "upper"}, {"column": "email", "operation": "trim"}]. Operations: upper, lower, trim, strip, title'
    )

    string_replace: Optional[str] = Field(
        default=None,
        description='JSON mapping of string replacements: {"column_name": {"old": "new", "pattern": "replacement"}}'
    )

    # Calculated columns
    calculated_columns: Optional[str] = Field(
        default=None,
        description='JSON mapping of calculated columns: {"new_col": "price * quantity", "full_name": "first_name + \' \' + last_name"}'
    )

    # Pivot/Unpivot operations
    pivot_config: Optional[str] = Field(
        default=None,
        description='JSON config for pivot: {"index": "date", "columns": "category", "values": "amount", "aggfunc": "sum"}'
    )

    unpivot_config: Optional[str] = Field(
        default=None,
        description='JSON config for unpivot/melt: {"id_vars": ["id", "name"], "value_vars": ["q1", "q2", "q3"], "var_name": "quarter", "value_name": "sales"}'
    )

    # Upstream asset keys for explicit data loading
    upstream_asset_keys: Optional[str] = Field(
        default=None,
        description='Comma-separated list of upstream asset keys to load data from (automatically set by custom lineage)'
    )

    # Sample metadata
    include_sample_metadata: bool = Field(
        default=False,
        description="Include sample data preview in metadata (first 5 rows as markdown table and interactive preview)"
    )

    # Field validators to handle Dagster Components auto-deserializing JSON strings
    @field_validator('rename_columns', 'agg_functions', 'string_operations', 'string_replace',
                     'calculated_columns', 'pivot_config', 'unpivot_config', mode='before')
    @classmethod
    def convert_dict_to_json_string(cls, v):
        """Convert dict to JSON string if needed.

        Dagster Components may auto-deserialize JSON strings in YAML to dicts,
        so we accept both and ensure they're converted to JSON strings.
        """
        if v is None:
            return None
        if isinstance(v, dict) or isinstance(v, list):
            return json.dumps(v)
        return v

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        filter_columns = self.filter_columns
        drop_columns = self.drop_columns
        rename_columns_str = self.rename_columns
        drop_duplicates_flag = self.drop_duplicates
        drop_na_flag = self.drop_na
        fill_na_value = self.fill_na_value
        filter_expression = self.filter_expression
        sort_by = self.sort_by
        sort_ascending = self.sort_ascending
        group_by = self.group_by
        agg_functions_str = self.agg_functions
        combine_method = self.combine_method
        string_operations_str = self.string_operations
        string_replace_str = self.string_replace
        calculated_columns_str = self.calculated_columns
        pivot_config_str = self.pivot_config
        unpivot_config_str = self.unpivot_config
        upstream_asset_keys_str = self.upstream_asset_keys
        description = self.description or "Transform DataFrames from upstream assets"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Parse upstream asset keys if provided
        upstream_keys = []
        if upstream_asset_keys_str:
            upstream_keys = [k.strip() for k in upstream_asset_keys_str.split(',')]

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def dataframe_transformer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that transforms DataFrames from upstream assets.

            Upstream DataFrames are automatically loaded by the IO manager
            and passed as keyword arguments.
            """

            # Load upstream assets based on configuration
            upstream_assets = {}

            # If upstream_asset_keys is configured, try to load assets explicitly
            if upstream_keys and hasattr(context, 'load_asset_value'):
                # Real execution context - load assets explicitly
                context.log.info(f"Loading {len(upstream_keys)} upstream asset(s) via context.load_asset_value()")
                for key in upstream_keys:
                    try:
                        # Convert string key to AssetKey object
                        asset_key = AssetKey(key)
                        value = context.load_asset_value(asset_key)
                        upstream_assets[key] = value
                        context.log.info(f"  - Loaded '{key}': {type(value).__name__}")
                    except Exception as e:
                        context.log.error(f"  - Failed to load '{key}': {e}")
                        raise
            else:
                # Preview/mock context or no upstream_keys - fall back to kwargs
                upstream_assets = {k: v for k, v in kwargs.items()}

            # Validate we have at least one upstream asset
            if not upstream_assets:
                raise ValueError(
                    f"DataFrame Transformer '{asset_name}' requires at least one upstream asset "
                    "that produces a DataFrame. Please connect a DataFrame-producing asset "
                    "in the visual editor, such as:\n"
                    "  - REST API Fetcher (with output_format: dataframe)\n"
                    "  - Database Query\n"
                    "  - CSV File Ingestion\n"
                    "  - Another DataFrame Transformer"
                )

            context.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

            # Validate all inputs are DataFrames
            non_dataframes = []
            dataframes = {}

            for key, value in upstream_assets.items():
                if isinstance(value, pd.DataFrame):
                    dataframes[key] = value
                    context.log.info(f"  - '{key}': DataFrame with {len(value)} rows, {len(value.columns)} columns")
                else:
                    non_dataframes.append((key, type(value).__name__))

            if non_dataframes:
                error_msg = (
                    f"DataFrame Transformer '{asset_name}' received non-DataFrame inputs:\n"
                )
                for key, type_name in non_dataframes:
                    error_msg += f"  - '{key}': {type_name}\n"
                error_msg += "\nThis component only accepts DataFrame inputs. Compatible assets:\n"
                error_msg += "  - REST API Fetcher (set output_format: dataframe)\n"
                error_msg += "  - Database Query (returns DataFrames by default)\n"
                error_msg += "  - CSV File Ingestion (returns DataFrames by default)\n"
                error_msg += "  - Other DataFrame Transformers\n"
                raise TypeError(error_msg)

            # Handle multiple DataFrames
            if len(dataframes) == 1:
                df = list(dataframes.values())[0]
                source_name = list(dataframes.keys())[0]
                context.log.info(f"Processing DataFrame from '{source_name}'")
            else:
                context.log.info(f"Combining {len(dataframes)} DataFrames using method: {combine_method}")

                if combine_method == "first":
                    # Just use the first DataFrame
                    df = list(dataframes.values())[0]
                    source_name = list(dataframes.keys())[0]
                    context.log.info(f"Using first DataFrame from '{source_name}'")

                elif combine_method == "concat":
                    # Concatenate all DataFrames vertically
                    df = pd.concat(dataframes.values(), ignore_index=True)
                    context.log.info(f"Concatenated into {len(df)} rows")

                elif combine_method == "merge":
                    # Merge DataFrames (assumes they have common columns)
                    df_list = list(dataframes.values())
                    df = df_list[0]
                    for next_df in df_list[1:]:
                        df = pd.merge(df, next_df, how='outer')
                    context.log.info(f"Merged into {len(df)} rows")

                else:
                    raise ValueError(f"Unknown combine_method: {combine_method}")

            original_rows = len(df)
            original_cols = len(df.columns)

            # Column dropping
            if drop_columns:
                cols = [c.strip() for c in drop_columns.split(',')]
                cols_to_drop = [c for c in cols if c in df.columns]
                df = df.drop(columns=cols_to_drop)
                context.log.info(f"Dropped {len(cols_to_drop)} columns")

            # Column renaming
            if rename_columns_str:
                try:
                    rename_map = json.loads(rename_columns_str)
                    df = df.rename(columns=rename_map)
                    context.log.info(f"Renamed {len(rename_map)} columns")
                except json.JSONDecodeError as e:
                    context.log.error(f"Invalid rename_columns JSON: {e}")

            # Drop duplicates
            if drop_duplicates_flag:
                before = len(df)
                df = df.drop_duplicates()
                context.log.info(f"Dropped {before - len(df)} duplicate rows")

            # Drop NA
            if drop_na_flag:
                before = len(df)
                df = df.dropna()
                context.log.info(f"Dropped {before - len(df)} rows with NA values")

            # Fill NA
            if fill_na_value is not None:
                df = df.fillna(fill_na_value)
                context.log.info(f"Filled NA values with: {fill_na_value}")

            # Filter expression
            if filter_expression:
                try:
                    before = len(df)
                    df = df.query(filter_expression)
                    context.log.info(f"Filter '{filter_expression}' kept {len(df)}/{before} rows")
                except Exception as e:
                    context.log.error(f"Filter expression failed: {e}")
                    raise

            # Sorting
            if sort_by:
                cols = [c.strip() for c in sort_by.split(',')]
                existing_cols = [c for c in cols if c in df.columns]
                if existing_cols:
                    df = df.sort_values(by=existing_cols, ascending=sort_ascending)
                    context.log.info(f"Sorted by: {existing_cols} ({'ascending' if sort_ascending else 'descending'})")

            # Aggregation
            if group_by and agg_functions_str:
                try:
                    group_cols = [c.strip() for c in group_by.split(',')]
                    agg_map = json.loads(agg_functions_str)

                    # Filter to existing columns
                    group_cols = [c for c in group_cols if c in df.columns]
                    agg_map = {k: v for k, v in agg_map.items() if k in df.columns}

                    if group_cols and agg_map:
                        before = len(df)
                        df = df.groupby(group_cols).agg(agg_map).reset_index()
                        context.log.info(f"Grouped by {group_cols}, aggregated {len(agg_map)} columns ({before} → {len(df)} rows)")
                except Exception as e:
                    context.log.error(f"Aggregation failed: {e}")
                    raise

            # String operations
            if string_operations_str:
                try:
                    operations = json.loads(string_operations_str)
                    for op in operations:
                        col = op.get('column')
                        operation = op.get('operation')

                        if col in df.columns:
                            if operation == 'upper':
                                df[col] = df[col].astype(str).str.upper()
                            elif operation == 'lower':
                                df[col] = df[col].astype(str).str.lower()
                            elif operation in ['trim', 'strip']:
                                df[col] = df[col].astype(str).str.strip()
                            elif operation == 'title':
                                df[col] = df[col].astype(str).str.title()

                            context.log.info(f"Applied '{operation}' to column '{col}'")
                except Exception as e:
                    context.log.error(f"String operations failed: {e}")
                    raise

            # String replace
            if string_replace_str:
                try:
                    replace_map = json.loads(string_replace_str)
                    for col, replacements in replace_map.items():
                        if col in df.columns:
                            for old_val, new_val in replacements.items():
                                df[col] = df[col].astype(str).str.replace(old_val, new_val, regex=False)
                                context.log.info(f"Replaced '{old_val}' with '{new_val}' in column '{col}'")
                except Exception as e:
                    context.log.error(f"String replace failed: {e}")
                    raise

            # Calculated columns
            if calculated_columns_str:
                try:
                    calc_cols = json.loads(calculated_columns_str)
                    for new_col, expression in calc_cols.items():
                        # Evaluate expression in context of DataFrame
                        df[new_col] = df.eval(expression)
                        context.log.info(f"Created calculated column '{new_col}' = '{expression}'")
                except Exception as e:
                    context.log.error(f"Calculated columns failed: {e}")
                    raise

            # Pivot
            if pivot_config_str:
                try:
                    pivot_cfg = json.loads(pivot_config_str)
                    df = df.pivot_table(
                        index=pivot_cfg.get('index'),
                        columns=pivot_cfg.get('columns'),
                        values=pivot_cfg.get('values'),
                        aggfunc=pivot_cfg.get('aggfunc', 'sum')
                    ).reset_index()
                    context.log.info(f"Pivoted DataFrame: {pivot_cfg}")
                except Exception as e:
                    context.log.error(f"Pivot failed: {e}")
                    raise

            # Unpivot (melt)
            if unpivot_config_str:
                try:
                    unpivot_cfg = json.loads(unpivot_config_str)
                    df = pd.melt(
                        df,
                        id_vars=unpivot_cfg.get('id_vars', []),
                        value_vars=unpivot_cfg.get('value_vars'),
                        var_name=unpivot_cfg.get('var_name', 'variable'),
                        value_name=unpivot_cfg.get('value_name', 'value')
                    )
                    context.log.info(f"Unpivoted DataFrame: {unpivot_cfg}")
                except Exception as e:
                    context.log.error(f"Unpivot failed: {e}")
                    raise

            # Column filtering (select final output columns) - applied LAST
            if filter_columns:
                cols = [c.strip() for c in filter_columns.split(',')]

                # Expand filter list to include renamed columns and calculated columns
                # Build the final list of columns to keep
                final_cols_to_keep = []

                # Add renamed versions of filter columns
                if rename_columns_str:
                    try:
                        rename_map = json.loads(rename_columns_str)
                        for col in cols:
                            # If this column was renamed, use the new name
                            if col in rename_map:
                                final_cols_to_keep.append(rename_map[col])
                            else:
                                final_cols_to_keep.append(col)
                    except json.JSONDecodeError:
                        final_cols_to_keep = cols
                else:
                    final_cols_to_keep = cols

                # Add calculated columns (they should always be kept)
                if calculated_columns_str:
                    try:
                        calc_cols = json.loads(calculated_columns_str)
                        for new_col in calc_cols.keys():
                            if new_col not in final_cols_to_keep:
                                final_cols_to_keep.append(new_col)
                    except json.JSONDecodeError:
                        pass

                missing = set(final_cols_to_keep) - set(df.columns)
                if missing:
                    context.log.warning(f"Columns not found: {missing}")
                existing = [c for c in final_cols_to_keep if c in df.columns]
                df = df[existing]
                context.log.info(f"Selected {len(existing)} output columns: {existing}")

            # Add metadata
            context.add_output_metadata({
                "upstream_assets": list(dataframes.keys()),
                "num_upstream": len(dataframes),
                "original_rows": original_rows,
                "original_columns": original_cols,
                "final_rows": len(df),
                "final_columns": len(df.columns),
                "columns": list(df.columns),
                "rows_removed": original_rows - len(df),
                "columns_removed": original_cols - len(df.columns),
            })

            context.log.info(
                f"Transformation complete: {original_rows} → {len(df)} rows, "
                f"{original_cols} → {len(df.columns)} columns"
            )

            # Return DataFrame - IO manager will handle persistence
            if include_sample and len(df) > 0:
                # Return with sample metadata
                return Output(
                    value=df,
                    metadata={
                        "row_count": len(df),
                        "columns": df.columns.tolist(),
                        "sample": MetadataValue.md(df.head().to_markdown()),
                        "preview": MetadataValue.dataframe(df.head())
                    }
                )
            else:
                return df

        return Definitions(assets=[dataframe_transformer_asset])
