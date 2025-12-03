"""A/B Test Analysis Component.

Perform statistical analysis of A/B tests including significance testing, confidence intervals,
and recommendations for test conclusions.
"""

from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


class ABTestAnalysisComponent(Component, Model, Resolvable):
    """Component for A/B test statistical analysis.

    Performs comprehensive A/B test analysis including:
    - Conversion rate comparison
    - Statistical significance (Z-test)
    - Confidence intervals
    - Minimum Detectable Effect (MDE)
    - Sample size validation
    - Test recommendations (continue, stop, winner)

    Example:
        ```yaml
        type: dagster_component_templates.ABTestAnalysisComponent
        attributes:
          asset_name: ab_test_results
          source_asset: experiment_data
          confidence_level: 0.95
          minimum_sample_size: 1000
          description: "A/B test statistical analysis"
          group_name: experimentation
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Source asset with experiment data (set via lineage)"
    )

    confidence_level: float = Field(
        default=0.95,
        description="Confidence level for significance testing (default: 0.95 for 95%)"
    )

    minimum_sample_size: int = Field(
        default=1000,
        description="Minimum sample size per variant to consider test valid"
    )

    minimum_runtime_hours: Optional[int] = Field(
        default=None,
        description="Minimum test runtime required (hours, optional)"
    )

    minimum_detectable_effect: float = Field(
        default=0.05,
        description="Minimum detectable effect (MDE) as decimal (e.g., 0.05 = 5%)"
    )

    experiment_id_field: Optional[str] = Field(
        default=None,
        description="Experiment ID column (auto-detected)"
    )

    variant_field: Optional[str] = Field(
        default=None,
        description="Variant column (control/treatment, A/B, etc.)"
    )

    user_id_field: Optional[str] = Field(
        default=None,
        description="User ID column (auto-detected)"
    )

    converted_field: Optional[str] = Field(
        default=None,
        description="Conversion indicator column (auto-detected)"
    )

    timestamp_field: Optional[str] = Field(
        default=None,
        description="Timestamp column for runtime validation (optional)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="experimentation",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        confidence_level = self.confidence_level
        min_sample_size = self.minimum_sample_size
        min_runtime_hours = self.minimum_runtime_hours
        min_detectable_effect = self.minimum_detectable_effect
        experiment_id_field = self.experiment_id_field
        variant_field = self.variant_field
        user_id_field = self.user_id_field
        converted_field = self.converted_field
        timestamp_field = self.timestamp_field
        description = self.description or "A/B test statistical analysis"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Set up dependencies
        upstream_keys = []
        if source_asset:
            upstream_keys.append(source_asset)

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def ab_test_analysis_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that performs A/B test analysis."""

            # Load upstream data
            upstream_data = {}
            if upstream_keys and hasattr(context, 'load_asset_value'):
                for key in upstream_keys:
                    try:
                        value = context.load_asset_value(AssetKey(key))
                        upstream_data[key] = value
                        context.log.info(f"Loaded {len(value)} rows from {key}")
                    except Exception as e:
                        context.log.warning(f"Could not load {key}: {e}")
            else:
                upstream_data = kwargs

            if not upstream_data:
                context.log.warning("No upstream data available")
                return pd.DataFrame()

            # Get the source DataFrame
            df = list(upstream_data.values())[0]
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} experiment records")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            experiment_col = find_column(
                ['experiment_id', 'test_id', 'experimentId', 'test_name'],
                experiment_id_field
            )
            variant_col = find_column(
                ['variant', 'group', 'variation', 'test_group', 'cohort'],
                variant_field
            )
            user_col = find_column(
                ['user_id', 'customer_id', 'userId', 'customerId'],
                user_id_field
            )
            converted_col = find_column(
                ['converted', 'conversion', 'success', 'converted_flag'],
                converted_field
            )
            timestamp_col = find_column(
                ['timestamp', 'date', 'created_at', 'enrolled_at'],
                timestamp_field
            )

            # Validate required columns
            missing = []
            if not experiment_col:
                missing.append("experiment_id")
            if not variant_col:
                missing.append("variant")
            if not user_col:
                missing.append("user_id")
            if not converted_col:
                missing.append("converted")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            context.log.info(f"Using columns - Experiment: {experiment_col}, Variant: {variant_col}, User: {user_col}, Converted: {converted_col}")

            # Prepare data
            cols_to_use = [experiment_col, variant_col, user_col, converted_col]
            col_names = ['experiment_id', 'variant', 'user_id', 'converted']

            if timestamp_col:
                cols_to_use.append(timestamp_col)
                col_names.append('timestamp')

            exp_df = df[cols_to_use].copy()
            exp_df.columns = col_names

            # Convert converted to boolean
            exp_df['converted'] = exp_df['converted'].astype(bool)

            # Deduplicate users (keep first exposure)
            exp_df = exp_df.drop_duplicates(subset=['experiment_id', 'user_id'], keep='first')

            context.log.info(f"Analyzing {exp_df['experiment_id'].nunique()} experiments")

            # Analyze each experiment
            results = []

            for experiment_id, exp_data in exp_df.groupby('experiment_id'):
                context.log.info(f"\nAnalyzing experiment: {experiment_id}")

                # Get unique variants
                variants = exp_data['variant'].unique()

                if len(variants) < 2:
                    context.log.warning(f"  Experiment {experiment_id} has only 1 variant, skipping")
                    continue

                # Identify control and treatment(s)
                # Assume control is named: control, Control, A, baseline
                control_candidates = ['control', 'Control', 'CONTROL', 'A', 'baseline', 'Baseline']
                control_variant = None
                for candidate in control_candidates:
                    if candidate in variants:
                        control_variant = candidate
                        break

                if not control_variant:
                    # Default to first variant alphabetically
                    control_variant = sorted(variants)[0]
                    context.log.info(f"  No explicit control found, using '{control_variant}' as control")

                treatment_variants = [v for v in variants if v != control_variant]

                # Calculate metrics for control
                control_data = exp_data[exp_data['variant'] == control_variant]
                control_n = len(control_data)
                control_conversions = control_data['converted'].sum()
                control_rate = control_conversions / control_n if control_n > 0 else 0

                context.log.info(f"  Control ({control_variant}): {control_n} users, {control_conversions} conversions, {control_rate*100:.2f}% rate")

                # Analyze each treatment against control
                for treatment_variant in treatment_variants:
                    treatment_data = exp_data[exp_data['variant'] == treatment_variant]
                    treatment_n = len(treatment_data)
                    treatment_conversions = treatment_data['converted'].sum()
                    treatment_rate = treatment_conversions / treatment_n if treatment_n > 0 else 0

                    context.log.info(f"  Treatment ({treatment_variant}): {treatment_n} users, {treatment_conversions} conversions, {treatment_rate*100:.2f}% rate")

                    # Check sample size
                    sample_size_valid = (control_n >= min_sample_size) and (treatment_n >= min_sample_size)

                    # Check runtime if timestamp available
                    runtime_valid = True
                    runtime_hours = None
                    if 'timestamp' in exp_data.columns and min_runtime_hours:
                        exp_data['timestamp'] = pd.to_datetime(exp_data['timestamp'], errors='coerce')
                        if exp_data['timestamp'].notna().any():
                            min_ts = exp_data['timestamp'].min()
                            max_ts = exp_data['timestamp'].max()
                            runtime_hours = (max_ts - min_ts).total_seconds() / 3600
                            runtime_valid = runtime_hours >= min_runtime_hours

                    # Calculate statistical significance (Z-test for proportions)
                    pooled_rate = (control_conversions + treatment_conversions) / (control_n + treatment_n)
                    se_pooled = np.sqrt(pooled_rate * (1 - pooled_rate) * (1/control_n + 1/treatment_n))

                    if se_pooled > 0:
                        z_score = (treatment_rate - control_rate) / se_pooled
                        # Two-tailed test
                        # For 95% confidence, critical z = 1.96
                        critical_z = 1.96 if confidence_level == 0.95 else (
                            2.576 if confidence_level == 0.99 else 1.645  # 90%
                        )
                        is_significant = abs(z_score) >= critical_z
                        p_value = 2 * (1 - (0.5 * (1 + np.sign(z_score) * np.sqrt(1 - np.exp(-z_score**2 * 2/np.pi)))))  # Approximation
                    else:
                        z_score = 0
                        p_value = 1.0
                        is_significant = False

                    # Calculate lift
                    if control_rate > 0:
                        lift_pct = ((treatment_rate - control_rate) / control_rate * 100)
                    else:
                        lift_pct = 0

                    # Confidence interval for treatment rate
                    se_treatment = np.sqrt(treatment_rate * (1 - treatment_rate) / treatment_n) if treatment_n > 0 else 0
                    ci_margin = critical_z * se_treatment
                    ci_lower = max(0, treatment_rate - ci_margin)
                    ci_upper = min(1, treatment_rate + ci_margin)

                    # Determine recommendation
                    if not sample_size_valid:
                        recommendation = "Continue (insufficient sample size)"
                        winner = None
                    elif not runtime_valid:
                        recommendation = "Continue (insufficient runtime)"
                        winner = None
                    elif is_significant:
                        if lift_pct >= min_detectable_effect * 100:
                            recommendation = "Implement treatment (statistically significant improvement)"
                            winner = treatment_variant
                        elif lift_pct <= -min_detectable_effect * 100:
                            recommendation = "Keep control (statistically significant decline)"
                            winner = control_variant
                        else:
                            recommendation = "No practical difference (below MDE)"
                            winner = None
                    else:
                        recommendation = "No significant difference (continue or stop)"
                        winner = None

                    results.append({
                        'experiment_id': experiment_id,
                        'control_variant': control_variant,
                        'treatment_variant': treatment_variant,
                        'control_n': control_n,
                        'treatment_n': treatment_n,
                        'control_conversions': control_conversions,
                        'treatment_conversions': treatment_conversions,
                        'control_rate': round(control_rate * 100, 2),
                        'treatment_rate': round(treatment_rate * 100, 2),
                        'lift_pct': round(lift_pct, 2),
                        'ci_lower': round(ci_lower * 100, 2),
                        'ci_upper': round(ci_upper * 100, 2),
                        'z_score': round(z_score, 3),
                        'p_value': round(p_value, 4),
                        'is_significant': is_significant,
                        'sample_size_valid': sample_size_valid,
                        'runtime_hours': round(runtime_hours, 2) if runtime_hours else None,
                        'runtime_valid': runtime_valid,
                        'recommendation': recommendation,
                        'winner': winner
                    })

            if not results:
                context.log.warning("No valid experiment results to analyze")
                return pd.DataFrame()

            result_df = pd.DataFrame(results)

            context.log.info(f"\nA/B test analysis complete: {len(result_df)} experiments analyzed")

            # Log summary
            significant_tests = result_df['is_significant'].sum()
            context.log.info(f"  Statistically significant results: {significant_tests}/{len(result_df)}")

            # Add metadata
            metadata = {
                "row_count": len(result_df),
                "total_experiments": len(result_df),
                "significant_results": int(significant_tests),
                "confidence_level": confidence_level,
                "minimum_sample_size": min_sample_size,
                "minimum_detectable_effect": min_detectable_effect,
            }

            # Return with metadata
            if include_sample and len(result_df) > 0:
                return Output(
                    value=result_df,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(result_df.to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(result_df)
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return result_df

        return Definitions(assets=[ab_test_analysis_asset])
