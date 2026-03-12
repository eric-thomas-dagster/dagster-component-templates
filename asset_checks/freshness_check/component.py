"""Freshness Policy Component.

Attaches a Dagster-native FreshnessPolicy to an asset, enabling continuous
SLA monitoring without manual check execution.

Two policy types are supported:
  - time_window: Asset must be materialized within a rolling time window
  - cron: Asset must be materialized before each cron deadline

Dagster evaluates the policy automatically and raises alerts when the SLA
is missed — no sensor or manual trigger required.

Docs: https://docs.dagster.io/guides/observe/asset-freshness-policies
"""
from typing import Optional, Literal
import dagster as dg
from pydantic import Field, model_validator


class FreshnessPolicyComponent(dg.Component, dg.Model, dg.Resolvable):
    """Attach a Dagster FreshnessPolicy to an asset for continuous SLA monitoring.

    Supports two policy types:

    **time_window** — asset must materialize within a rolling window:
        ```yaml
        type: dagster_component_templates.FreshnessPolicyComponent
        attributes:
          asset_key: warehouse/orders
          policy_type: time_window
          fail_window_hours: 25
          warn_window_hours: 13
        ```

    **cron** — asset must materialize before each cron deadline:
        ```yaml
        type: dagster_component_templates.FreshnessPolicyComponent
        attributes:
          asset_key: warehouse/orders
          policy_type: cron
          deadline_cron: "0 10 * * *"
          lower_bound_delta_hours: 1
          timezone: America/New_York
        ```
    """

    asset_key: str = Field(description="Asset key to attach the freshness policy to (e.g. 'warehouse/orders')")
    policy_type: Literal["time_window", "cron"] = Field(
        default="time_window",
        description="time_window: rolling window SLA. cron: deadline-based SLA.",
    )

    # --- time_window fields ---
    fail_window_hours: Optional[float] = Field(
        default=None,
        description="[time_window] Hours after which the asset is considered stale and the check fails. Must exceed 1 minute.",
    )
    warn_window_hours: Optional[float] = Field(
        default=None,
        description="[time_window] Hours after which a warning is raised. Must be shorter than fail_window_hours.",
    )

    # --- cron fields ---
    deadline_cron: Optional[str] = Field(
        default=None,
        description="[cron] Cron expression for the materialization deadline (e.g. '0 10 * * *').",
    )
    lower_bound_delta_hours: Optional[float] = Field(
        default=None,
        description="[cron] How many hours before the deadline the materialization window opens.",
    )
    timezone: Optional[str] = Field(
        default=None,
        description="[cron] Timezone for the cron expression (e.g. 'America/New_York'). Defaults to UTC.",
    )

    @model_validator(mode="after")
    def validate_policy_fields(self):
        if self.policy_type == "time_window":
            if self.fail_window_hours is None:
                raise ValueError("fail_window_hours is required for policy_type='time_window'")
        else:
            if not self.deadline_cron:
                raise ValueError("deadline_cron is required for policy_type='cron'")
            if self.lower_bound_delta_hours is None:
                raise ValueError("lower_bound_delta_hours is required for policy_type='cron'")
        return self

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from datetime import timedelta
        from dagster import FreshnessPolicy

        asset_key = dg.AssetKey(self.asset_key.split("/"))

        if self.policy_type == "time_window":
            policy = FreshnessPolicy.time_window(
                fail_window=timedelta(hours=self.fail_window_hours),
                warn_window=timedelta(hours=self.warn_window_hours) if self.warn_window_hours else None,
            )
        else:
            kwargs = {
                "deadline_cron": self.deadline_cron,
                "lower_bound_delta": timedelta(hours=self.lower_bound_delta_hours),
            }
            if self.timezone:
                kwargs["timezone"] = self.timezone
            policy = FreshnessPolicy.cron(**kwargs)

        spec = dg.AssetSpec(
            key=asset_key,
            freshness_policy=policy,
        )
        return dg.Definitions(assets=[spec])
