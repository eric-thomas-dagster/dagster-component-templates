"""Conversation Memory Asset Component.

Manage conversation history and context for multi-turn LLM interactions.
Accepts a DataFrame with message columns and updates conversation memory for each row.
"""

import os
import json
from typing import Optional
import pandas as pd

from dagster import (
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


class ConversationMemoryComponent(Component, Model, Resolvable):
    """Component for managing conversation memory.

    Accepts a DataFrame with user and/or assistant message columns, appends each row's
    messages to the conversation memory file, and returns the DataFrame enriched with
    the current memory state.

    Example:
        ```yaml
        type: dagster_component_templates.ConversationMemoryComponent
        attributes:
          asset_name: conversation_state
          upstream_asset_key: chat_messages
          memory_file: /path/to/memory.json
          user_message_column: user_message
          assistant_message_column: assistant_message
          max_messages: 10
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame with message columns")
    memory_file: str = Field(description="Path to memory file")
    user_message_column: str = Field(default="user_message", description="Column name containing user messages")
    assistant_message_column: str = Field(default="assistant_message", description="Column name containing assistant messages")
    max_messages: int = Field(default=10, description="Max messages to keep")
    include_system_prompt: bool = Field(default=True, description="Include system prompt")
    system_prompt: str = Field(default="You are a helpful assistant.", description="System prompt")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        memory_file = self.memory_file
        user_message_column = self.user_message_column
        assistant_message_column = self.assistant_message_column
        max_messages = self.max_messages
        include_system_prompt = self.include_system_prompt
        system_prompt = self.system_prompt
        description = self.description or "Manage conversation memory"
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
            group_name=group_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
        )
        def conversation_memory_asset(ctx: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            """Manage conversation memory from upstream DataFrame messages."""

            df = upstream.copy()

            # Load existing memory
            if os.path.exists(memory_file):
                with open(memory_file, 'r') as f:
                    memory = json.load(f)
            else:
                memory = {"messages": []}

            # Add system prompt if needed
            if include_system_prompt and (not memory["messages"] or memory["messages"][0].get("role") != "system"):
                memory["messages"].insert(0, {"role": "system", "content": system_prompt})

            # Process each row's messages into the memory
            for idx, row in df.iterrows():
                user_message = None
                assistant_message = None

                if user_message_column in df.columns:
                    val = row.get(user_message_column)
                    if pd.notna(val) and str(val).strip():
                        user_message = str(val)

                if assistant_message_column in df.columns:
                    val = row.get(assistant_message_column)
                    if pd.notna(val) and str(val).strip():
                        assistant_message = str(val)

                if user_message:
                    memory["messages"].append({"role": "user", "content": user_message})
                    ctx.log.info(f"Row {idx}: Added user message: {user_message[:50]}...")

                if assistant_message:
                    memory["messages"].append({"role": "assistant", "content": assistant_message})
                    ctx.log.info(f"Row {idx}: Added assistant message: {assistant_message[:50]}...")

            # Trim to max messages (keep system prompt)
            if len(memory["messages"]) > max_messages + 1:
                system_msg = memory["messages"][0] if memory["messages"][0].get("role") == "system" else None
                memory["messages"] = memory["messages"][-(max_messages):]
                if system_msg and memory["messages"][0].get("role") != "system":
                    memory["messages"].insert(0, system_msg)

            # Save memory
            os.makedirs(os.path.dirname(os.path.abspath(memory_file)), exist_ok=True)
            with open(memory_file, 'w') as f:
                json.dump(memory, f, indent=2)

            ctx.log.info(f"Memory updated: {len(memory['messages'])} messages")

            # Enrich DataFrame with current memory state
            df["memory_message_count"] = len(memory["messages"])
            df["memory_file"] = memory_file

            ctx.add_output_metadata({
                "num_messages": len(memory["messages"]),
                "rows_processed": len(df),
            })

            return df

        return Definitions(assets=[conversation_memory_asset])
