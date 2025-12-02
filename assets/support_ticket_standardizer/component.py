"""Support Ticket Standardizer Component.

Transform platform-specific support ticket data (Zendesk, Freshdesk, Intercom) into a
standardized common schema for cross-platform support analysis.
"""

from typing import Optional, Literal
import pandas as pd
import numpy as np
from dagster import (
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
    Output,
    MetadataValue,
)
from pydantic import Field


class SupportTicketStandardizerComponent(Component, Model, Resolvable):
    """Component for standardizing support ticket data across platforms.

    Transforms platform-specific schemas (Zendesk, Freshdesk, Intercom) into a
    unified support ticket data model with consistent field names and metrics.

    Standard Schema Output:
    - ticket_id: Ticket identifier
    - ticket_number: Human-readable ticket number
    - platform: Source platform (zendesk, freshdesk, intercom)
    - subject: Ticket subject
    - description: Ticket description/body
    - status: Ticket status (open, pending, resolved, closed)
    - priority: Priority level
    - type: Ticket type/category
    - requester_email: Requester email
    - requester_name: Requester name
    - requester_id: Requester identifier
    - assignee_email: Assignee email
    - assignee_name: Assignee name
    - assignee_id: Assignee identifier
    - group_name: Support group name
    - group_id: Support group identifier
    - created_date: Creation timestamp
    - updated_date: Last updated timestamp
    - closed_date: Closed/resolved timestamp
    - first_response_time: Time to first response (hours)
    - resolution_time: Time to resolution (hours)
    - reopens: Number of times ticket was reopened
    - tags: Tags/labels (JSON array)
    - satisfaction_rating: Customer satisfaction score
    - channel: Communication channel (email, chat, phone)

    Example:
        ```yaml
        type: dagster_component_templates.SupportTicketStandardizerComponent
        attributes:
          asset_name: standardized_support_tickets
          platform: "zendesk"
          source_asset: "zendesk_tickets"
        ```
    """

    asset_name: str = Field(
        description="Name of the standardized output asset"
    )

    platform: Literal["zendesk", "freshdesk", "intercom"] = Field(
        description="Source support platform to standardize"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Upstream asset containing raw platform data (automatically set via lineage)"
    )

    ticket_id_field: Optional[str] = Field(
        default=None,
        description="Field name for ticket ID (auto-detected if not provided)"
    )

    status_field: Optional[str] = Field(
        default=None,
        description="Field name for status (auto-detected if not provided)"
    )

    # Optional filters
    filter_status: Optional[str] = Field(
        default=None,
        description="Filter by ticket status (comma-separated)"
    )

    filter_priority: Optional[str] = Field(
        default=None,
        description="Filter by priority level (comma-separated)"
    )

    filter_date_from: Optional[str] = Field(
        default=None,
        description="Filter tickets from this date (YYYY-MM-DD)"
    )

    filter_date_to: Optional[str] = Field(
        default=None,
        description="Filter tickets to this date (YYYY-MM-DD)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="support",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        platform = self.platform
        source_asset = self.source_asset
        ticket_id_field = self.ticket_id_field
        status_field = self.status_field
        filter_status = self.filter_status
        filter_priority = self.filter_priority
        filter_date_from = self.filter_date_from
        filter_date_to = self.filter_date_to
        description = self.description or f"Standardized {platform} support ticket data"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Parse upstream asset keys
        upstream_keys = []
        if source_asset:
            upstream_keys = [source_asset]

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def support_ticket_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that standardizes platform-specific support ticket data."""

            context.log.info(f"Standardizing {platform} support ticket data")

            # Load upstream data
            if upstream_keys and hasattr(context, 'load_asset_value'):
                context.log.info(f"Loading data from upstream asset: {source_asset}")
                raw_data = context.load_asset_value(AssetKey(source_asset))
            elif kwargs:
                raw_data = list(kwargs.values())[0]
            else:
                raise ValueError(
                    f"Support Ticket Standardizer '{asset_name}' requires upstream data. "
                    f"Connect to a support platform ingestion component (Zendesk, Freshdesk, etc.)"
                )

            # Convert to DataFrame if needed
            if isinstance(raw_data, dict):
                if 'data' in raw_data:
                    df = pd.DataFrame(raw_data['data'])
                elif 'rows' in raw_data:
                    df = pd.DataFrame(raw_data['rows'])
                else:
                    df = pd.DataFrame([raw_data])
            elif isinstance(raw_data, pd.DataFrame):
                df = raw_data
            else:
                raise TypeError(f"Unexpected data type: {type(raw_data)}")

            context.log.info(f"Raw data: {len(df)} rows, {len(df.columns)} columns")
            original_rows = len(df)

            # Platform-specific field mappings
            field_mappings = {
                "zendesk": {
                    "ticket_id": ["id", "ticket_id"],
                    "ticket_number": ["id"],
                    "subject": ["subject"],
                    "description": ["description", "body"],
                    "status": ["status"],
                    "priority": ["priority"],
                    "type": ["type"],
                    "requester_email": ["requester.email", "requester_email"],
                    "requester_name": ["requester.name", "requester_name"],
                    "requester_id": ["requester.id", "requester_id"],
                    "assignee_email": ["assignee.email", "assignee_email"],
                    "assignee_name": ["assignee.name", "assignee_name"],
                    "assignee_id": ["assignee.id", "assignee_id"],
                    "group_name": ["group.name", "group_name"],
                    "group_id": ["group.id", "group_id"],
                    "created_date": ["created_at"],
                    "updated_date": ["updated_at"],
                    "closed_date": ["solved_at", "closed_at"],
                    "tags": ["tags"],
                    "satisfaction_rating": ["satisfaction_rating.score"],
                    "channel": ["via.channel", "channel"],
                },
                "freshdesk": {
                    "ticket_id": ["id", "ticket_id"],
                    "ticket_number": ["id"],
                    "subject": ["subject"],
                    "description": ["description", "description_text"],
                    "status": ["status"],
                    "priority": ["priority"],
                    "type": ["type"],
                    "requester_email": ["requester.email"],
                    "requester_name": ["requester.name"],
                    "requester_id": ["requester.id", "requester_id"],
                    "assignee_email": ["responder.email"],
                    "assignee_name": ["responder.name"],
                    "assignee_id": ["responder.id", "responder_id"],
                    "group_name": ["group.name"],
                    "group_id": ["group.id", "group_id"],
                    "created_date": ["created_at"],
                    "updated_date": ["updated_at"],
                    "closed_date": ["resolved_at", "closed_at"],
                    "tags": ["tags"],
                    "satisfaction_rating": ["satisfaction_rating.rating"],
                    "channel": ["source", "channel"],
                },
                "intercom": {
                    "ticket_id": ["id", "conversation_id"],
                    "ticket_number": ["id"],
                    "subject": ["subject", "title"],
                    "description": ["body", "message"],
                    "status": ["state", "status"],
                    "priority": ["priority"],
                    "type": ["type"],
                    "requester_email": ["user.email", "customer.email"],
                    "requester_name": ["user.name", "customer.name"],
                    "requester_id": ["user.id", "user_id"],
                    "assignee_email": ["assignee.email"],
                    "assignee_name": ["assignee.name"],
                    "assignee_id": ["assignee.id", "assignee_id"],
                    "group_name": ["team.name"],
                    "group_id": ["team.id", "team_id"],
                    "created_date": ["created_at"],
                    "updated_date": ["updated_at"],
                    "closed_date": ["closed_at"],
                    "tags": ["tags"],
                    "satisfaction_rating": ["rating.rating"],
                    "channel": ["source.type"],
                },
            }

            mapping = field_mappings.get(platform)
            if not mapping:
                raise ValueError(f"Unsupported platform: {platform}")

            # Helper function to find field in DataFrame
            def find_field(possible_names, custom_field=None):
                if custom_field and custom_field in df.columns:
                    return custom_field
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            # Build standardized DataFrame
            standardized_data = {}

            # Platform identifier
            standardized_data['platform'] = platform

            # Ticket ID
            ticket_id_col = find_field(mapping['ticket_id'], ticket_id_field)
            if ticket_id_col:
                standardized_data['ticket_id'] = df[ticket_id_col].astype(str)

            # Ticket number
            ticket_number_col = find_field(mapping.get('ticket_number', []))
            if ticket_number_col:
                standardized_data['ticket_number'] = df[ticket_number_col].astype(str)

            # Subject
            subject_col = find_field(mapping.get('subject', []))
            if subject_col:
                standardized_data['subject'] = df[subject_col]

            # Description
            description_col = find_field(mapping.get('description', []))
            if description_col:
                standardized_data['description'] = df[description_col]

            # Status
            status_col = find_field(mapping['status'], status_field)
            if status_col:
                standardized_data['status'] = df[status_col]

            # Priority
            priority_col = find_field(mapping.get('priority', []))
            if priority_col:
                standardized_data['priority'] = df[priority_col]

            # Type
            type_col = find_field(mapping.get('type', []))
            if type_col:
                standardized_data['type'] = df[type_col]

            # Requester fields
            requester_email_col = find_field(mapping.get('requester_email', []))
            if requester_email_col:
                standardized_data['requester_email'] = df[requester_email_col]

            requester_name_col = find_field(mapping.get('requester_name', []))
            if requester_name_col:
                standardized_data['requester_name'] = df[requester_name_col]

            requester_id_col = find_field(mapping.get('requester_id', []))
            if requester_id_col:
                standardized_data['requester_id'] = df[requester_id_col].astype(str)

            # Assignee fields
            assignee_email_col = find_field(mapping.get('assignee_email', []))
            if assignee_email_col:
                standardized_data['assignee_email'] = df[assignee_email_col]

            assignee_name_col = find_field(mapping.get('assignee_name', []))
            if assignee_name_col:
                standardized_data['assignee_name'] = df[assignee_name_col]

            assignee_id_col = find_field(mapping.get('assignee_id', []))
            if assignee_id_col:
                standardized_data['assignee_id'] = df[assignee_id_col].astype(str)

            # Group fields
            group_name_col = find_field(mapping.get('group_name', []))
            if group_name_col:
                standardized_data['group_name'] = df[group_name_col]

            group_id_col = find_field(mapping.get('group_id', []))
            if group_id_col:
                standardized_data['group_id'] = df[group_id_col].astype(str)

            # Date fields
            created_col = find_field(mapping.get('created_date', []))
            if created_col:
                standardized_data['created_date'] = pd.to_datetime(df[created_col], errors='coerce')

            updated_col = find_field(mapping.get('updated_date', []))
            if updated_col:
                standardized_data['updated_date'] = pd.to_datetime(df[updated_col], errors='coerce')

            closed_col = find_field(mapping.get('closed_date', []))
            if closed_col:
                standardized_data['closed_date'] = pd.to_datetime(df[closed_col], errors='coerce')

            # Tags
            tags_col = find_field(mapping.get('tags', []))
            if tags_col:
                standardized_data['tags'] = df[tags_col]

            # Satisfaction rating
            satisfaction_col = find_field(mapping.get('satisfaction_rating', []))
            if satisfaction_col:
                standardized_data['satisfaction_rating'] = df[satisfaction_col]

            # Channel
            channel_col = find_field(mapping.get('channel', []))
            if channel_col:
                standardized_data['channel'] = df[channel_col]

            # Create standardized DataFrame
            std_df = pd.DataFrame(standardized_data)

            # Calculate derived metrics (time-based)
            if 'created_date' in std_df.columns and 'closed_date' in std_df.columns:
                time_diff = std_df['closed_date'] - std_df['created_date']
                std_df['resolution_time'] = (time_diff.dt.total_seconds() / 3600).round(2)

            # Placeholder for first_response_time and reopens (would need additional data)
            std_df['first_response_time'] = np.nan
            std_df['reopens'] = 0

            # Apply filters
            if filter_status and 'status' in std_df.columns:
                statuses = [s.strip() for s in filter_status.split(',')]
                std_df = std_df[std_df['status'].isin(statuses)]
                context.log.info(f"Filtered to statuses: {statuses}")

            if filter_priority and 'priority' in std_df.columns:
                priorities = [p.strip() for p in filter_priority.split(',')]
                std_df = std_df[std_df['priority'].isin(priorities)]
                context.log.info(f"Filtered to priorities: {priorities}")

            if filter_date_from and 'created_date' in std_df.columns:
                std_df = std_df[std_df['created_date'] >= pd.to_datetime(filter_date_from)]
                context.log.info(f"Filtered from date: {filter_date_from}")

            if filter_date_to and 'created_date' in std_df.columns:
                std_df = std_df[std_df['created_date'] <= pd.to_datetime(filter_date_to)]
                context.log.info(f"Filtered to date: {filter_date_to}")

            # Replace inf and -inf with NaN
            std_df = std_df.replace([float('inf'), float('-inf')], pd.NA)

            final_rows = len(std_df)
            context.log.info(
                f"Standardization complete: {original_rows} → {final_rows} rows, "
                f"{len(std_df.columns)} columns"
            )

            # Add metadata
            metadata = {
                "platform": platform,
                "original_rows": original_rows,
                "final_rows": final_rows,
                "columns": list(std_df.columns),
            }

            # Calculate average resolution time if available
            if 'resolution_time' in std_df.columns:
                avg_resolution = std_df['resolution_time'].mean()
                if not pd.isna(avg_resolution):
                    metadata["avg_resolution_time_hours"] = float(avg_resolution)

            context.add_output_metadata(metadata)

            # Return DataFrame
            if include_sample and len(std_df) > 0:
                return Output(
                    value=std_df,
                    metadata={
                        "row_count": len(std_df),
                        "columns": std_df.columns.tolist(),
                        "sample": MetadataValue.md(std_df.head(10).to_markdown()),
                        "preview": MetadataValue.dataframe(std_df.head(10))
                    }
                )
            else:
                return std_df

        return Definitions(assets=[support_ticket_standardizer_asset])
