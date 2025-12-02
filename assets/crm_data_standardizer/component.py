"""CRM Data Standardizer Component.

Transform platform-specific CRM data (HubSpot, Salesforce, Pipedrive) into a
standardized common schema for cross-platform CRM analysis.
"""

from typing import Optional, Literal
import pandas as pd
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


class CRMDataStandardizerComponent(Component, Model, Resolvable):
    """Component for standardizing CRM data across platforms.

    Transforms platform-specific schemas (HubSpot, Salesforce, Pipedrive) into a
    unified CRM data model with consistent field names and structure.

    Standard Schema Output:
    - record_id: Record identifier
    - record_type: Type (contact, company, deal, activity)
    - platform: Source platform (hubspot, salesforce, pipedrive)
    - name: Name/title
    - email: Email (for contacts)
    - phone: Phone number
    - company_name: Company/organization name
    - owner: Owner/assigned user
    - status: Status/stage
    - created_date: Creation date
    - modified_date: Last modified date
    - value: Deal value/revenue (for deals)
    - close_date: Expected/actual close date (for deals)
    - source: Lead source
    - tags: Tags/categories (JSON array)

    Example:
        ```yaml
        type: dagster_component_templates.CRMDataStandardizerComponent
        attributes:
          asset_name: standardized_crm_contacts
          platform: "hubspot"
          resource_type: "contacts"
          source_asset: "hubspot_contacts"
        ```
    """

    asset_name: str = Field(
        description="Name of the standardized output asset"
    )

    platform: Literal["hubspot", "salesforce", "pipedrive"] = Field(
        description="Source CRM platform to standardize"
    )

    resource_type: Literal["contacts", "companies", "deals", "activities"] = Field(
        description="Type of CRM resource to standardize"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Upstream asset containing raw platform data (automatically set via lineage)"
    )

    record_id_field: Optional[str] = Field(
        default=None,
        description="Field name for record ID (auto-detected if not provided)"
    )

    name_field: Optional[str] = Field(
        default=None,
        description="Field name for name/title (auto-detected if not provided)"
    )

    email_field: Optional[str] = Field(
        default=None,
        description="Field name for email (auto-detected if not provided)"
    )

    # Optional filters
    filter_status: Optional[str] = Field(
        default=None,
        description="Filter by status/stage (comma-separated)"
    )

    filter_owner: Optional[str] = Field(
        default=None,
        description="Filter by owner name or ID"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="crm",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        platform = self.platform
        resource_type = self.resource_type
        source_asset = self.source_asset
        record_id_field = self.record_id_field
        name_field = self.name_field
        email_field = self.email_field
        filter_status = self.filter_status
        filter_owner = self.filter_owner
        description = self.description or f"Standardized {platform} {resource_type} data"
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
        def crm_standardizer_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that standardizes platform-specific CRM data."""

            context.log.info(f"Standardizing {platform} {resource_type} data")

            # Load upstream data
            if upstream_keys and hasattr(context, 'load_asset_value'):
                context.log.info(f"Loading data from upstream asset: {source_asset}")
                raw_data = context.load_asset_value(AssetKey(source_asset))
            elif kwargs:
                raw_data = list(kwargs.values())[0]
            else:
                raise ValueError(
                    f"CRM Standardizer '{asset_name}' requires upstream data. "
                    f"Connect to a CRM ingestion component (HubSpot, Salesforce, etc.)"
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

            # Platform-specific field mappings per resource type
            field_mappings = {
                "hubspot": {
                    "contacts": {
                        "record_id": ["id", "vid", "canonical-vid"],
                        "name": ["firstname", "lastname", "fullname"],
                        "email": ["email"],
                        "phone": ["phone", "mobilephone"],
                        "company_name": ["company", "associatedcompanyid"],
                        "owner": ["hubspot_owner_id", "hs_owner_id"],
                        "status": ["lifecyclestage", "hs_lead_status"],
                        "created_date": ["createdate", "hs_createdate"],
                        "modified_date": ["lastmodifieddate", "hs_lastmodifieddate"],
                        "source": ["hs_analytics_source", "hs_latest_source"],
                    },
                    "companies": {
                        "record_id": ["id", "companyId"],
                        "name": ["name", "company"],
                        "owner": ["hubspot_owner_id", "hs_owner_id"],
                        "status": ["lifecyclestage"],
                        "created_date": ["createdate", "hs_createdate"],
                        "modified_date": ["hs_lastmodifieddate"],
                        "value": ["annualrevenue"],
                    },
                    "deals": {
                        "record_id": ["id", "dealId"],
                        "name": ["dealname", "name"],
                        "company_name": ["associatedcompanyids"],
                        "owner": ["hubspot_owner_id", "hs_owner_id"],
                        "status": ["dealstage", "pipeline"],
                        "created_date": ["createdate", "hs_createdate"],
                        "modified_date": ["hs_lastmodifieddate"],
                        "value": ["amount", "dealamount"],
                        "close_date": ["closedate", "hs_closedate"],
                    },
                    "activities": {
                        "record_id": ["id", "engagement_id"],
                        "name": ["type", "engagement_type"],
                        "created_date": ["timestamp", "created_at"],
                        "owner": ["owner_id", "ownerId"],
                    },
                },
                "salesforce": {
                    "contacts": {
                        "record_id": ["Id", "ContactId"],
                        "name": ["FirstName", "LastName", "Name"],
                        "email": ["Email"],
                        "phone": ["Phone", "MobilePhone"],
                        "company_name": ["AccountId", "Account.Name"],
                        "owner": ["OwnerId", "Owner.Name"],
                        "status": ["LeadSource", "Status"],
                        "created_date": ["CreatedDate"],
                        "modified_date": ["LastModifiedDate"],
                        "source": ["LeadSource"],
                    },
                    "companies": {
                        "record_id": ["Id", "AccountId"],
                        "name": ["Name"],
                        "owner": ["OwnerId", "Owner.Name"],
                        "status": ["Type"],
                        "created_date": ["CreatedDate"],
                        "modified_date": ["LastModifiedDate"],
                        "value": ["AnnualRevenue"],
                    },
                    "deals": {
                        "record_id": ["Id", "OpportunityId"],
                        "name": ["Name"],
                        "company_name": ["AccountId", "Account.Name"],
                        "owner": ["OwnerId", "Owner.Name"],
                        "status": ["StageName"],
                        "created_date": ["CreatedDate"],
                        "modified_date": ["LastModifiedDate"],
                        "value": ["Amount"],
                        "close_date": ["CloseDate"],
                    },
                    "activities": {
                        "record_id": ["Id"],
                        "name": ["Type", "Subject"],
                        "created_date": ["CreatedDate"],
                        "owner": ["OwnerId"],
                    },
                },
                "pipedrive": {
                    "contacts": {
                        "record_id": ["id"],
                        "name": ["name", "first_name", "last_name"],
                        "email": ["email", "primary_email"],
                        "phone": ["phone", "primary_phone"],
                        "company_name": ["org_id", "org_name"],
                        "owner": ["owner_id", "owner_name"],
                        "created_date": ["add_time"],
                        "modified_date": ["update_time"],
                    },
                    "companies": {
                        "record_id": ["id"],
                        "name": ["name"],
                        "owner": ["owner_id", "owner_name"],
                        "created_date": ["add_time"],
                        "modified_date": ["update_time"],
                    },
                    "deals": {
                        "record_id": ["id"],
                        "name": ["title"],
                        "company_name": ["org_id", "org_name"],
                        "owner": ["user_id", "owner_name"],
                        "status": ["stage_id", "status"],
                        "created_date": ["add_time"],
                        "modified_date": ["update_time"],
                        "value": ["value"],
                        "close_date": ["expected_close_date", "close_time"],
                    },
                    "activities": {
                        "record_id": ["id"],
                        "name": ["type", "subject"],
                        "created_date": ["add_time"],
                        "owner": ["user_id"],
                    },
                },
            }

            mapping = field_mappings.get(platform, {}).get(resource_type)
            if not mapping:
                raise ValueError(f"Unsupported platform/resource: {platform}/{resource_type}")

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

            # Platform and record type identifiers
            standardized_data['platform'] = platform
            standardized_data['record_type'] = resource_type

            # Record ID
            record_id_col = find_field(mapping['record_id'], record_id_field)
            if record_id_col:
                standardized_data['record_id'] = df[record_id_col].astype(str)

            # Name/Title
            name_col = find_field(mapping.get('name', []), name_field)
            if name_col:
                standardized_data['name'] = df[name_col]
            # For contacts, combine first and last name if needed
            elif resource_type == "contacts" and platform == "hubspot":
                first = find_field(["firstname"])
                last = find_field(["lastname"])
                if first and last:
                    standardized_data['name'] = df[first] + " " + df[last]

            # Email
            email_col = find_field(mapping.get('email', []), email_field)
            if email_col:
                standardized_data['email'] = df[email_col]

            # Phone
            phone_col = find_field(mapping.get('phone', []))
            if phone_col:
                standardized_data['phone'] = df[phone_col]

            # Company name
            company_col = find_field(mapping.get('company_name', []))
            if company_col:
                standardized_data['company_name'] = df[company_col]

            # Owner
            owner_col = find_field(mapping.get('owner', []))
            if owner_col:
                standardized_data['owner'] = df[owner_col].astype(str)

            # Status
            status_col = find_field(mapping.get('status', []))
            if status_col:
                standardized_data['status'] = df[status_col]

            # Created date
            created_col = find_field(mapping.get('created_date', []))
            if created_col:
                standardized_data['created_date'] = pd.to_datetime(df[created_col], errors='coerce')

            # Modified date
            modified_col = find_field(mapping.get('modified_date', []))
            if modified_col:
                standardized_data['modified_date'] = pd.to_datetime(df[modified_col], errors='coerce')

            # Value (for deals/companies)
            value_col = find_field(mapping.get('value', []))
            if value_col:
                standardized_data['value'] = pd.to_numeric(df[value_col], errors='coerce')

            # Close date (for deals)
            close_col = find_field(mapping.get('close_date', []))
            if close_col:
                standardized_data['close_date'] = pd.to_datetime(df[close_col], errors='coerce')

            # Source
            source_col = find_field(mapping.get('source', []))
            if source_col:
                standardized_data['source'] = df[source_col]

            # Tags (placeholder - would need JSON parsing)
            tags_col = find_field(mapping.get('tags', []))
            if tags_col:
                standardized_data['tags'] = df[tags_col]

            # Create standardized DataFrame
            std_df = pd.DataFrame(standardized_data)

            # Apply filters
            if filter_status and 'status' in std_df.columns:
                statuses = [s.strip() for s in filter_status.split(',')]
                std_df = std_df[std_df['status'].isin(statuses)]
                context.log.info(f"Filtered to statuses: {statuses}")

            if filter_owner and 'owner' in std_df.columns:
                std_df = std_df[std_df['owner'] == filter_owner]
                context.log.info(f"Filtered to owner: {filter_owner}")

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
                "resource_type": resource_type,
                "original_rows": original_rows,
                "final_rows": final_rows,
                "columns": list(std_df.columns),
            }

            # Add resource-specific metadata
            if resource_type == "deals" and 'value' in std_df.columns:
                metadata["total_value"] = float(std_df['value'].sum())

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

        return Definitions(assets=[crm_standardizer_asset])
