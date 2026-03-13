#!/usr/bin/env python3
"""
Add additional dlt destinations and environment-aware configuration.
"""

import json
from pathlib import Path
from typing import Dict, Any

# Additional destination fields to add
ADDITIONAL_DESTINATIONS = {
    "clickhouse": {
        "clickhouse_host": {
            "type": "string",
            "required": False,
            "label": "Host",
            "description": "ClickHouse server host",
            "placeholder": "localhost",
            "condition": {"destination": "clickhouse"}
        },
        "clickhouse_port": {
            "type": "number",
            "required": False,
            "label": "Port",
            "description": "ClickHouse server port",
            "default": 9000,
            "placeholder": "9000",
            "condition": {"destination": "clickhouse"}
        },
        "clickhouse_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "ClickHouse database name",
            "placeholder": "default",
            "condition": {"destination": "clickhouse"}
        },
        "clickhouse_username": {
            "type": "string",
            "required": False,
            "label": "Username",
            "description": "ClickHouse username",
            "placeholder": "${CLICKHOUSE_USERNAME}",
            "condition": {"destination": "clickhouse"}
        },
        "clickhouse_password": {
            "type": "string",
            "required": False,
            "label": "Password",
            "description": "ClickHouse password",
            "placeholder": "${CLICKHOUSE_PASSWORD}",
            "secret": True,
            "condition": {"destination": "clickhouse"}
        }
    },
    "mssql": {
        "mssql_host": {
            "type": "string",
            "required": False,
            "label": "Host",
            "description": "SQL Server host",
            "placeholder": "localhost",
            "condition": {"destination": "mssql"}
        },
        "mssql_port": {
            "type": "number",
            "required": False,
            "label": "Port",
            "description": "SQL Server port",
            "default": 1433,
            "placeholder": "1433",
            "condition": {"destination": "mssql"}
        },
        "mssql_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "SQL Server database name",
            "placeholder": "analytics",
            "condition": {"destination": "mssql"}
        },
        "mssql_username": {
            "type": "string",
            "required": False,
            "label": "Username",
            "description": "SQL Server username",
            "placeholder": "${MSSQL_USERNAME}",
            "condition": {"destination": "mssql"}
        },
        "mssql_password": {
            "type": "string",
            "required": False,
            "label": "Password",
            "description": "SQL Server password",
            "placeholder": "${MSSQL_PASSWORD}",
            "secret": True,
            "condition": {"destination": "mssql"}
        }
    },
    "athena": {
        "athena_query_result_bucket": {
            "type": "string",
            "required": False,
            "label": "Query Result Bucket",
            "description": "S3 bucket for query results (s3://bucket-name)",
            "placeholder": "s3://my-athena-results",
            "condition": {"destination": "athena"}
        },
        "athena_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "Athena database name",
            "placeholder": "default",
            "condition": {"destination": "athena"}
        },
        "athena_aws_access_key_id": {
            "type": "string",
            "required": False,
            "label": "AWS Access Key ID",
            "description": "AWS access key ID",
            "placeholder": "${AWS_ACCESS_KEY_ID}",
            "condition": {"destination": "athena"}
        },
        "athena_aws_secret_access_key": {
            "type": "string",
            "required": False,
            "label": "AWS Secret Access Key",
            "description": "AWS secret access key",
            "placeholder": "${AWS_SECRET_ACCESS_KEY}",
            "secret": True,
            "condition": {"destination": "athena"}
        },
        "athena_region": {
            "type": "string",
            "required": False,
            "label": "AWS Region",
            "description": "AWS region",
            "default": "us-east-1",
            "placeholder": "us-east-1",
            "condition": {"destination": "athena"}
        }
    },
    "mysql": {
        "mysql_host": {
            "type": "string",
            "required": False,
            "label": "Host",
            "description": "MySQL server host",
            "placeholder": "localhost",
            "condition": {"destination": "mysql"}
        },
        "mysql_port": {
            "type": "number",
            "required": False,
            "label": "Port",
            "description": "MySQL server port",
            "default": 3306,
            "placeholder": "3306",
            "condition": {"destination": "mysql"}
        },
        "mysql_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "MySQL database name",
            "placeholder": "analytics",
            "condition": {"destination": "mysql"}
        },
        "mysql_username": {
            "type": "string",
            "required": False,
            "label": "Username",
            "description": "MySQL username",
            "placeholder": "${MYSQL_USERNAME}",
            "condition": {"destination": "mysql"}
        },
        "mysql_password": {
            "type": "string",
            "required": False,
            "label": "Password",
            "description": "MySQL password",
            "placeholder": "${MYSQL_PASSWORD}",
            "secret": True,
            "condition": {"destination": "mysql"}
        }
    },
    "filesystem": {
        "filesystem_bucket_path": {
            "type": "string",
            "required": False,
            "label": "Bucket Path",
            "description": "Local or S3 path (e.g., /path/to/dir or s3://bucket/path)",
            "placeholder": "/tmp/dlt_data",
            "condition": {"destination": "filesystem"}
        },
        "filesystem_format": {
            "type": "string",
            "required": False,
            "label": "File Format",
            "description": "Output file format",
            "default": "parquet",
            "enum": ["parquet", "jsonl", "csv"],
            "condition": {"destination": "filesystem"}
        }
    },
    "synapse": {
        "synapse_host": {
            "type": "string",
            "required": False,
            "label": "Host",
            "description": "Azure Synapse workspace hostname",
            "placeholder": "myworkspace.sql.azuresynapse.net",
            "condition": {"destination": "synapse"}
        },
        "synapse_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "Synapse database name",
            "placeholder": "analytics",
            "condition": {"destination": "synapse"}
        },
        "synapse_username": {
            "type": "string",
            "required": False,
            "label": "Username",
            "description": "Synapse username",
            "placeholder": "${SYNAPSE_USERNAME}",
            "condition": {"destination": "synapse"}
        },
        "synapse_password": {
            "type": "string",
            "required": False,
            "label": "Password",
            "description": "Synapse password",
            "placeholder": "${SYNAPSE_PASSWORD}",
            "secret": True,
            "condition": {"destination": "synapse"}
        }
    }
}

# Environment-aware destination fields
ENVIRONMENT_AWARE_FIELDS = {
    "use_environment_routing": {
        "type": "boolean",
        "required": False,
        "label": "Use Environment-Based Routing",
        "description": "Automatically select destination based on Dagster deployment environment",
        "default": False
    },
    "destination_local": {
        "type": "string",
        "required": False,
        "label": "Local Destination",
        "description": "Destination for local development (when not in Dagster Cloud)",
        "enum": ["", "duckdb", "postgres", "filesystem"],
        "condition": {"use_environment_routing": True}
    },
    "destination_branch": {
        "type": "string",
        "required": False,
        "label": "Branch Deployment Destination",
        "description": "Destination for branch deployments (DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT=true)",
        "enum": ["", "duckdb", "snowflake", "bigquery", "postgres", "redshift", "databricks"],
        "condition": {"use_environment_routing": True}
    },
    "destination_prod": {
        "type": "string",
        "required": False,
        "label": "Production Destination",
        "description": "Destination for production deployment (full deployment, not branch)",
        "enum": ["", "snowflake", "bigquery", "postgres", "redshift", "databricks", "synapse"],
        "condition": {"use_environment_routing": True}
    }
}

def update_schema_with_new_destinations(schema_path: Path) -> bool:
    """Add new destinations and environment routing to schema."""
    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)

        attributes = schema.get('attributes', {})

        # Update destination enum
        if 'destination' in attributes:
            current_enum = attributes['destination'].get('enum', [])
            new_destinations = ["clickhouse", "mssql", "athena", "mysql", "filesystem", "synapse"]

            # Add new destinations if not already present
            updated = False
            for dest in new_destinations:
                if dest not in current_enum:
                    current_enum.append(dest)
                    updated = True

            if updated:
                attributes['destination']['enum'] = current_enum

                # Update enumLabels
                enum_labels = attributes['destination'].get('enumLabels', {})
                enum_labels.update({
                    "clickhouse": "ClickHouse",
                    "mssql": "Microsoft SQL Server",
                    "athena": "AWS Athena",
                    "mysql": "MySQL",
                    "filesystem": "Filesystem (Parquet/CSV)",
                    "synapse": "Azure Synapse"
                })
                attributes['destination']['enumLabels'] = enum_labels

        # Find where to insert new fields (after persist_and_return)
        keys = list(attributes.keys())
        if 'persist_and_return' in keys:
            insert_idx = keys.index('persist_and_return') + 1
        else:
            insert_idx = len(keys)

        # Create new attributes dict with fields inserted
        new_attributes = {}

        # Add fields before insertion point
        for key in keys[:insert_idx]:
            new_attributes[key] = attributes[key]

        # Add new destination fields
        for dest_name, fields in ADDITIONAL_DESTINATIONS.items():
            for field_name, field_config in fields.items():
                new_attributes[field_name] = field_config

        # Add environment-aware fields
        for field_name, field_config in ENVIRONMENT_AWARE_FIELDS.items():
            new_attributes[field_name] = field_config

        # Add remaining fields
        for key in keys[insert_idx:]:
            new_attributes[key] = attributes[key]

        schema['attributes'] = new_attributes

        # Write back
        with open(schema_path, 'w') as f:
            json.dump(schema, f, indent=2)
            f.write('\n')

        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def main():
    """Update all ingestion component schemas."""
    assets_dir = Path("/Users/ericthomas/dagster_components/dagster-component-templates/assets")

    ingestion_schemas = []
    for schema_path in assets_dir.glob("*_ingestion/schema.json"):
        if schema_path.parent.name != "csv_file_ingestion":
            ingestion_schemas.append(schema_path)

    print(f"Found {len(ingestion_schemas)} dlt ingestion component schemas\n")

    updated_count = 0
    for schema_path in sorted(ingestion_schemas):
        component_name = schema_path.parent.name
        print(f"Processing {component_name}...")
        if update_schema_with_new_destinations(schema_path):
            updated_count += 1
            print(f"  ✅ Updated")
        print()

    print(f"\n✅ Updated {updated_count}/{len(ingestion_schemas)} schemas")

if __name__ == "__main__":
    main()
