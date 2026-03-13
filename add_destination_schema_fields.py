#!/usr/bin/env python3
"""
Add destination-specific configuration fields to all dlt ingestion component schemas.
"""

import json
from pathlib import Path
from typing import Dict, Any

# Define destination-specific configuration fields
DESTINATION_FIELDS = {
    "snowflake": {
        "snowflake_account": {
            "type": "string",
            "required": False,
            "label": "Snowflake Account",
            "description": "Snowflake account identifier (e.g., abc123.us-east-1)",
            "placeholder": "myaccount.us-east-1",
            "condition": {"destination": "snowflake"}
        },
        "snowflake_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "Snowflake database name",
            "placeholder": "analytics",
            "condition": {"destination": "snowflake"}
        },
        "snowflake_schema": {
            "type": "string",
            "required": False,
            "label": "Schema",
            "description": "Snowflake schema name",
            "default": "public",
            "placeholder": "raw_data",
            "condition": {"destination": "snowflake"}
        },
        "snowflake_warehouse": {
            "type": "string",
            "required": False,
            "label": "Warehouse",
            "description": "Snowflake warehouse name",
            "placeholder": "transforming",
            "condition": {"destination": "snowflake"}
        },
        "snowflake_username": {
            "type": "string",
            "required": False,
            "label": "Username",
            "description": "Snowflake username",
            "placeholder": "${SNOWFLAKE_USERNAME}",
            "condition": {"destination": "snowflake"}
        },
        "snowflake_password": {
            "type": "string",
            "required": False,
            "label": "Password",
            "description": "Snowflake password",
            "placeholder": "${SNOWFLAKE_PASSWORD}",
            "secret": True,
            "condition": {"destination": "snowflake"}
        },
        "snowflake_role": {
            "type": "string",
            "required": False,
            "label": "Role",
            "description": "Snowflake role (optional)",
            "placeholder": "dlt_role",
            "condition": {"destination": "snowflake"}
        }
    },
    "bigquery": {
        "bigquery_project_id": {
            "type": "string",
            "required": False,
            "label": "Project ID",
            "description": "Google Cloud project ID",
            "placeholder": "my-project-123",
            "condition": {"destination": "bigquery"}
        },
        "bigquery_dataset": {
            "type": "string",
            "required": False,
            "label": "Dataset",
            "description": "BigQuery dataset name",
            "placeholder": "raw_data",
            "condition": {"destination": "bigquery"}
        },
        "bigquery_credentials_path": {
            "type": "string",
            "required": False,
            "label": "Credentials Path",
            "description": "Path to service account JSON file",
            "placeholder": "/path/to/service-account.json",
            "condition": {"destination": "bigquery"}
        },
        "bigquery_location": {
            "type": "string",
            "required": False,
            "label": "Location",
            "description": "BigQuery dataset location (optional)",
            "default": "US",
            "placeholder": "US",
            "condition": {"destination": "bigquery"}
        }
    },
    "postgres": {
        "postgres_host": {
            "type": "string",
            "required": False,
            "label": "Host",
            "description": "PostgreSQL host",
            "placeholder": "localhost",
            "condition": {"destination": "postgres"}
        },
        "postgres_port": {
            "type": "number",
            "required": False,
            "label": "Port",
            "description": "PostgreSQL port",
            "default": 5432,
            "placeholder": "5432",
            "condition": {"destination": "postgres"}
        },
        "postgres_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "PostgreSQL database name",
            "placeholder": "analytics",
            "condition": {"destination": "postgres"}
        },
        "postgres_username": {
            "type": "string",
            "required": False,
            "label": "Username",
            "description": "PostgreSQL username",
            "placeholder": "${POSTGRES_USERNAME}",
            "condition": {"destination": "postgres"}
        },
        "postgres_password": {
            "type": "string",
            "required": False,
            "label": "Password",
            "description": "PostgreSQL password",
            "placeholder": "${POSTGRES_PASSWORD}",
            "secret": True,
            "condition": {"destination": "postgres"}
        },
        "postgres_schema": {
            "type": "string",
            "required": False,
            "label": "Schema",
            "description": "PostgreSQL schema (optional)",
            "default": "public",
            "placeholder": "public",
            "condition": {"destination": "postgres"}
        }
    },
    "redshift": {
        "redshift_host": {
            "type": "string",
            "required": False,
            "label": "Host",
            "description": "Redshift cluster endpoint",
            "placeholder": "cluster.region.redshift.amazonaws.com",
            "condition": {"destination": "redshift"}
        },
        "redshift_port": {
            "type": "number",
            "required": False,
            "label": "Port",
            "description": "Redshift port",
            "default": 5439,
            "placeholder": "5439",
            "condition": {"destination": "redshift"}
        },
        "redshift_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "Redshift database name",
            "placeholder": "analytics",
            "condition": {"destination": "redshift"}
        },
        "redshift_username": {
            "type": "string",
            "required": False,
            "label": "Username",
            "description": "Redshift username",
            "placeholder": "${REDSHIFT_USERNAME}",
            "condition": {"destination": "redshift"}
        },
        "redshift_password": {
            "type": "string",
            "required": False,
            "label": "Password",
            "description": "Redshift password",
            "placeholder": "${REDSHIFT_PASSWORD}",
            "secret": True,
            "condition": {"destination": "redshift"}
        },
        "redshift_schema": {
            "type": "string",
            "required": False,
            "label": "Schema",
            "description": "Redshift schema (optional)",
            "default": "public",
            "placeholder": "public",
            "condition": {"destination": "redshift"}
        }
    },
    "duckdb": {
        "duckdb_database_path": {
            "type": "string",
            "required": False,
            "label": "Database Path",
            "description": "Path to DuckDB file (use ':memory:' for in-memory)",
            "placeholder": "/path/to/analytics.duckdb",
            "condition": {"destination": "duckdb"}
        }
    },
    "motherduck": {
        "motherduck_database": {
            "type": "string",
            "required": False,
            "label": "Database",
            "description": "MotherDuck database name",
            "placeholder": "my_database",
            "condition": {"destination": "motherduck"}
        },
        "motherduck_token": {
            "type": "string",
            "required": False,
            "label": "Token",
            "description": "MotherDuck authentication token",
            "placeholder": "${MOTHERDUCK_TOKEN}",
            "secret": True,
            "condition": {"destination": "motherduck"}
        }
    },
    "databricks": {
        "databricks_server_hostname": {
            "type": "string",
            "required": False,
            "label": "Server Hostname",
            "description": "Databricks workspace hostname",
            "placeholder": "dbc-a1b2c3d4-e5f6.cloud.databricks.com",
            "condition": {"destination": "databricks"}
        },
        "databricks_http_path": {
            "type": "string",
            "required": False,
            "label": "HTTP Path",
            "description": "SQL warehouse HTTP path",
            "placeholder": "/sql/1.0/warehouses/abc123",
            "condition": {"destination": "databricks"}
        },
        "databricks_access_token": {
            "type": "string",
            "required": False,
            "label": "Access Token",
            "description": "Databricks personal access token",
            "placeholder": "${DATABRICKS_TOKEN}",
            "secret": True,
            "condition": {"destination": "databricks"}
        },
        "databricks_catalog": {
            "type": "string",
            "required": False,
            "label": "Catalog",
            "description": "Unity Catalog name (optional)",
            "placeholder": "main",
            "condition": {"destination": "databricks"}
        },
        "databricks_schema": {
            "type": "string",
            "required": False,
            "label": "Schema",
            "description": "Databricks schema name",
            "default": "default",
            "placeholder": "raw_data",
            "condition": {"destination": "databricks"}
        }
    }
}

def add_destination_fields_to_schema(schema_path: Path) -> bool:
    """Add destination fields to a component schema."""
    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)

        # Check if already has destination field
        if 'destination' in schema.get('attributes', {}):
            print(f"  ⚠️  Already has destination fields, skipping")
            return False

        attributes = schema.get('attributes', {})

        # Find insert position (after include_sample_metadata)
        keys = list(attributes.keys())
        if 'include_sample_metadata' in keys:
            insert_idx = keys.index('include_sample_metadata') + 1
        else:
            insert_idx = len(keys)

        # Create new attributes dict with destination fields inserted
        new_attributes = {}

        # Add fields before destination
        for i, key in enumerate(keys[:insert_idx]):
            new_attributes[key] = attributes[key]

        # Add destination enum
        new_attributes['destination'] = {
            "type": "string",
            "required": False,
            "label": "Destination",
            "description": "Database destination for persisting data (leave empty for DataFrame-only mode)",
            "enum": ["", "snowflake", "bigquery", "postgres", "redshift", "duckdb", "motherduck", "databricks"],
            "enumLabels": {
                "": "None (DataFrame only)",
                "snowflake": "Snowflake",
                "bigquery": "Google BigQuery",
                "postgres": "PostgreSQL",
                "redshift": "Amazon Redshift",
                "duckdb": "DuckDB (local file)",
                "motherduck": "MotherDuck (cloud DuckDB)",
                "databricks": "Databricks"
            }
        }

        # Add all destination-specific fields
        for dest_name, fields in DESTINATION_FIELDS.items():
            for field_name, field_config in fields.items():
                new_attributes[field_name] = field_config

        # Add persist_and_return
        new_attributes['persist_and_return'] = {
            "type": "boolean",
            "required": False,
            "label": "Persist and Return DataFrame",
            "description": "When destination is set: persist to database AND return DataFrame (vs. persist only)",
            "default": False
        }

        # Add remaining fields after destination
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
    """Add destination fields to all ingestion component schemas."""
    assets_dir = Path("/Users/ericthomas/dagster_components/dagster-component-templates/assets")

    # Find all ingestion schemas (excluding csv_file_ingestion)
    ingestion_schemas = []
    for schema_path in assets_dir.glob("*_ingestion/schema.json"):
        if schema_path.parent.name != "csv_file_ingestion":
            ingestion_schemas.append(schema_path)

    print(f"Found {len(ingestion_schemas)} dlt ingestion component schemas\n")

    updated_count = 0
    for schema_path in sorted(ingestion_schemas):
        component_name = schema_path.parent.name
        print(f"Processing {component_name}...")
        if add_destination_fields_to_schema(schema_path):
            updated_count += 1
            print(f"  ✅ Updated")
        print()

    print(f"\n✅ Updated {updated_count}/{len(ingestion_schemas)} schemas")

if __name__ == "__main__":
    main()
