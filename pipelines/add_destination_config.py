#!/usr/bin/env python3
"""
Add comprehensive destination-specific configuration parameters.
These appear conditionally based on the selected output_destination.
"""

import json

# Destination-specific configuration parameters
DESTINATION_PARAMS = {
    "snowflake": {
        "snowflake_account": {
            "type": "string",
            "default": "",
            "description": "Snowflake account identifier (e.g., xy12345.us-east-1)",
            "placeholder": "xy12345.us-east-1",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "snowflake"}
        },
        "snowflake_user": {
            "type": "string",
            "default": "",
            "description": "Snowflake username",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "snowflake"}
        },
        "snowflake_password": {
            "type": "string",
            "default": "",
            "description": "Snowflake password",
            "required": True,
            "sensitive": True,
            "show_if": {"output_destination": "snowflake"}
        },
        "snowflake_database": {
            "type": "string",
            "default": "",
            "description": "Snowflake database name",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "snowflake"}
        },
        "snowflake_schema": {
            "type": "string",
            "default": "public",
            "description": "Snowflake schema name",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "snowflake"}
        },
        "snowflake_warehouse": {
            "type": "string",
            "default": "COMPUTE_WH",
            "description": "Snowflake warehouse name",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "snowflake"}
        }
    },
    "bigquery": {
        "bigquery_project_id": {
            "type": "string",
            "default": "",
            "description": "Google Cloud project ID",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "bigquery"}
        },
        "bigquery_dataset": {
            "type": "string",
            "default": "",
            "description": "BigQuery dataset name",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "bigquery"}
        },
        "bigquery_credentials_json": {
            "type": "string",
            "default": "",
            "description": "Service account credentials JSON (or path to file)",
            "required": True,
            "sensitive": True,
            "show_if": {"output_destination": "bigquery"}
        }
    },
    "postgres": {
        "postgres_host": {
            "type": "string",
            "default": "localhost",
            "description": "PostgreSQL host",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "postgres"}
        },
        "postgres_port": {
            "type": "integer",
            "default": 5432,
            "description": "PostgreSQL port",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "postgres"}
        },
        "postgres_database": {
            "type": "string",
            "default": "",
            "description": "PostgreSQL database name",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "postgres"}
        },
        "postgres_user": {
            "type": "string",
            "default": "",
            "description": "PostgreSQL username",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "postgres"}
        },
        "postgres_password": {
            "type": "string",
            "default": "",
            "description": "PostgreSQL password",
            "required": True,
            "sensitive": True,
            "show_if": {"output_destination": "postgres"}
        },
        "postgres_schema": {
            "type": "string",
            "default": "public",
            "description": "PostgreSQL schema",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "postgres"}
        }
    },
    "redshift": {
        "redshift_host": {
            "type": "string",
            "default": "",
            "description": "Redshift cluster endpoint",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "redshift"}
        },
        "redshift_port": {
            "type": "integer",
            "default": 5439,
            "description": "Redshift port",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "redshift"}
        },
        "redshift_database": {
            "type": "string",
            "default": "",
            "description": "Redshift database name",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "redshift"}
        },
        "redshift_user": {
            "type": "string",
            "default": "",
            "description": "Redshift username",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "redshift"}
        },
        "redshift_password": {
            "type": "string",
            "default": "",
            "description": "Redshift password",
            "required": True,
            "sensitive": True,
            "show_if": {"output_destination": "redshift"}
        },
        "redshift_schema": {
            "type": "string",
            "default": "public",
            "description": "Redshift schema",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "redshift"}
        }
    },
    "databricks": {
        "databricks_host": {
            "type": "string",
            "default": "",
            "description": "Databricks workspace URL",
            "placeholder": "https://dbc-12345678-90ab.cloud.databricks.com",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "databricks"}
        },
        "databricks_token": {
            "type": "string",
            "default": "",
            "description": "Databricks personal access token",
            "required": True,
            "sensitive": True,
            "show_if": {"output_destination": "databricks"}
        },
        "databricks_http_path": {
            "type": "string",
            "default": "",
            "description": "HTTP path for SQL warehouse",
            "placeholder": "/sql/1.0/warehouses/abc123def456",
            "required": True,
            "sensitive": False,
            "show_if": {"output_destination": "databricks"}
        },
        "databricks_catalog": {
            "type": "string",
            "default": "main",
            "description": "Databricks catalog (Unity Catalog)",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "databricks"}
        },
        "databricks_schema": {
            "type": "string",
            "default": "default",
            "description": "Databricks schema",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "databricks"}
        }
    },
    "motherduck": {
        "motherduck_token": {
            "type": "string",
            "default": "",
            "description": "MotherDuck service token",
            "required": True,
            "sensitive": True,
            "show_if": {"output_destination": "motherduck"}
        },
        "motherduck_database": {
            "type": "string",
            "default": "my_db",
            "description": "MotherDuck database name",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "motherduck"}
        }
    },
    "duckdb": {
        "duckdb_path": {
            "type": "string",
            "default": ":memory:",
            "description": "DuckDB file path (:memory: for in-memory database)",
            "required": False,
            "sensitive": False,
            "show_if": {"output_destination": "duckdb"}
        }
    }
}

def add_destination_params():
    """Add destination-specific parameters to all pipelines"""
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)

    for pipeline in manifest['pipelines']:
        params = pipeline['pipeline_params']

        # Remove old snowflake_account and database_connection_string if present
        params.pop('snowflake_account', None)
        params.pop('database_connection_string', None)

        # Add destination-specific params for all destinations
        for destination, dest_params in DESTINATION_PARAMS.items():
            for param_name, param_config in dest_params.items():
                # Only add if not already present
                if param_name not in params:
                    params[param_name] = param_config.copy()

        print(f"✓ {pipeline['id']}: {len(params)} total params")

    # Save updated manifest
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    print("\n✅ Added conditional destination parameters:")
    print("  - Snowflake: account, user, password, database, schema, warehouse")
    print("  - BigQuery: project_id, dataset, credentials")
    print("  - Postgres: host, port, database, user, password, schema")
    print("  - Redshift: host, port, database, user, password, schema")
    print("  - Databricks: host, token, http_path, catalog, schema")
    print("  - MotherDuck: token, database")
    print("  - DuckDB: file path")

if __name__ == '__main__':
    add_destination_params()
