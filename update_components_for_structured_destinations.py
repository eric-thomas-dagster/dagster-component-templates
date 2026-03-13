#!/usr/bin/env python3
"""
Update component.py files to use structured destination fields.
"""

import re
from pathlib import Path

# Template for destination config building method
DESTINATION_CONFIG_METHOD = '''
    def _build_destination_config(self) -> dict:
        """Build dlt destination config from structured fields."""
        if not self.destination:
            return {}

        if self.destination == "snowflake":
            return {
                "credentials": {
                    "database": self.snowflake_database,
                    "username": self.snowflake_username,
                    "password": self.snowflake_password,
                    "host": self.snowflake_account,
                    "warehouse": self.snowflake_warehouse,
                    "role": self.snowflake_role if self.snowflake_role else None,
                }
            }
        elif self.destination == "bigquery":
            config = {
                "project_id": self.bigquery_project_id,
                "dataset": self.bigquery_dataset,
            }
            if self.bigquery_credentials_path:
                config["credentials"] = self.bigquery_credentials_path
            if self.bigquery_location:
                config["location"] = self.bigquery_location
            return config
        elif self.destination == "postgres":
            return {
                "credentials": {
                    "database": self.postgres_database,
                    "username": self.postgres_username,
                    "password": self.postgres_password,
                    "host": self.postgres_host,
                    "port": self.postgres_port,
                }
            }
        elif self.destination == "redshift":
            return {
                "credentials": {
                    "database": self.redshift_database,
                    "username": self.redshift_username,
                    "password": self.redshift_password,
                    "host": self.redshift_host,
                    "port": self.redshift_port,
                }
            }
        elif self.destination == "duckdb":
            return {
                "credentials": self.duckdb_database_path if self.duckdb_database_path else ":memory:"
            }
        elif self.destination == "motherduck":
            return {
                "credentials": {
                    "database": self.motherduck_database,
                    "token": self.motherduck_token,
                }
            }
        elif self.destination == "databricks":
            config = {
                "credentials": {
                    "server_hostname": self.databricks_server_hostname,
                    "http_path": self.databricks_http_path,
                    "access_token": self.databricks_access_token,
                }
            }
            if self.databricks_catalog:
                config["credentials"]["catalog"] = self.databricks_catalog
            if self.databricks_schema:
                config["credentials"]["schema"] = self.databricks_schema
            return config
        else:
            return {}
'''

# Destination field definitions for Pydantic
DESTINATION_FIELDS_PYDANTIC = '''
    destination: Optional[str] = Field(
        default=None,
        description="Database destination for persisting data (leave empty for DataFrame-only mode)"
    )

    # Snowflake fields
    snowflake_account: Optional[str] = Field(default=None, description="Snowflake account identifier")
    snowflake_database: Optional[str] = Field(default=None, description="Snowflake database name")
    snowflake_schema: Optional[str] = Field(default="public", description="Snowflake schema name")
    snowflake_warehouse: Optional[str] = Field(default=None, description="Snowflake warehouse name")
    snowflake_username: Optional[str] = Field(default=None, description="Snowflake username")
    snowflake_password: Optional[str] = Field(default=None, description="Snowflake password")
    snowflake_role: Optional[str] = Field(default=None, description="Snowflake role (optional)")

    # BigQuery fields
    bigquery_project_id: Optional[str] = Field(default=None, description="Google Cloud project ID")
    bigquery_dataset: Optional[str] = Field(default=None, description="BigQuery dataset name")
    bigquery_credentials_path: Optional[str] = Field(default=None, description="Path to service account JSON")
    bigquery_location: Optional[str] = Field(default="US", description="BigQuery dataset location")

    # Postgres fields
    postgres_host: Optional[str] = Field(default="localhost", description="PostgreSQL host")
    postgres_port: Optional[int] = Field(default=5432, description="PostgreSQL port")
    postgres_database: Optional[str] = Field(default=None, description="PostgreSQL database name")
    postgres_username: Optional[str] = Field(default=None, description="PostgreSQL username")
    postgres_password: Optional[str] = Field(default=None, description="PostgreSQL password")
    postgres_schema: Optional[str] = Field(default="public", description="PostgreSQL schema")

    # Redshift fields
    redshift_host: Optional[str] = Field(default=None, description="Redshift cluster endpoint")
    redshift_port: Optional[int] = Field(default=5439, description="Redshift port")
    redshift_database: Optional[str] = Field(default=None, description="Redshift database name")
    redshift_username: Optional[str] = Field(default=None, description="Redshift username")
    redshift_password: Optional[str] = Field(default=None, description="Redshift password")
    redshift_schema: Optional[str] = Field(default="public", description="Redshift schema")

    # DuckDB fields
    duckdb_database_path: Optional[str] = Field(default=None, description="Path to DuckDB file")

    # MotherDuck fields
    motherduck_database: Optional[str] = Field(default=None, description="MotherDuck database name")
    motherduck_token: Optional[str] = Field(default=None, description="MotherDuck authentication token")

    # Databricks fields
    databricks_server_hostname: Optional[str] = Field(default=None, description="Databricks workspace hostname")
    databricks_http_path: Optional[str] = Field(default=None, description="SQL warehouse HTTP path")
    databricks_access_token: Optional[str] = Field(default=None, description="Databricks personal access token")
    databricks_catalog: Optional[str] = Field(default=None, description="Unity Catalog name")
    databricks_schema: Optional[str] = Field(default="default", description="Databricks schema name")

    persist_and_return: bool = Field(
        default=False,
        description="When destination is set: persist to database AND return DataFrame"
    )
'''

def update_component_file(file_path: Path) -> bool:
    """Update a component.py file with structured destination fields."""
    print(f"Updating {file_path.parent.name}...")

    content = file_path.read_text()

    # Check if already has structured fields
    if 'snowflake_account' in content:
        print(f"  ⚠️  Already has structured fields, skipping")
        return False

    try:
        # Remove old destination, destination_config, persist_and_return fields
        # They're currently after include_sample_metadata
        pattern = r'(include_sample_metadata: bool = Field\([^)]+\)\s+\)\s*\n)(.*?)(destination: Optional\[str\] = Field.*?\n.*?\n.*?\n.*?destination_config: Optional\[str\] = Field.*?\n.*?\n.*?\n.*?persist_and_return: bool = Field.*?\n.*?\n.*?\n.*?\n)'

        # Simpler approach: just find and replace the three destination fields
        content = re.sub(
            r'\n\s+destination: Optional\[str\] = Field\([^)]+\)\s+\)\s*\n',
            '\n',
            content
        )
        content = re.sub(
            r'\n\s+destination_config: Optional\[str\] = Field\([^)]+\)\s+\)\s*\n',
            '\n',
            content
        )
        content = re.sub(
            r'\n\s+persist_and_return: bool = Field\([^)]+\)\s+\)\s*\n',
            '\n',
            content
        )

        # Add new structured fields after include_sample_metadata
        content = re.sub(
            r'(include_sample_metadata: bool = Field\([^)]+\)\s+\))\s*\n',
            r'\1\n' + DESTINATION_FIELDS_PYDANTIC + '\n',
            content
        )

        # Add destination config building method after the class definition starts
        # Insert before def build_defs
        content = re.sub(
            r'(\s+)(def build_defs\(self, context: ComponentLoadContext\))',
            r'\1' + DESTINATION_CONFIG_METHOD.strip() + '\n\n\1\\2',
            content
        )

        # Update the pipeline creation to use structured config
        # Change: if destination and not destination_config: raise ValueError...
        # To: destination_config = self._build_destination_config()
        content = re.sub(
            r'(\s+)# Determine destination\s+use_destination = destination if destination else "duckdb"\s+if destination and not destination_config:\s+raise ValueError\(f"destination_config is required when destination is set to \'{destination}\'"\)\s+context\.log\.info\(f"Using destination: \{use_destination\}"\)',
            r'''\1# Determine destination
\1use_destination = destination if destination else "duckdb"
\1destination_config = self._build_destination_config() if destination else {}
\1context.log.info(f"Using destination: {use_destination}")''',
            content
        )

        # Update pipeline creation to pass destination config
        # The dlt.pipeline call needs to be updated if it uses credentials
        # Actually, dlt uses environment variables or we pass via credentials parameter
        # Let me just make sure we're creating pipeline correctly

        file_path.write_text(content)
        print(f"  ✅ Updated")
        return True

    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Update all ingestion component.py files."""
    assets_dir = Path("/Users/ericthomas/dagster_components/dagster-component-templates/assets")

    # Find all ingestion components (excluding csv_file)
    component_files = []
    for component_py in assets_dir.glob("*_ingestion/component.py"):
        if component_py.parent.name != "csv_file_ingestion":
            component_files.append(component_py)

    print(f"Found {len(component_files)} dlt ingestion components\n")

    updated_count = 0
    for component_file in sorted(component_files):
        if update_component_file(component_file):
            updated_count += 1
        print()

    print(f"\n✅ Updated {updated_count}/{len(component_files)} components")

if __name__ == "__main__":
    main()
