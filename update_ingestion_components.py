#!/usr/bin/env python3
"""
Script to add database destination support to all ingestion components.
Adds three new fields: destination, destination_config, persist_and_return
"""

import re
from pathlib import Path

def update_component_file(file_path: Path):
    """Update a single component.py file with destination support."""
    print(f"Updating {file_path.parent.name}...")

    content = file_path.read_text()
    original_content = content

    # Check if already updated
    if 'destination: Optional[str]' in content:
        print(f"  ⚠️  Already updated, skipping")
        return False

    # Step 1: Add three new fields after include_sample_metadata
    new_fields = '''
    destination: Optional[str] = Field(
        default=None,
        description="Optional dlt destination (e.g., 'snowflake', 'bigquery', 'postgres', 'redshift'). If not set, uses in-memory DuckDB and returns DataFrame."
    )

    destination_config: Optional[str] = Field(
        default=None,
        description="Optional destination configuration as connection string or JSON. Required if destination is set."
    )

    persist_and_return: bool = Field(
        default=False,
        description="If True with destination set: persist to database AND return DataFrame. If False: only persist to database."
    )
'''

    # Find include_sample_metadata field and add new fields after it
    pattern = r'(include_sample_metadata: bool = Field\([^)]+\)\s+)'
    match = re.search(pattern, content)
    if match:
        insert_pos = match.end()
        content = content[:insert_pos] + new_fields + '\n' + content[insert_pos:]
    else:
        print(f"  ❌ Could not find include_sample_metadata field")
        return False

    # Step 2: Add field extraction in build_defs
    # Find the line with include_sample = self.include_sample_metadata
    pattern = r'(include_sample = self\.include_sample_metadata)\n'
    replacement = r'\1\n        destination = self.destination\n        destination_config = self.destination_config\n        persist_and_return = self.persist_and_return\n'
    content = re.sub(pattern, replacement, content)

    # Step 3: Update pipeline creation
    # Find the pipeline creation and add destination logic before it
    pipeline_pattern = r'(\s+# Create.*pipeline.*\n\s+pipeline = dlt\.pipeline\(\n\s+pipeline_name=)'

    destination_logic = '''
            # Determine destination
            use_destination = destination if destination else "duckdb"
            if destination and not destination_config:
                raise ValueError(f"destination_config is required when destination is set to '{destination}'")

            context.log.info(f"Using destination: {use_destination}")

            # Create pipeline (in-memory DuckDB or specified destination)
            pipeline = dlt.pipeline(
                pipeline_name='''

    # Replace the pipeline creation section
    content = re.sub(
        r'(\s+)# Create.*pipeline.*\n\s+pipeline = dlt\.pipeline\(\n\s+pipeline_name=',
        destination_logic,
        content,
        count=1
    )

    # Update destination parameter in pipeline
    content = re.sub(
        r'destination="duckdb",\s+# Use DuckDB in-memory',
        'destination=use_destination,',
        content
    )
    content = re.sub(
        r'destination="duckdb",\s+# .*',
        'destination=use_destination,',
        content
    )

    # Update dataset_name to use asset_name when destination is set
    content = re.sub(
        r'dataset_name=f"{asset_name}_temp"',
        'dataset_name=asset_name if destination else f"{asset_name}_temp"',
        content
    )

    # Step 4: Add persist_and_return conditional logic
    # Find where data is loaded and add conditional logic
    # This needs to be inserted after load_info logging
    persist_logic = '''
            # Handle based on destination mode
            if destination and not persist_and_return:
                # Persist only mode: data is in destination, return metadata only
                context.log.info(f"Data persisted to {destination}. Not returning DataFrame (persist_and_return=False)")

                # Get row counts from load_info if available
                try:
                    total_rows = sum(
                        package.get('row_counts', {}).get(resource_name, 0)
                        for package in load_info.load_packages
                        for resource_name in resources_list if 'resources_list' in locals()
                    )
                except:
                    total_rows = 0

                metadata = {
                    "destination": destination,
                    "dataset_name": asset_name,
                    "row_count": total_rows,
                }
                context.add_output_metadata(metadata)

                # Return empty DataFrame with metadata
                return pd.DataFrame({"status": ["persisted"], "destination": [destination], "row_count": [total_rows]})

            # DataFrame return mode: extract data from destination
            dataset_name = asset_name if destination else f"{asset_name}_temp"
'''

    # Insert after "context.log.info.*loaded.*load_info"
    content = re.sub(
        r'(context\.log\.info\(f".*load.*{load_info}.*"\))\n',
        r'\1\n' + persist_logic + '\n',
        content,
        count=1
    )

    # Update dataset references in queries
    content = re.sub(
        r'f"SELECT \* FROM {asset_name}_temp\.',
        r'f"SELECT * FROM {dataset_name}.',
        content
    )

    # Step 5: Add destination info to metadata
    metadata_pattern = r'(metadata = \{[^}]+\})'

    def add_destination_metadata(match):
        metadata_dict = match.group(1)
        # Add destination fields before the closing brace
        enhanced = metadata_dict.rstrip('}').rstrip() + '''
            }

            # Add destination info if persisting
            if destination:
                metadata["destination"] = destination
                metadata["dataset_name"] = asset_name
                metadata["persist_and_return"] = persist_and_return'''
        return enhanced

    content = re.sub(metadata_pattern, add_destination_metadata, content, count=1)

    # Write back if changed
    if content != original_content:
        file_path.write_text(content)
        print(f"  ✅ Updated successfully")
        return True
    else:
        print(f"  ⚠️  No changes made")
        return False


def main():
    """Update all ingestion components."""
    assets_dir = Path("/Users/ericthomas/dagster_components/dagster-component-templates/assets")

    # Find all ingestion component files
    ingestion_files = sorted(assets_dir.glob("*_ingestion/component.py"))

    print(f"Found {len(ingestion_files)} ingestion components\n")

    updated_count = 0
    for file_path in ingestion_files:
        try:
            if update_component_file(file_path):
                updated_count += 1
        except Exception as e:
            print(f"  ❌ Error: {e}")

    print(f"\n✅ Updated {updated_count}/{len(ingestion_files)} components")


if __name__ == "__main__":
    main()
