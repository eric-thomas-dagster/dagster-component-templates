#!/usr/bin/env python3
"""Create stub components for missing pipeline dependencies."""

import json
from pathlib import Path

# Define missing components with minimal schemas
MISSING_COMPONENTS = {
    "hub_spot_ingestion": {
        "type": "dagster_component_templates.HubSpotIngestionComponent",
        "name": "HubSpot Ingestion",
        "category": "cdp",
        "description": "Ingest CRM data from HubSpot",
        "icon": "Users",
        "attributes": {
            "asset_name": {"type": "string", "required": True, "label": "Asset Name"},
            "api_key": {"type": "string", "required": True, "label": "API Key", "secret": True},
            "description": {"type": "string", "required": False, "label": "Description"}
        }
    },
    "customer360": {
        "type": "dagster_component_templates.Customer360Component",
        "name": "Customer 360",
        "category": "transformation",
        "description": "Create unified customer profile from multiple data sources",
        "icon": "Users",
        "attributes": {
            "asset_name": {"type": "string", "required": True, "label": "Asset Name"},
            "description": {"type": "string", "required": False, "label": "Description"}
        }
    },
    "content_ingestion": {
        "type": "dagster_component_templates.ContentIngestionComponent",
        "name": "Content Ingestion",
        "category": "ingestion",
        "description": "Ingest user-generated content for moderation",
        "icon": "FileText",
        "attributes": {
            "asset_name": {"type": "string", "required": True, "label": "Asset Name"},
            "description": {"type": "string", "required": False, "label": "Description"}
        }
    },
    "moderation_scorer": {
        "type": "dagster_component_templates.ModerationScorerComponent",
        "name": "Moderation Scorer",
        "category": "ml",
        "description": "Score content for moderation decisions",
        "icon": "Shield",
        "attributes": {
            "asset_name": {"type": "string", "required": True, "label": "Asset Name"},
            "source_asset": {"type": "string", "required": False, "label": "Source Asset"},
            "description": {"type": "string", "required": False, "label": "Description"}
        }
    },
    "support_ticket_ingestion": {
        "type": "dagster_component_templates.SupportTicketIngestionComponent",
        "name": "Support Ticket Ingestion",
        "category": "cdp",
        "description": "Ingest support tickets from help desk systems",
        "icon": "MessageCircle",
        "attributes": {
            "asset_name": {"type": "string", "required": True, "label": "Asset Name"},
            "platform": {"type": "string", "required": True, "label": "Platform"},
            "description": {"type": "string", "required": False, "label": "Description"}
        }
    },
    "document_ingestion": {
        "type": "dagster_component_templates.DocumentIngestionComponent",
        "name": "Document Ingestion",
        "category": "ingestion",
        "description": "Ingest documents for RAG/Q&A systems",
        "icon": "FileText",
        "attributes": {
            "asset_name": {"type": "string", "required": True, "label": "Asset Name"},
            "source_path": {"type": "string", "required": False, "label": "Source Path"},
            "description": {"type": "string", "required": False, "label": "Description"}
        }
    }
}

COMPONENT_TEMPLATE = '''from dagster import AssetExecutionContext
from dagster_components import Component, component_type
from dagster_components.core.component import ComponentLoadContext
from pydantic import BaseModel
import pandas as pd


class {class_name}Model(BaseModel):
    """Model for {class_name} component configuration."""
    asset_name: str
    description: str = ""


@component_type(name="{component_type}")
class {class_name}(Component):
    """
    {description}

    This is a stub component. Implement the actual logic as needed.
    """

    params_schema = {class_name}Model

    def __init__(self, dirpath, context: ComponentLoadContext):
        super().__init__(dirpath, context)

    def build_defs(self, load_context):
        from dagster import asset, Definitions, Output, AssetSpec

        params = self.params

        @asset(
            name=params.asset_name,
            description=params.description or "{description}",
        )
        def {asset_name}(context: AssetExecutionContext):
            """Stub implementation - returns empty DataFrame."""
            context.log.warning("Using stub implementation of {class_name}")
            return pd.DataFrame()

        return Definitions(assets=[{asset_name}])
'''

def create_component(component_id: str, schema: dict):
    """Create a stub component with minimal implementation."""
    component_dir = Path("assets") / component_id
    component_dir.mkdir(parents=True, exist_ok=True)

    # Create schema.json
    schema_file = component_dir / "schema.json"
    with open(schema_file, 'w') as f:
        json.dump(schema, f, indent=2)

    # Generate class name (e.g., hub_spot_ingestion -> HubSpotIngestion)
    class_name = ''.join(word.capitalize() for word in component_id.split('_')) + "Component"
    asset_name = f"{component_id}_asset"
    component_type = schema["type"]

    # Create component.py
    component_file = component_dir / f"{component_id}.py"
    code = COMPONENT_TEMPLATE.format(
        class_name=class_name,
        component_type=component_type,
        description=schema["description"],
        asset_name=asset_name,
    )
    with open(component_file, 'w') as f:
        f.write(code)

    # Create __init__.py
    init_file = component_dir / "__init__.py"
    with open(init_file, 'w') as f:
        f.write(f"from .{component_id} import {class_name}\n\n__all__ = ['{class_name}']\n")

    # Create README.md
    readme_file = component_dir / "README.md"
    with open(readme_file, 'w') as f:
        f.write(f"""# {schema['name']}

{schema['description']}

**Status:** Stub implementation - needs full implementation

## Configuration

- `asset_name`: Name of the asset to create

## Usage

This is a placeholder component. Implement the actual logic in `{component_id}.py`.
""")

    print(f"✅ Created {component_id}")

def main():
    print("Creating missing components...\n")

    for component_id, schema in MISSING_COMPONENTS.items():
        create_component(component_id, schema)

    print(f"\n✅ Created {len(MISSING_COMPONENTS)} stub components")
    print("\nNext steps:")
    print("1. Review the generated components in assets/")
    print("2. Implement actual logic in each component's .py file")
    print("3. Commit and push to GitHub")

if __name__ == "__main__":
    main()
