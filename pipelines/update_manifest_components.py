#!/usr/bin/env python3
"""
Update manifest.json with components extracted from pipeline YAML files.
This ensures the manifest accurately reflects what's in each pipeline.
"""

import json
import yaml
import re
from pathlib import Path

def parse_old_format_yaml(yaml_path):
    """Parse old format YAML (type: dagster_component_templates.XComponent)"""
    with open(yaml_path, 'r') as f:
        content = f.read()

    # Split by --- (with optional whitespace) to get each component
    sections = re.split(r'\n---+\s*\n', content)
    components = []

    for section in sections:
        if not section.strip():
            continue

        # Extract type
        type_match = re.search(r'type:\s*dagster_component_templates\.(\w+)', section)
        if not type_match:
            continue

        component_type = type_match.group(1)

        # Convert type to component_id (e.g., ShopifyIngestionComponent -> shopify_ingestion)
        component_id = re.sub(r'Component$', '', component_type)
        component_id = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', component_id).lower()

        # Extract attributes section
        attrs_match = re.search(r'attributes:\s*\n((?:[ \t]+.*\n)*)', section)
        if not attrs_match:
            continue

        attrs_text = attrs_match.group(1)

        # Parse key attributes
        asset_name = re.search(r'asset_name:\s*(\S+)', attrs_text)
        source_asset = re.search(r'source_asset:\s*(\S+)', attrs_text)
        description = re.search(r'description:\s*["\']([^"\']+)["\']', attrs_text)

        config = {}
        if asset_name:
            config['asset_name'] = asset_name.group(1)
            instance_name = asset_name.group(1)
        else:
            instance_name = component_id

        if description:
            config['description'] = description.group(1)

        # Determine dependencies
        depends_on = []
        if source_asset:
            depends_on = [source_asset.group(1)]

        components.append({
            "component_id": component_id,
            "instance_name": instance_name,
            "config_mapping": config,
            "depends_on": depends_on
        })

    return components

def parse_new_format_yaml(yaml_path):
    """Parse new format YAML (- id: component_id)"""
    with open(yaml_path, 'r') as f:
        content = f.read()

    components = []

    # Find all component blocks with "- id:" (with indentation)
    pattern = r'\s+- id:\s+(\w+)\s+instance_name:\s+(\w+)\s+(?:enabled:.*?\s+)?config:(.*?)(?=\s+- id:|\n\n|\Z)'
    matches = re.finditer(pattern, content, re.DOTALL)

    for match in matches:
        component_id = match.group(1)
        instance_name = match.group(2)
        config_text = match.group(3)

        # Extract config fields
        config = {}

        asset_name = re.search(r'asset_name:\s*["\']?([^"\n]+)["\']?', config_text)
        if asset_name:
            config['asset_name'] = asset_name.group(1).strip('"\'')

        source_asset = re.search(r'source_asset:\s*["\']?([^"\n]+)["\']?', config_text)
        description = re.search(r'description:\s*["\']?([^"\n]+)["\']?', config_text)

        if description:
            config['description'] = description.group(1).strip('"\'')

        # Determine dependencies
        depends_on = []
        if source_asset:
            depends_on = [source_asset.group(1).strip('"\'')]

        components.append({
            "component_id": component_id,
            "instance_name": instance_name,
            "config_mapping": config,
            "depends_on": depends_on
        })

    return components

def update_manifest():
    """Update manifest.json with components from YAML files"""
    manifest_path = Path('manifest.json')

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)

    # Map of pipeline ID to YAML filename
    pipeline_files = {
        'customer_ltv_pipeline': 'customer_ltv_pipeline.yaml',
        'marketing_attribution_pipeline': 'marketing_attribution_pipeline.yaml',
        'churn_prevention_pipeline': 'churn_prevention_pipeline.yaml',
        'product_recommendations_pipeline': 'product_recommendations_pipeline.yaml',
        'fraud_detection_pipeline': 'fraud_detection_pipeline.yaml',
        'ab_testing_pipeline': 'ab_testing_pipeline.yaml',
        'full_cdp_pipeline': 'full_cdp_pipeline.yaml',
        'customer_review_sentiment_pipeline': 'customer_review_sentiment_pipeline.yaml',
        'document_qa_rag_pipeline': 'document_qa_rag_pipeline.yaml',
        'customer_support_automation': 'customer_support_automation_pipeline.yaml',
        'content_moderation': 'content_moderation_pipeline.yaml'
    }

    # Pipelines that use old format (type: dagster_component_templates.X)
    old_format_pipelines = {
        'customer_ltv_pipeline',
        'marketing_attribution_pipeline',
        'churn_prevention_pipeline',
        'product_recommendations_pipeline',
        'fraud_detection_pipeline',
        'ab_testing_pipeline',
        'full_cdp_pipeline'
    }

    for pipeline in manifest['pipelines']:
        pipeline_id = pipeline['id']
        yaml_file = pipeline_files.get(pipeline_id)

        if not yaml_file:
            print(f"⚠️  No YAML file mapped for {pipeline_id}")
            continue

        yaml_path = Path(yaml_file)
        if not yaml_path.exists():
            print(f"❌ {yaml_file} not found")
            continue

        # Parse the YAML file based on format
        if pipeline_id in old_format_pipelines:
            components = parse_old_format_yaml(yaml_path)
        else:
            components = parse_new_format_yaml(yaml_path)

        # Update the manifest
        old_count = len(pipeline.get('components', []))
        pipeline['components'] = components
        new_count = len(components)

        print(f"✓ {pipeline_id}: {old_count} -> {new_count} components")

    # Write updated manifest
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f"\n✅ Updated manifest.json")

if __name__ == '__main__':
    update_manifest()
