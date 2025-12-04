#!/usr/bin/env python3
"""
Generate pipelines/manifest.json by parsing pipeline YAML files.

This script discovers all pipeline YAML files and extracts metadata from comments
and component definitions to auto-generate the manifest.json file.
"""

import json
import yaml
from pathlib import Path
import re
from typing import Dict, List, Any


def extract_metadata_from_comments(yaml_content: str) -> Dict[str, str]:
    """Extract metadata from YAML file comments."""
    lines = yaml_content.split('\n')
    metadata = {}

    # Extract name from first comment line
    for line in lines[:5]:
        if line.strip().startswith('#') and not line.strip().startswith('##'):
            name = line.strip('#').strip()
            if name and 'Pipeline' in name:
                metadata['name'] = name.replace(' Pipeline', '')
                break

    # Extract use case, business outcome, estimated savings
    for line in lines:
        line = line.strip()
        if line.startswith('# Use Case:'):
            metadata['use_case'] = line.replace('# Use Case:', '').strip()
        elif line.startswith('# Business Outcome:'):
            metadata['business_outcome'] = line.replace('# Business Outcome:', '').strip()
        elif line.startswith('# Estimated Savings:'):
            metadata['estimated_savings'] = line.replace('# Estimated Savings:', '').strip()
        elif line.startswith('# Category:'):
            metadata['category'] = line.replace('# Category:', '').strip()
        elif line.startswith('# Description:'):
            metadata['description'] = line.replace('# Description:', '').strip()

    return metadata


def parse_pipeline_yaml(file_path: Path) -> Dict[str, Any]:
    """Parse a pipeline YAML file and extract all metadata."""
    with open(file_path, 'r') as f:
        content = f.read()

    # Extract metadata from comments
    metadata = extract_metadata_from_comments(content)

    # Parse YAML - try to detect format
    documents = list(yaml.safe_load_all(content))
    components = []
    pipeline_params = {}

    # Check if this is a structured pipeline format (has 'pipeline:' key)
    if len(documents) == 1 and 'pipeline' in documents[0]:
        doc = documents[0]
        pipeline_def = doc['pipeline']

        # Extract name from pipeline definition
        if 'name' in pipeline_def:
            metadata['name'] = pipeline_def['name']

        # Extract parameters
        if 'parameters' in pipeline_def:
            pipeline_params = pipeline_def['parameters']

        # Extract components
        if 'components' in pipeline_def:
            for comp_def in pipeline_def['components']:
                component_id = comp_def.get('id', 'unknown')
                instance_name = comp_def.get('instance_name', component_id)
                config = comp_def.get('config', {})
                depends_on = comp_def.get('depends_on', [])

                components.append({
                    'component_id': component_id,
                    'instance_name': instance_name,
                    'config_mapping': config,
                    'depends_on': depends_on if isinstance(depends_on, list) else [depends_on]
                })
    else:
        # Multi-document YAML format (old format)
        for doc in documents:
            if doc and isinstance(doc, dict):
                if 'type' in doc:
                    # This is a component
                    component_type = doc['type'].split('.')[-1]
                    component_id = _type_to_id(component_type)

                    attributes = doc.get('attributes', {})
                    asset_name = attributes.get('asset_name', 'unknown')

                    # Try to find dependencies from source_asset or similar fields
                    depends_on = []
                    for key in ['source_asset', 'input_asset', 'transaction_data_asset', 'event_data_asset']:
                        if key in attributes and attributes[key]:
                            depends_on.append(attributes[key])

                    components.append({
                        'component_id': component_id,
                        'instance_name': asset_name,
                        'config_mapping': {
                            'asset_name': asset_name,
                            'description': attributes.get('description', '')
                        },
                        'depends_on': depends_on[:1] if depends_on else []  # Take first dependency only
                    })

    # Derive pipeline ID from filename
    pipeline_id = file_path.stem

    # Set defaults for missing metadata
    if 'name' not in metadata:
        metadata['name'] = pipeline_id.replace('_', ' ').title()

    if 'description' not in metadata:
        metadata['description'] = metadata.get('use_case', '')

    if 'category' not in metadata:
        # Infer category from name/use case
        name_lower = metadata['name'].lower()
        if any(word in name_lower for word in ['customer', 'ltv', 'segment', 'churn']):
            metadata['category'] = 'customer_analytics'
        elif any(word in name_lower for word in ['support', 'ticket', 'qa']):
            metadata['category'] = 'customer_support'
        elif any(word in name_lower for word in ['content', 'moderation', 'sentiment']):
            metadata['category'] = 'content'
        elif any(word in name_lower for word in ['marketing', 'attribution', 'campaign']):
            metadata['category'] = 'marketing'
        elif any(word in name_lower for word in ['fraud', 'anomaly']):
            metadata['category'] = 'security'
        else:
            metadata['category'] = 'other'

    # Determine icon based on category
    icon_map = {
        'customer_analytics': 'TrendingUp',
        'customer_support': 'MessageSquare',
        'content': 'FileText',
        'marketing': 'Target',
        'security': 'Shield',
        'other': 'Workflow'
    }

    return {
        'id': pipeline_id,
        'name': metadata.get('name', pipeline_id.replace('_', ' ').title()),
        'description': metadata.get('description', metadata.get('use_case', '')),
        'category': metadata.get('category', 'other'),
        'use_case': metadata.get('use_case', metadata.get('description', '')),
        'business_outcome': metadata.get('business_outcome', ''),
        'estimated_savings': metadata.get('estimated_savings'),
        'components': components,
        'pipeline_params': pipeline_params,  # TODO: Extract from manifest or add to YAML
        'readme_url': f'https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/pipelines/{pipeline_id}.yaml',
        'yaml_url': f'https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main/pipelines/{pipeline_id}.yaml',
        'icon': icon_map.get(metadata.get('category', 'other'), 'Workflow')
    }


def _type_to_id(type_name: str) -> str:
    """Convert component type name to ID.

    Example: ShopifyIngestionComponent -> shopify_ingestion
    """
    # Remove 'Component' suffix
    name = type_name.replace('Component', '')

    # Special case for DLTDataFrameWriter
    if name == 'DLTDataFrameWriter':
        return 'dlt_dataframe_writer'

    # Convert CamelCase to snake_case
    snake_case = re.sub('([A-Z]+)([A-Z][a-z])', r'\1_\2', name)
    snake_case = re.sub('([a-z\d])([A-Z])', r'\1_\2', snake_case)
    snake_case = snake_case.lower()

    return snake_case


def generate_manifest():
    """Generate the pipeline manifest from YAML files."""
    pipelines_dir = Path(__file__).parent
    yaml_files = sorted(pipelines_dir.glob('*.yaml'))

    pipelines = []

    for yaml_file in yaml_files:
        print(f"Processing {yaml_file.name}...")
        try:
            pipeline_data = parse_pipeline_yaml(yaml_file)
            pipelines.append(pipeline_data)
            print(f"  ✓ {pipeline_data['name']} - {len(pipeline_data['components'])} components")
        except Exception as e:
            print(f"  ✗ Error: {e}")

    # Load existing manifest to preserve environment-specific pipeline_params
    # (These are not in the YAML files - they're added via manifest maintenance)
    manifest_path = pipelines_dir / 'manifest.json'
    existing_manifest = {}
    if manifest_path.exists():
        with open(manifest_path, 'r') as f:
            existing_manifest = json.load(f)

        # Merge environment-specific params and show_if conditions from existing manifest
        # (pipeline_params from YAML are already parsed, we just need to add env-specific metadata)
        existing_params = {p['id']: p.get('pipeline_params', {}) for p in existing_manifest.get('pipelines', [])}
        for pipeline in pipelines:
            if pipeline['id'] in existing_params:
                # Merge: keep YAML params structure, add environment_specific/show_if from existing
                yaml_params = pipeline.get('pipeline_params', {})
                existing_pipe_params = existing_params[pipeline['id']]

                # Merge each parameter
                for param_key, param_value in yaml_params.items():
                    if param_key in existing_pipe_params:
                        # Preserve environment_specific, show_if, sensitive flags
                        existing_param = existing_pipe_params[param_key]
                        if 'environment_specific' in existing_param:
                            param_value['environment_specific'] = existing_param['environment_specific']
                        if 'show_if' in existing_param:
                            param_value['show_if'] = existing_param['show_if']
                        if 'sensitive' in existing_param:
                            param_value['sensitive'] = existing_param['sensitive']
                        if 'environment_defaults' in existing_param:
                            param_value['environment_defaults'] = existing_param['environment_defaults']

                # Also add params that exist in existing but not in YAML (like auth credentials)
                for param_key, param_value in existing_pipe_params.items():
                    if param_key not in yaml_params:
                        yaml_params[param_key] = param_value

                pipeline['pipeline_params'] = yaml_params

    manifest = {
        'version': '1.0.0',
        'repository': 'https://github.com/eric-thomas-dagster/dagster-component-templates',
        'last_updated': '2024-12-04',
        'pipelines': pipelines
    }

    # Write manifest
    with open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f"\n✅ Generated manifest with {len(pipelines)} pipelines")
    print(f"📝 Saved to {manifest_path}")


if __name__ == '__main__':
    generate_manifest()
