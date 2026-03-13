#!/usr/bin/env python3
"""Add requirements_url to manifest for components that have requirements.txt"""
import json
from pathlib import Path

# Load manifest
manifest_path = Path(__file__).parent / "manifest.json"
with open(manifest_path, 'r') as f:
    manifest = json.load(f)

# GitHub base URL
base_url = "https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main"

# Update components
updated_count = 0
for component in manifest['components']:
    component_path = Path(__file__).parent / component['path']
    requirements_file = component_path / "requirements.txt"

    # Check if requirements.txt exists
    if requirements_file.exists():
        # Add requirements_url if not already present
        requirements_url = f"{base_url}/{component['path']}/requirements.txt"
        if 'requirements_url' not in component or component.get('requirements_url') != requirements_url:
            component['requirements_url'] = requirements_url
            print(f"✓ Added requirements_url for {component['id']}")
            updated_count += 1

        # Also update dependencies from requirements.txt
        with open(requirements_file, 'r') as f:
            requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        if 'dependencies' not in component:
            component['dependencies'] = {}
        component['dependencies']['pip'] = requirements
    else:
        # Remove requirements_url if file doesn't exist
        if 'requirements_url' in component:
            del component['requirements_url']
            print(f"✗ Removed requirements_url for {component['id']} (file not found)")

# Save manifest
with open(manifest_path, 'w') as f:
    json.dump(manifest, f, indent=2)

print(f"\n✅ Updated {updated_count} components with requirements_url")
print(f"📝 Saved to {manifest_path}")
