#!/usr/bin/env python3
"""Generate manifest.json from all components in the repository."""

import json
from pathlib import Path

# Base URL for raw GitHub content
BASE_URL = "https://raw.githubusercontent.com/eric-thomas-dagster/dagster-component-templates/main"

def scan_components():
    """Scan assets/ directory for components."""
    components = []
    assets_dir = Path("assets")

    if not assets_dir.exists():
        print(f"Error: {assets_dir} directory not found")
        return components

    for component_dir in sorted(assets_dir.iterdir()):
        if not component_dir.is_dir() or component_dir.name.startswith('.'):
            continue

        component_id = component_dir.name

        # Check for required files
        schema_file = component_dir / "schema.json"
        component_file = component_dir / "component.py"
        readme_file = component_dir / "README.md"

        if not schema_file.exists() or not component_file.exists():
            print(f"Skipping {component_id}: missing schema.json or component.py")
            continue

        # Load schema to get metadata
        try:
            with open(schema_file) as f:
                schema = json.load(f)
        except Exception as e:
            print(f"Error loading schema for {component_id}: {e}")
            continue

        # Build component entry
        component_entry = {
            "id": component_id,
            "name": schema.get("name", component_id),
            "category": schema.get("category", "other"),
            "description": schema.get("description", ""),
            "version": "1.0.0",
            "author": "Dagster Community",
            "path": f"assets/{component_id}",
            "tags": schema.get("tags", []),
            "dependencies": {
                "pip": []
            },
            "readme_url": f"{BASE_URL}/assets/{component_id}/README.md",
            "component_url": f"{BASE_URL}/assets/{component_id}/component.py",
            "schema_url": f"{BASE_URL}/assets/{component_id}/schema.json",
            "example_url": f"{BASE_URL}/assets/{component_id}/example.yaml",
        }

        # Add requirements URL if exists
        requirements_file = component_dir / "requirements.txt"
        if requirements_file.exists():
            component_entry["requirements_url"] = f"{BASE_URL}/assets/{component_id}/requirements.txt"

            # Parse requirements
            try:
                with open(requirements_file) as f:
                    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                    component_entry["dependencies"]["pip"] = requirements
            except Exception as e:
                print(f"Warning: Could not parse requirements for {component_id}: {e}")

        components.append(component_entry)
        print(f"Added component: {component_id}")

    return components

def scan_sensors():
    """Scan sensors/ directory for sensor components."""
    sensors = []
    sensors_dir = Path("sensors")

    if not sensors_dir.exists():
        return sensors

    for sensor_dir in sorted(sensors_dir.iterdir()):
        if not sensor_dir.is_dir() or sensor_dir.name.startswith('.'):
            continue

        sensor_id = sensor_dir.name

        # Check for required files
        schema_file = sensor_dir / "schema.json"
        component_file = sensor_dir / "component.py"

        if not schema_file.exists() or not component_file.exists():
            continue

        # Load schema
        try:
            with open(schema_file) as f:
                schema = json.load(f)
        except:
            continue

        # Build sensor entry
        sensor_entry = {
            "id": sensor_id,
            "name": schema.get("name", sensor_id),
            "category": "sensors",
            "description": schema.get("description", ""),
            "version": "1.0.0",
            "author": "Dagster Community",
            "path": f"sensors/{sensor_id}",
            "tags": schema.get("tags", []),
            "dependencies": {"pip": []},
            "readme_url": f"{BASE_URL}/sensors/{sensor_id}/README.md",
            "component_url": f"{BASE_URL}/sensors/{sensor_id}/component.py",
            "schema_url": f"{BASE_URL}/sensors/{sensor_id}/schema.json",
            "example_url": f"{BASE_URL}/sensors/{sensor_id}/example.yaml",
        }

        requirements_file = sensor_dir / "requirements.txt"
        if requirements_file.exists():
            sensor_entry["requirements_url"] = f"{BASE_URL}/sensors/{sensor_id}/requirements.txt"

        sensors.append(sensor_entry)
        print(f"Added sensor: {sensor_id}")

    return sensors

def main():
    print("Generating manifest.json...")

    # Scan components and sensors
    components = scan_components()
    sensors = scan_sensors()

    # Combine all components
    all_components = components + sensors

    # Create manifest
    manifest = {
        "version": "1.0.0",
        "repository": "https://github.com/eric-thomas-dagster/dagster-component-templates",
        "last_updated": "2025-12-05",
        "components": all_components
    }

    # Write manifest
    with open("manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"\nManifest generated with {len(all_components)} components")
    print(f"  - {len(components)} asset components")
    print(f"  - {len(sensors)} sensor components")

if __name__ == "__main__":
    main()
