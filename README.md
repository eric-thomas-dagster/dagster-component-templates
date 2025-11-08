# Dagster Component Templates

A community repository of reusable Dagster component templates for sensors, assets, resources, and more.

## What are Dagster Components?

Dagster components are reusable, configurable building blocks that can be instantiated via YAML configuration. They allow you to package common patterns (like monitoring an S3 bucket or ingesting CSV files) into components that can be configured without writing Python code.

## Repository Structure

```
dagster-component-templates/
├── README.md                 # This file
├── CONTRIBUTING.md           # How to contribute new components
├── manifest.json             # Registry of all available components
├── sensors/                  # Event-driven triggers
│   ├── slack_notification/   # Example: Slack webhook sensor
│   ├── github_webhook/       # Example: GitHub webhook sensor
│   └── ...
├── assets/                   # Data assets
│   ├── csv_file_ingestion/   # Example: CSV file loader
│   ├── rest_api_poller/      # Example: REST API data fetcher
│   └── ...
├── resources/                # Shared resources
│   ├── slack_client/         # Example: Slack API client
│   └── ...
└── factories/                # Asset factories for patterns
    └── ...
```

## Component Structure

Each component lives in its own directory with the following files:

```
component_name/
├── component.py       # Python component class implementation
├── schema.json        # JSON schema describing configuration
├── README.md          # Component documentation
├── example.yaml       # Example YAML configuration
└── requirements.txt   # Python dependencies (optional)
```

### component.py

The main Python file containing the Dagster component class:

```python
from dagster import Component, Resolvable, Definitions
from pydantic import BaseModel, Field

class MyComponent(Component, Resolvable, BaseModel):
    """My custom component."""

    # Define configuration fields
    my_param: str = Field(description="Description")

    def build_defs(self, context) -> Definitions:
        # Return Dagster definitions (sensors, assets, etc.)
        return Definitions(...)
```

### schema.json

Describes the component's configuration for UI rendering:

```json
{
  "type": "dagster_component_templates.MyComponent",
  "name": "My Component",
  "category": "sensors",
  "description": "What this component does",
  "version": "1.0.0",
  "author": "Your Name",
  "attributes": {
    "my_param": {
      "type": "string",
      "required": true,
      "label": "My Parameter",
      "description": "Help text",
      "placeholder": "example_value"
    }
  },
  "dependencies": {
    "pip": ["requests", "slack-sdk"]
  },
  "tags": ["webhook", "notification"]
}
```

### README.md

Component documentation with usage examples:

```markdown
# My Component

Description of what this component does.

## Configuration

- `my_param` - Description of parameter

## Example

\`\`\`yaml
type: dagster_component_templates.MyComponent
attributes:
  my_param: "example"
\`\`\`

## Requirements

- Python packages: requests, slack-sdk
- Environment variables: SLACK_TOKEN
```

### example.yaml

A working example configuration:

```yaml
type: dagster_component_templates.MyComponent
attributes:
  my_param: "example_value"
  another_param: 123
```

## manifest.json

The manifest file lists all available components:

```json
{
  "version": "1.0.0",
  "components": [
    {
      "id": "slack_notification_sensor",
      "name": "Slack Notification Sensor",
      "category": "sensors",
      "description": "Sends notifications to Slack when events occur",
      "version": "1.0.0",
      "author": "Community",
      "path": "sensors/slack_notification",
      "tags": ["slack", "notification", "webhook"]
    }
  ]
}
```

## Using Components

### 1. Install Component Files

Copy the component directory to your project:

```bash
# Copy component.py to your project's lib directory
cp sensors/slack_notification/component.py \
   my_project/src/my_project/lib/dagster_component_templates/

# Update __init__.py to export the component
```

### 2. Create YAML Configuration

Create a YAML file in your project's `defs/components/` directory:

```yaml
# defs/components/my_slack_sensor.yaml
type: dagster_component_templates.SlackNotificationSensor
attributes:
  sensor_name: my_slack_sensor
  webhook_url: "${SLACK_WEBHOOK_URL}"
  job_name: my_job
```

### 3. Run Your Project

The component will be automatically discovered and instantiated by Dagster.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on adding new components.

## License

MIT License - see LICENSE file for details
