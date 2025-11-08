# Contributing to Dagster Component Templates

Thank you for contributing! This guide will help you add new components to the repository.

## Quick Start

1. Fork the repository
2. Create a new branch for your component
3. Add your component following the structure below
4. Update `manifest.json`
5. Submit a pull request

## Component Checklist

Before submitting, ensure your component includes:

- [ ] `component.py` - Python component implementation
- [ ] `schema.json` - JSON schema for UI rendering
- [ ] `README.md` - Documentation
- [ ] `example.yaml` - Working example configuration
- [ ] Updated `manifest.json` with your component entry
- [ ] Tests (optional but recommended)

## Step-by-Step Guide

### 1. Choose the Right Category

Determine which category your component fits into:

- **sensors/** - Event-driven triggers (webhooks, file watchers, etc.)
- **assets/** - Data transformation assets
- **resources/** - Shared resources (API clients, database connections)
- **factories/** - Patterns for generating multiple similar assets

### 2. Create Component Directory

```bash
mkdir -p sensors/my_component_name
cd sensors/my_component_name
```

### 3. Write component.py

Create the Python component class:

```python
"""My Component - Brief description."""

from typing import Optional
from dagster import (
    Component,
    Resolvable,
    Definitions,
    SensorDefinition,
    RunRequest,
    SkipReason,
    SensorEvaluationContext,
    ComponentLoadContext,
    DefaultSensorStatus,
)
from pydantic import BaseModel, Field


class MyComponent(Component, Resolvable, BaseModel):
    """
    Detailed description of what this component does.

    This component monitors X and triggers Y when Z happens.
    """

    # Configuration fields
    sensor_name: str = Field(description="Name of the sensor")
    webhook_url: str = Field(description="Webhook URL to monitor")
    job_name: str = Field(description="Job to trigger")
    minimum_interval_seconds: int = Field(
        default=30,
        description="Check interval in seconds"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions from this component."""

        # Capture fields for closure
        webhook_url = self.webhook_url
        job_name = self.job_name

        def sensor_fn(sensor_context: SensorEvaluationContext):
            """Sensor evaluation function."""
            # Your sensor logic here
            # Use cursor for state: sensor_context.cursor
            # Update cursor: sensor_context.update_cursor(new_value)
            # Yield RunRequest or return SkipReason

            return SkipReason("No events to process")

        sensor = SensorDefinition(
            name=self.sensor_name,
            evaluation_fn=sensor_fn,
            job_name=job_name,
            minimum_interval_seconds=self.minimum_interval_seconds,
            default_status=DefaultSensorStatus.RUNNING,
        )

        return Definitions(sensors=[sensor])
```

**Best Practices:**

- Use type hints for all parameters
- Add clear docstrings
- Capture Pydantic fields in closures for sensor/asset functions
- Handle errors gracefully
- Use `sensor_context.cursor` for stateful tracking
- Log informative messages with `sensor_context.log.info()`
- Support environment variables with `os.getenv()`

### 4. Create schema.json

Define the component schema for UI rendering:

```json
{
  "type": "dagster_component_templates.MyComponent",
  "name": "My Component Name",
  "category": "sensors",
  "description": "One-line description of what this component does",
  "version": "1.0.0",
  "author": "Your Name or Organization",
  "attributes": {
    "sensor_name": {
      "type": "string",
      "required": true,
      "label": "Sensor Name",
      "description": "Unique identifier for this sensor",
      "default": "my_sensor",
      "placeholder": "my_webhook_sensor"
    },
    "webhook_url": {
      "type": "string",
      "required": true,
      "label": "Webhook URL",
      "description": "Full URL of the webhook to monitor",
      "placeholder": "https://example.com/webhook",
      "sensitive": false
    },
    "api_token": {
      "type": "string",
      "required": false,
      "label": "API Token",
      "description": "Authentication token (use ${TOKEN} for env vars)",
      "placeholder": "${MY_API_TOKEN}",
      "sensitive": true
    },
    "interval_seconds": {
      "type": "number",
      "required": false,
      "label": "Check Interval",
      "description": "How often to check for events (seconds)",
      "default": 30,
      "min": 10,
      "max": 3600
    },
    "enable_feature": {
      "type": "boolean",
      "required": false,
      "label": "Enable Feature X",
      "description": "Whether to enable this optional feature",
      "default": false
    }
  },
  "outputs": [],
  "dependencies": {
    "pip": ["requests>=2.28.0", "pydantic>=2.0.0"]
  },
  "tags": ["webhook", "notification", "http"]
}
```

**Field Types:**

- `string` - Text input
- `number` - Numeric input
- `boolean` - Checkbox
- `array` - List of values
- `object` - Nested configuration

**Field Properties:**

- `required` (boolean) - Is this field mandatory?
- `label` (string) - Display label in UI
- `description` (string) - Help text
- `default` - Default value
- `placeholder` (string) - Placeholder text
- `sensitive` (boolean) - Hide value in UI (for passwords/tokens)
- `min/max` (number) - For numeric fields
- `enum` (array) - List of allowed values

### 5. Write README.md

Document your component:

```markdown
# My Component Name

Brief description of what this component does and when to use it.

## Features

- Feature 1
- Feature 2
- Feature 3

## Configuration

### Required Parameters

- **sensor_name** (string) - Description
- **webhook_url** (string) - Description

### Optional Parameters

- **api_token** (string) - Description (default: none)
- **interval_seconds** (number) - Description (default: 30)

## Usage Example

\`\`\`yaml
type: dagster_component_templates.MyComponent
attributes:
  sensor_name: my_webhook_sensor
  webhook_url: https://api.example.com/webhook
  api_token: ${MY_API_TOKEN}
  interval_seconds: 60
\`\`\`

## Environment Variables

This component uses the following environment variables:

- `MY_API_TOKEN` - API authentication token

## Requirements

### Python Packages

- requests >= 2.28.0
- pydantic >= 2.0.0

### External Services

- Access to webhook endpoint
- Valid API credentials

## How It Works

Detailed explanation of the component's behavior:

1. Step 1
2. Step 2
3. Step 3

## Limitations

- Limitation 1
- Limitation 2

## Troubleshooting

### Issue: Connection timeout

**Solution:** Check network connectivity and firewall rules.

### Issue: Authentication failed

**Solution:** Verify API token is correct and has required permissions.

## Contributing

Found a bug or have a feature request? Open an issue or submit a PR!

## License

MIT
```

### 6. Create example.yaml

Provide a working example configuration:

```yaml
type: dagster_component_templates.MyComponent
attributes:
  sensor_name: my_webhook_sensor
  webhook_url: https://api.example.com/webhook
  api_token: ${MY_API_TOKEN}
  job_name: process_webhook_job
  interval_seconds: 60
```

### 7. Update manifest.json

Add your component to the manifest:

```json
{
  "version": "1.0.0",
  "components": [
    {
      "id": "my_component",
      "name": "My Component Name",
      "category": "sensors",
      "description": "Brief description",
      "version": "1.0.0",
      "author": "Your Name",
      "path": "sensors/my_component",
      "tags": ["webhook", "http"],
      "dependencies": {
        "pip": ["requests>=2.28.0"]
      }
    }
  ]
}
```

**Manifest Fields:**

- `id` - Unique identifier (snake_case)
- `name` - Display name
- `category` - Category (sensors, assets, resources, factories)
- `description` - Brief description
- `version` - Semantic version (e.g., "1.0.0")
- `author` - Your name or organization
- `path` - Relative path to component directory
- `tags` - Searchable keywords
- `dependencies` - Python package requirements

### 8. Add Tests (Optional)

Create `test_component.py`:

```python
from dagster import build_sensor_context
from .component import MyComponent


def test_my_component():
    """Test component instantiation."""
    component = MyComponent(
        sensor_name="test_sensor",
        webhook_url="https://example.com",
        job_name="test_job"
    )
    assert component.sensor_name == "test_sensor"


def test_sensor_evaluation():
    """Test sensor evaluation logic."""
    component = MyComponent(
        sensor_name="test_sensor",
        webhook_url="https://example.com",
        job_name="test_job"
    )

    defs = component.build_defs(None)
    assert len(defs.sensors) == 1

    sensor = defs.sensors[0]
    context = build_sensor_context()
    result = sensor.evaluate_tick(context)
    # Assert expected behavior
```

## Pull Request Guidelines

### PR Title Format

```
[Category] Component Name - Brief description
```

Examples:
- `[Sensors] Slack Notification - Add Slack webhook sensor`
- `[Assets] CSV Ingestion - Add CSV file loader`

### PR Description Template

```markdown
## Component Name

Brief description of what this component does.

## Category

- [ ] Sensors
- [ ] Assets
- [ ] Resources
- [ ] Factories

## Checklist

- [ ] Added component.py
- [ ] Added schema.json
- [ ] Added README.md
- [ ] Added example.yaml
- [ ] Updated manifest.json
- [ ] Tested component locally
- [ ] Added tests (optional)

## Testing

Describe how you tested this component:

1. Created test project
2. Configured component with example.yaml
3. Verified sensor triggers correctly

## Screenshots (if applicable)

Add screenshots showing the component in action.

## Additional Notes

Any other relevant information.
```

## Review Process

1. Maintainer reviews code for quality and safety
2. Maintainer tests component functionality
3. Feedback provided via PR comments
4. Once approved, component is merged

## Code Quality Standards

- Follow PEP 8 style guide
- Type hints for all functions
- Clear, descriptive variable names
- Comprehensive error handling
- No hardcoded credentials
- Support environment variables for sensitive data

## Security Considerations

- Never commit secrets or credentials
- Use environment variables for sensitive data
- Validate all user inputs
- Document required permissions
- Be mindful of rate limits and API quotas

## Questions?

Open an issue or reach out to the maintainers!
