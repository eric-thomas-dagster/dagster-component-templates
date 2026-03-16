# Stripe Resource

Register a `StripeResource` wrapping the Stripe Python SDK for use by other Dagster components.

## Installation

```bash
pip install stripe
```

## Configuration

```yaml
type: dagster_component_templates.StripeResourceComponent
attributes:
  resource_key: stripe_resource  # key other components use
  api_key_env_var: STRIPE_API_KEY  # env var name holding the secret key
```

## Usage in other components

Once placed on the canvas, other components can reference this resource:

```yaml
attributes:
  resource_key: stripe_resource
```

## Authentication

`api_key_env_var` accepts an **environment variable name** (not the secret value itself).
Set the corresponding environment variable before running Dagster:

```bash
export STRIPE_API_KEY="sk_live_XXXXXXXXXXXXXXXXXXXX"
```

Use `sk_test_...` for development and `sk_live_...` for production. Obtain keys from the Stripe Dashboard under Developers > API Keys.
