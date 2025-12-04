#!/usr/bin/env python3
"""
Add environment-based configuration to all pipeline templates.
This allows different settings for local development, branch deployments, and production.
"""

import json

def add_environment_params():
    """Add environment parameter and environment-aware configuration to all pipelines"""
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)

    for pipeline in manifest['pipelines']:
        params = pipeline['pipeline_params']

        # Add environment selector at the top (should be one of the first params)
        if 'environment' not in params:
            # Insert environment param first
            environment_param = {
                "type": "string",
                "default": "local",
                "enum": ["local", "branch", "production"],
                "description": "Deployment environment (local=dev/testing, branch=staging, production=live)",
                "required": True,
                "order": 0  # Show first in UI
            }

            # Create new params dict with environment first
            new_params = {"environment": environment_param}
            new_params.update(params)
            pipeline['pipeline_params'] = new_params
            params = new_params

        # Update output_destination to have environment-aware defaults
        if 'output_destination' in params:
            params['output_destination']['description'] = (
                "Database destination for pipeline output. "
                "Recommended: DuckDB for local, Postgres/Snowflake for branch/production"
            )

            # Add environment-specific default suggestion
            params['output_destination']['environment_defaults'] = {
                "local": "duckdb",
                "branch": "postgres",
                "production": "snowflake"
            }

        # Add environment-specific credential sets
        # For local: can use empty/test credentials
        # For branch/production: require real credentials

        # Mark credential fields as environment-sensitive
        credential_fields = [
            'shopify_access_token', 'hubspot_api_key', 'google_ads_developer_token',
            'facebook_access_token', 'stripe_api_key', 'zendesk_api_token',
            'snowflake_password', 'bigquery_credentials_json', 'postgres_password',
            'redshift_password', 'databricks_token', 'motherduck_token'
        ]

        for field in credential_fields:
            if field in params:
                # Add hint about environment-specific values
                current_desc = params[field].get('description', '')
                if 'environment' not in current_desc.lower():
                    params[field]['description'] = (
                        f"{current_desc} "
                        "(Use test credentials for local, staging credentials for branch, "
                        "production credentials for production)"
                    ).strip()

                # Mark that validation can be skipped in local environment
                params[field]['required_in_local'] = False
                params[field]['required_in_branch'] = True
                params[field]['required_in_production'] = True

        # Add deployment name parameter (for branch deployments)
        if 'deployment_name' not in params:
            params['deployment_name'] = {
                "type": "string",
                "default": "",
                "description": "Deployment name (for branch deployments, e.g., 'feature-new-pipeline'). Leave empty for local/production.",
                "required": False,
                "show_if": {"environment": "branch"},
                "placeholder": "feature-branch-name",
                "order": 1
            }

        # Add environment-aware logging/monitoring
        if 'enable_verbose_logging' not in params:
            params['enable_verbose_logging'] = {
                "type": "boolean",
                "default": True,
                "description": "Enable detailed logging (recommended for local/branch, optional for production)",
                "required": False,
                "order": 99  # Show near the end
            }

        print(f"✓ {pipeline['id']}: Added environment support")

    # Save updated manifest
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    print("\n✅ Environment support added to all pipelines:")
    print("  - environment: Dropdown (local, branch, production)")
    print("  - deployment_name: For branch deployments")
    print("  - Environment-aware credential requirements")
    print("  - Environment-specific destination defaults")
    print("  - Verbose logging toggle")
    print()
    print("Environment Configuration:")
    print("  • Local: DuckDB, test credentials optional, verbose logging")
    print("  • Branch: Staging database, staging credentials required, named deployment")
    print("  • Production: Production database, production credentials required")

if __name__ == '__main__':
    add_environment_params()
