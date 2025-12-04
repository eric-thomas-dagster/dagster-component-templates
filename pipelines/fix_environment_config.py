#!/usr/bin/env python3
"""
Fix environment configuration to support proper multi-environment setup.

Users should be able to configure all environments at once:
- Local: DuckDB + test credentials
- Branch: Postgres + staging credentials
- Production: Snowflake + production credentials

The configuration structure will be:
{
  "shared": {
    "ecommerce_platform": "shopify",
    "prediction_period_months": 24
  },
  "environments": {
    "local": {
      "output_destination": "duckdb",
      "shopify_access_token": "test_token"
    },
    "branch": {
      "output_destination": "postgres",
      "shopify_access_token": "staging_token"
    },
    "production": {
      "output_destination": "snowflake",
      "shopify_access_token": "prod_token"
    }
  }
}
"""

import json

def fix_environment_params():
    """Update parameters to support multi-environment configuration"""
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)

    # Parameters that should be environment-specific (vary by environment)
    environment_specific_params = {
        # Output/destination
        'output_destination', 'output_write_mode', 'output_table_name',

        # DuckDB
        'duckdb_path',

        # Snowflake
        'snowflake_account', 'snowflake_user', 'snowflake_password',
        'snowflake_database', 'snowflake_schema', 'snowflake_warehouse',

        # BigQuery
        'bigquery_project_id', 'bigquery_dataset', 'bigquery_credentials_json',

        # Postgres
        'postgres_host', 'postgres_port', 'postgres_database',
        'postgres_user', 'postgres_password', 'postgres_schema',

        # Redshift
        'redshift_host', 'redshift_port', 'redshift_database',
        'redshift_user', 'redshift_password', 'redshift_schema',

        # Databricks
        'databricks_host', 'databricks_token', 'databricks_http_path',
        'databricks_catalog', 'databricks_schema',

        # MotherDuck
        'motherduck_token', 'motherduck_database',

        # Source credentials (typically different per environment)
        'shopify_shop_domain', 'shopify_access_token',
        'hubspot_api_key',
        'google_ads_customer_id', 'google_ads_developer_token',
        'facebook_access_token', 'facebook_ad_account_id',
        'stripe_api_key',
        'zendesk_subdomain', 'zendesk_email', 'zendesk_api_token',

        # Environment-specific settings
        'enable_verbose_logging', 'deployment_name'
    }

    for pipeline in manifest['pipelines']:
        params = pipeline['pipeline_params']

        # Remove the single environment dropdown
        params.pop('environment', None)

        # Mark parameters as environment-specific or shared
        for param_name, param_config in params.items():
            if param_name in environment_specific_params:
                param_config['environment_specific'] = True

                # Add environment-specific defaults if not present
                if 'environment_defaults' not in param_config:
                    # Add sensible defaults based on parameter type
                    if 'output_destination' in param_name:
                        param_config['environment_defaults'] = {
                            'local': 'duckdb',
                            'branch': 'postgres',
                            'production': 'snowflake'
                        }
                    elif 'enable_verbose_logging' in param_name:
                        param_config['environment_defaults'] = {
                            'local': True,
                            'branch': True,
                            'production': False
                        }
                    elif 'write_mode' in param_name:
                        param_config['environment_defaults'] = {
                            'local': 'replace',
                            'branch': 'replace',
                            'production': 'append'
                        }
            else:
                # Shared parameters (same across all environments)
                param_config['environment_specific'] = False

        print(f"✓ {pipeline['id']}")
        env_specific_count = sum(1 for p in params.values() if p.get('environment_specific'))
        shared_count = len(params) - env_specific_count
        print(f"  - Environment-specific: {env_specific_count} params")
        print(f"  - Shared: {shared_count} params")

    # Save updated manifest
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    print("\n✅ Environment configuration fixed!")
    print("\n📋 Configuration Structure:")
    print("   {")
    print('     "shared": {')
    print('       "ecommerce_platform": "shopify",')
    print('       "prediction_period_months": 24')
    print('     },')
    print('     "environments": {')
    print('       "local": {')
    print('         "output_destination": "duckdb",')
    print('         "shopify_access_token": "test_token"')
    print('       },')
    print('       "branch": {')
    print('         "output_destination": "postgres",')
    print('         "postgres_host": "staging-db.company.com",')
    print('         "shopify_access_token": "staging_token"')
    print('       },')
    print('       "production": {')
    print('         "output_destination": "snowflake",')
    print('         "snowflake_account": "xy12345",')
    print('         "shopify_access_token": "prod_token"')
    print('       }')
    print('     }')
    print('   }')

if __name__ == '__main__':
    fix_environment_params()
