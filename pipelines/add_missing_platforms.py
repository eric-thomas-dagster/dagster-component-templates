#!/usr/bin/env python3
"""
Add configuration parameters for all ecommerce platforms that are in the enum
but don't have corresponding auth/config parameters.
"""

import json

def add_missing_platform_params():
    """Add missing platform parameters"""
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)

    # Define auth parameters for each platform
    platform_auth_params = {
        'woocommerce': {
            'woocommerce_url': {
                'type': 'string',
                'description': 'WooCommerce store URL (e.g., https://mystore.com)',
                'required': True,
                'sensitive': False,
                'placeholder': 'https://mystore.com',
                'environment_specific': True,
                'show_if': {'ecommerce_platform': 'woocommerce'}
            },
            'woocommerce_consumer_key': {
                'type': 'string',
                'description': 'WooCommerce REST API consumer key',
                'required': True,
                'sensitive': True,
                'environment_specific': True,
                'show_if': {'ecommerce_platform': 'woocommerce'}
            },
            'woocommerce_consumer_secret': {
                'type': 'string',
                'description': 'WooCommerce REST API consumer secret',
                'required': True,
                'sensitive': True,
                'environment_specific': True,
                'show_if': {'ecommerce_platform': 'woocommerce'}
            }
        },
        'magento': {
            'magento_url': {
                'type': 'string',
                'description': 'Magento store URL (e.g., https://mystore.com)',
                'required': True,
                'sensitive': False,
                'placeholder': 'https://mystore.com',
                'environment_specific': True,
                'show_if': {'ecommerce_platform': 'magento'}
            },
            'magento_access_token': {
                'type': 'string',
                'description': 'Magento REST API access token',
                'required': True,
                'sensitive': True,
                'environment_specific': True,
                'show_if': {'ecommerce_platform': 'magento'}
            }
        },
        'bigcommerce': {
            'bigcommerce_store_hash': {
                'type': 'string',
                'description': 'BigCommerce store hash (found in API Path)',
                'required': True,
                'sensitive': False,
                'placeholder': 'abc123xyz',
                'environment_specific': True,
                'show_if': {'ecommerce_platform': 'bigcommerce'}
            },
            'bigcommerce_access_token': {
                'type': 'string',
                'description': 'BigCommerce API access token',
                'required': True,
                'sensitive': True,
                'environment_specific': True,
                'show_if': {'ecommerce_platform': 'bigcommerce'}
            }
        }
    }

    for pipeline in manifest['pipelines']:
        params = pipeline['pipeline_params']

        # Only add to pipelines that have ecommerce_platform selector
        if 'ecommerce_platform' not in params:
            continue

        # Get current enum values
        enum_values = params['ecommerce_platform'].get('enum', [])

        # Add missing parameters for each platform in enum
        for platform in enum_values:
            if platform in platform_auth_params:
                for param_name, param_config in platform_auth_params[platform].items():
                    if param_name not in params:
                        params[param_name] = param_config
                        print(f"  ✓ {pipeline['id']}: Added {param_name} for {platform}")

    # Save updated manifest
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    print("\n✅ Missing platform parameters added!")
    print("All ecommerce platforms now have proper configuration fields.")

if __name__ == '__main__':
    add_missing_platform_params()
