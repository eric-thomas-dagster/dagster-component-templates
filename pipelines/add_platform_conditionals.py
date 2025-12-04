#!/usr/bin/env python3
"""
Add show_if conditions to source system parameters so they only show
when the relevant platform is selected.
"""

import json

def add_platform_conditionals():
    """Add show_if conditions for all platform-specific parameters"""
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)

    # Define which parameters belong to which platform
    platform_params = {
        'shopify': ['shopify_shop_domain', 'shopify_access_token'],
        'woocommerce': ['woocommerce_url', 'woocommerce_consumer_key', 'woocommerce_consumer_secret'],
        'magento': ['magento_url', 'magento_access_token'],
        'bigcommerce': ['bigcommerce_store_hash', 'bigcommerce_access_token'],
        'stripe': ['stripe_api_key']
    }

    for pipeline in manifest['pipelines']:
        params = pipeline['pipeline_params']

        # Check if this pipeline has an ecommerce_platform or similar selector
        platform_selector = None
        if 'ecommerce_platform' in params:
            platform_selector = 'ecommerce_platform'
        elif 'data_source' in params:
            platform_selector = 'data_source'
        elif 'source_platform' in params:
            platform_selector = 'source_platform'

        if not platform_selector:
            continue

        # Add show_if to all platform-specific parameters
        for platform, param_names in platform_params.items():
            for param_name in param_names:
                if param_name in params:
                    # Add show_if condition
                    params[param_name]['show_if'] = {
                        platform_selector: platform
                    }
                    print(f"  ✓ {pipeline['id']}: {param_name} -> show_if {platform_selector}={platform}")

        # Handle Google Ads parameters
        if 'google_ads_customer_id' in params:
            params['google_ads_customer_id']['show_if'] = {platform_selector: 'google_ads'}
        if 'google_ads_developer_token' in params:
            params['google_ads_developer_token']['show_if'] = {platform_selector: 'google_ads'}

        # Handle Facebook Ads parameters
        if 'facebook_access_token' in params:
            params['facebook_access_token']['show_if'] = {platform_selector: 'facebook_ads'}
        if 'facebook_ad_account_id' in params:
            params['facebook_ad_account_id']['show_if'] = {platform_selector: 'facebook_ads'}

        # Handle HubSpot parameters
        if 'hubspot_api_key' in params:
            params['hubspot_api_key']['show_if'] = {platform_selector: 'hubspot'}

        # Handle Zendesk parameters
        if 'zendesk_subdomain' in params:
            params['zendesk_subdomain']['show_if'] = {platform_selector: 'zendesk'}
        if 'zendesk_email' in params:
            params['zendesk_email']['show_if'] = {platform_selector: 'zendesk'}
        if 'zendesk_api_token' in params:
            params['zendesk_api_token']['show_if'] = {platform_selector: 'zendesk'}

    # Save updated manifest
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    print("\n✅ Platform conditionals added!")
    print("Now parameters will only show when their platform is selected.")

if __name__ == '__main__':
    add_platform_conditionals()
