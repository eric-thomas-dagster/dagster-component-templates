#!/usr/bin/env python3
"""
Add show_if conditions to all source-specific parameters across all pipelines.
"""

import json

def fix_all_conditionals():
    """Add missing show_if conditions"""
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)

    # Define which parameters belong to which selector values
    conditional_mappings = {
        # Ticket sources
        'ticket_source': {
            'zendesk': ['zendesk_subdomain', 'zendesk_email', 'zendesk_api_token'],
            'intercom': ['intercom_access_token', 'intercom_app_id'],
            'freshdesk': ['freshdesk_domain', 'freshdesk_api_key']
        },
        # Content sources
        'content_source': {
            's3': ['s3_bucket', 's3_prefix', 's3_access_key', 's3_secret_key'],
            'api': ['api_url', 'api_key', 'api_auth_header'],
            'database': ['database_connection_string', 'database_query']
        }
    }

    changes_made = []

    for pipeline in manifest['pipelines']:
        params = pipeline['pipeline_params']

        # Apply conditional mappings
        for selector_field, value_mappings in conditional_mappings.items():
            if selector_field not in params:
                continue

            for selector_value, param_names in value_mappings.items():
                for param_name in param_names:
                    if param_name in params:
                        # Add or update show_if
                        params[param_name]['show_if'] = {selector_field: selector_value}
                        changes_made.append(f"{pipeline['id']}: {param_name} -> show_if {selector_field}={selector_value}")

    # Save updated manifest
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    if changes_made:
        print("✅ Added show_if conditions:")
        for change in changes_made:
            print(f"  ✓ {change}")
    else:
        print("✓ No changes needed - all conditionals already set")

if __name__ == '__main__':
    fix_all_conditionals()
