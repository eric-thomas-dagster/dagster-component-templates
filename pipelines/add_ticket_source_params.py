#!/usr/bin/env python3
"""
Add missing authentication parameters for ticket sources.
"""

import json

def add_ticket_source_params():
    """Add missing ticket source auth parameters"""
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)

    # Define auth parameters for each ticket source
    ticket_source_params = {
        'intercom': {
            'intercom_access_token': {
                'type': 'string',
                'description': 'Intercom API access token',
                'required': True,
                'sensitive': True,
                'environment_specific': True,
                'show_if': {'ticket_source': 'intercom'}
            },
            'intercom_app_id': {
                'type': 'string',
                'description': 'Intercom application ID',
                'required': True,
                'sensitive': False,
                'placeholder': 'abc123xyz',
                'environment_specific': True,
                'show_if': {'ticket_source': 'intercom'}
            }
        },
        'freshdesk': {
            'freshdesk_domain': {
                'type': 'string',
                'description': 'Freshdesk domain (e.g., yourcompany.freshdesk.com)',
                'required': True,
                'sensitive': False,
                'placeholder': 'yourcompany.freshdesk.com',
                'environment_specific': True,
                'show_if': {'ticket_source': 'freshdesk'}
            },
            'freshdesk_api_key': {
                'type': 'string',
                'description': 'Freshdesk API key',
                'required': True,
                'sensitive': True,
                'environment_specific': True,
                'show_if': {'ticket_source': 'freshdesk'}
            }
        }
    }

    for pipeline in manifest['pipelines']:
        params = pipeline['pipeline_params']

        # Only add to pipelines that have ticket_source selector
        if 'ticket_source' not in params:
            continue

        # Get current enum values
        enum_values = params['ticket_source'].get('enum', [])

        # Add missing parameters for each source in enum
        for source in enum_values:
            if source in ticket_source_params:
                for param_name, param_config in ticket_source_params[source].items():
                    if param_name not in params:
                        params[param_name] = param_config
                        print(f"  ✓ {pipeline['id']}: Added {param_name} for {source}")

    # Save updated manifest
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    print("\n✅ Ticket source parameters added!")

if __name__ == '__main__':
    add_ticket_source_params()
