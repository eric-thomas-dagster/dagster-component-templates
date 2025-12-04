#!/usr/bin/env python3
"""
Add authentication and output destination parameters to all pipeline templates.
This ensures users can configure credentials and where data should be written.
"""

import json

# Common output destination parameters (for dlt_dataframe_writer)
OUTPUT_PARAMS = {
    "output_destination": {
        "type": "string",
        "default": "duckdb",
        "enum": ["duckdb", "snowflake", "bigquery", "postgres", "redshift", "databricks", "clickhouse", "motherduck"],
        "description": "Database destination for pipeline output (DuckDB for local development, production databases for deployment)",
        "required": True
    },
    "output_write_mode": {
        "type": "string",
        "default": "replace",
        "enum": ["replace", "append", "merge"],
        "description": "Write mode: replace (drop/recreate), append (add rows), merge (upsert)",
        "required": False
    }
}

# Authentication parameters by source system
AUTH_PARAMS = {
    "shopify": {
        "shopify_shop_domain": {
            "type": "string",
            "default": "",
            "description": "Your Shopify shop domain (e.g., mystore.myshopify.com)",
            "placeholder": "mystore.myshopify.com",
            "required": True,
            "sensitive": False
        },
        "shopify_access_token": {
            "type": "string",
            "default": "",
            "description": "Shopify Admin API access token",
            "required": True,
            "sensitive": True
        }
    },
    "hubspot": {
        "hubspot_api_key": {
            "type": "string",
            "default": "",
            "description": "HubSpot API key or private app token",
            "required": True,
            "sensitive": True
        }
    },
    "google_ads": {
        "google_ads_customer_id": {
            "type": "string",
            "default": "",
            "description": "Google Ads customer ID (without dashes)",
            "placeholder": "1234567890",
            "required": True,
            "sensitive": False
        },
        "google_ads_developer_token": {
            "type": "string",
            "default": "",
            "description": "Google Ads developer token",
            "required": True,
            "sensitive": True
        }
    },
    "facebook_ads": {
        "facebook_access_token": {
            "type": "string",
            "default": "",
            "description": "Facebook Ads API access token",
            "required": True,
            "sensitive": True
        },
        "facebook_ad_account_id": {
            "type": "string",
            "default": "",
            "description": "Facebook Ad Account ID",
            "placeholder": "act_123456789",
            "required": True,
            "sensitive": False
        }
    },
    "stripe": {
        "stripe_api_key": {
            "type": "string",
            "default": "",
            "description": "Stripe API secret key (starts with sk_)",
            "required": True,
            "sensitive": True
        }
    },
    "zendesk": {
        "zendesk_subdomain": {
            "type": "string",
            "default": "",
            "description": "Zendesk subdomain (e.g., mycompany from mycompany.zendesk.com)",
            "placeholder": "mycompany",
            "required": True,
            "sensitive": False
        },
        "zendesk_email": {
            "type": "string",
            "default": "",
            "description": "Zendesk admin email",
            "required": True,
            "sensitive": False
        },
        "zendesk_api_token": {
            "type": "string",
            "default": "",
            "description": "Zendesk API token",
            "required": True,
            "sensitive": True
        }
    }
}

# Map pipelines to their source systems
PIPELINE_SOURCES = {
    "customer_ltv_pipeline": ["shopify"],
    "marketing_attribution_pipeline": ["google_ads", "facebook_ads"],
    "churn_prevention_pipeline": ["hubspot", "zendesk"],
    "product_recommendations_pipeline": ["shopify"],
    "fraud_detection_pipeline": ["stripe"],
    "ab_testing_pipeline": [],  # Uses product analytics, handled separately
    "full_cdp_pipeline": ["hubspot", "shopify", "google_ads", "facebook_ads", "zendesk"],
    "customer_review_sentiment_pipeline": ["shopify"],
    "document_qa_rag_pipeline": [],  # File-based, no API auth
    "customer_support_automation": ["zendesk"],
    "content_moderation": []  # Database/API based, no specific auth
}

def add_enum_to_existing_params(pipeline):
    """Add enums to existing parameters that should be dropdowns"""
    params = pipeline.get('pipeline_params', {})

    # ecommerce_platform
    if 'ecommerce_platform' in params:
        params['ecommerce_platform']['enum'] = [
            "shopify", "woocommerce", "magento", "bigcommerce", "stripe"
        ]

    # classification_method, sentiment_method, etc.
    if 'classification_method' in params:
        if 'enum' not in params['classification_method']:
            params['classification_method']['enum'] = ["transformer", "llm", "zero_shot"]

    if 'sentiment_method' in params:
        if 'enum' not in params['sentiment_method']:
            params['sentiment_method']['enum'] = ["transformer", "llm"]

    if 'llm_provider' in params:
        if 'enum' not in params['llm_provider']:
            params['llm_provider']['enum'] = ["openai", "anthropic"]

def update_manifest():
    """Update manifest.json with auth and output params for all pipelines"""
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)

    for pipeline in manifest['pipelines']:
        pipeline_id = pipeline['id']

        # Initialize pipeline_params if it doesn't exist
        if 'pipeline_params' not in pipeline:
            pipeline['pipeline_params'] = {}

        params = pipeline['pipeline_params']

        # Add enums to existing params
        add_enum_to_existing_params(pipeline)

        # Add authentication parameters based on sources
        sources = PIPELINE_SOURCES.get(pipeline_id, [])
        for source in sources:
            if source in AUTH_PARAMS:
                for param_name, param_config in AUTH_PARAMS[source].items():
                    # Only add if not already present
                    if param_name not in params:
                        params[param_name] = param_config.copy()

        # Add output destination parameters (for all pipelines)
        for param_name, param_config in OUTPUT_PARAMS.items():
            if param_name not in params:
                params[param_name] = param_config.copy()

        print(f"✓ Updated {pipeline_id}")
        print(f"  - Auth sources: {', '.join(sources) if sources else 'none (file/database based)'}")
        print(f"  - Total params: {len(params)}")

    # Save updated manifest
    with open('manifest.json', 'w') as f:
        json.dump(manifest, f, indent=2)

    print("\n✅ All pipelines updated with:")
    print("  - Authentication parameters for source systems")
    print("  - Output destination configuration")
    print("  - Proper enums for dropdown fields")

if __name__ == '__main__':
    update_manifest()
