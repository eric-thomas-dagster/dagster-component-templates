#!/usr/bin/env python3
"""
Generate comprehensive READMEs for ingestion components including database destination docs.
"""

import json
from pathlib import Path
from typing import Dict, Any

# Component metadata
COMPONENT_INFO = {
    "shopify_ingestion": {
        "title": "Shopify Ingestion",
        "description": "Ingest e-commerce data from Shopify (customers, orders, products) using dlt's verified Shopify source.",
        "use_cases": [
            "E-commerce Analytics: Track sales, orders, customer behavior",
            "Inventory Management: Monitor product stock and variants",
            "Customer Insights: Analyze customer purchase patterns",
            "Revenue Analysis: Calculate metrics like AOV, LTV",
            "Marketing Attribution: Connect orders to marketing campaigns"
        ],
        "data_types": "Customers, Orders, Products, Transactions, Inventory"
    },
    "stripe_ingestion": {
        "title": "Stripe Ingestion",
        "description": "Ingest payment and subscription data from Stripe (customers, charges, subscriptions, invoices) using dlt's verified Stripe source.",
        "use_cases": [
            "Payment Analytics: Track charges, refunds, payment methods",
            "Subscription Metrics: Calculate MRR, churn, LTV",
            "Revenue Recognition: Financial reporting and forecasting",
            "Customer Billing: Analyze payment patterns and failures",
            "Fraud Detection: Monitor suspicious transaction patterns"
        ],
        "data_types": "Customers, Charges, Subscriptions, Invoices, Payment Intents, Balance Transactions"
    },
    "salesforce_ingestion": {
        "title": "Salesforce Ingestion",
        "description": "Ingest CRM data from Salesforce (accounts, contacts, opportunities, leads) using dlt's Salesforce source.",
        "use_cases": [
            "Sales Pipeline: Track deals through sales stages",
            "Lead Management: Monitor lead conversion rates",
            "Customer Relationship: Build complete customer profiles",
            "Sales Performance: Analyze rep performance and quotas",
            "Forecasting: Predict revenue based on pipeline"
        ],
        "data_types": "Accounts, Contacts, Opportunities, Leads, Activities, Tasks"
    },
    "hubspot_ingestion": {
        "title": "HubSpot Ingestion",
        "description": "Ingest marketing and CRM data from HubSpot (contacts, companies, deals, tickets) using dlt's HubSpot source.",
        "use_cases": [
            "Marketing Analytics: Track campaigns, forms, email performance",
            "Lead Scoring: Analyze engagement and conversion",
            "Sales Enablement: Connect marketing to sales pipeline",
            "Customer Support: Monitor ticket volume and resolution",
            "Attribution: Multi-touch attribution modeling"
        ],
        "data_types": "Contacts, Companies, Deals, Tickets, Campaigns, Forms, Emails"
    },
    "google_ads_ingestion": {
        "title": "Google Ads Ingestion",
        "description": "Ingest advertising data from Google Ads (campaigns, ad groups, ads, keywords) using dlt's Google Ads source.",
        "use_cases": [
            "Ad Performance: Track clicks, impressions, conversions",
            "Budget Optimization: Analyze spend efficiency by campaign",
            "Keyword Analysis: Find high-performing keywords",
            "Quality Score: Monitor ad relevance and CTR",
            "ROAS Calculation: Measure return on ad spend"
        ],
        "data_types": "Campaigns, Ad Groups, Ads, Keywords, Performance Metrics"
    },
    "facebook_ads_ingestion": {
        "title": "Facebook Ads Ingestion",
        "description": "Ingest advertising data from Facebook/Meta Ads (campaigns, ad sets, ads) using dlt's Facebook Ads source.",
        "use_cases": [
            "Social Ad Performance: Track engagement and conversions",
            "Audience Insights: Analyze demographic performance",
            "Creative Testing: A/B test ad creatives",
            "Budget Management: Optimize spend across campaigns",
            "Attribution: Connect social ads to conversions"
        ],
        "data_types": "Campaigns, Ad Sets, Ads, Creative, Insights, Conversions"
    },
    "zendesk_ingestion": {
        "title": "Zendesk Ingestion",
        "description": "Ingest support ticket data from Zendesk (tickets, users, organizations) using dlt's Zendesk source.",
        "use_cases": [
            "Support Analytics: Track ticket volume and resolution times",
            "Customer Satisfaction: Monitor CSAT and NPS scores",
            "Agent Performance: Analyze response times and workload",
            "SLA Compliance: Ensure service level agreements are met",
            "Issue Trends: Identify common problems and FAQ opportunities"
        ],
        "data_types": "Tickets, Users, Organizations, Ticket Metrics, Satisfaction Ratings"
    },
    "jira_ingestion": {
        "title": "Jira Ingestion",
        "description": "Ingest project management data from Jira (issues, projects, sprints) using dlt's Jira source.",
        "use_cases": [
            "Sprint Metrics: Track velocity, burndown, cycle time",
            "Project Management: Monitor project progress and blockers",
            "Team Performance: Analyze throughput and work distribution",
            "Bug Tracking: Monitor defect trends and resolution",
            "Resource Planning: Forecast capacity and timeline"
        ],
        "data_types": "Issues, Projects, Sprints, Workflows, Comments"
    },
    "github_ingestion": {
        "title": "GitHub Ingestion",
        "description": "Ingest development data from GitHub (repos, pull requests, issues, commits) using dlt's GitHub source.",
        "use_cases": [
            "DevOps Metrics: Track deployment frequency and lead time",
            "Code Review: Monitor PR review times and approval rates",
            "Issue Management: Analyze bug reports and feature requests",
            "Contributor Activity: Track commits and contributions",
            "Repository Health: Monitor activity and maintenance"
        ],
        "data_types": "Repositories, Pull Requests, Issues, Commits, Contributors"
    },
    "slack_ingestion": {
        "title": "Slack Ingestion",
        "description": "Ingest communication data from Slack (messages, channels, users) using dlt's Slack source.",
        "use_cases": [
            "Team Communication: Analyze message volume and patterns",
            "Channel Activity: Monitor engagement across channels",
            "Sentiment Analysis: Understand team morale and culture",
            "Knowledge Mining: Extract insights from conversations",
            "Compliance: Archive messages for regulatory requirements"
        ],
        "data_types": "Messages, Channels, Users, Threads, Reactions"
    },
    "notion_ingestion": {
        "title": "Notion Ingestion",
        "description": "Ingest workspace data from Notion (pages, databases, blocks) using dlt's Notion source.",
        "use_cases": [
            "Knowledge Base: Index and search documentation",
            "Project Tracking: Sync project databases",
            "Content Management: Track page updates and versions",
            "Team Collaboration: Monitor workspace activity",
            "Data Export: Backup Notion workspace data"
        ],
        "data_types": "Pages, Databases, Blocks, Users, Comments"
    },
    "airtable_ingestion": {
        "title": "Airtable Ingestion",
        "description": "Ingest database data from Airtable (tables, records, views) using dlt's Airtable source.",
        "use_cases": [
            "Data Sync: Keep Airtable data in your warehouse",
            "Custom CRM: Ingest custom database structures",
            "Project Management: Track tasks and projects",
            "Content Catalog: Manage content inventory",
            "Operations: Sync operational databases"
        ],
        "data_types": "Tables, Records, Attachments, Linked Records"
    },
    "asana_ingestion": {
        "title": "Asana Ingestion",
        "description": "Ingest project management data from Asana (tasks, projects, teams) using dlt's Asana source.",
        "use_cases": [
            "Task Management: Track task completion and assignments",
            "Project Tracking: Monitor project progress and milestones",
            "Team Productivity: Analyze work patterns and capacity",
            "Portfolio Reporting: Aggregate multi-project metrics",
            "Time Tracking: Analyze task duration and estimates"
        ],
        "data_types": "Tasks, Projects, Teams, Tags, Portfolios"
    },
    "pipedrive_ingestion": {
        "title": "Pipedrive Ingestion",
        "description": "Ingest sales CRM data from Pipedrive (deals, contacts, organizations) using dlt's Pipedrive source.",
        "use_cases": [
            "Sales Pipeline: Track deals through sales stages",
            "Lead Management: Monitor lead sources and conversion",
            "Sales Forecasting: Predict revenue from pipeline",
            "Rep Performance: Analyze sales team productivity",
            "Activity Tracking: Monitor calls, meetings, emails"
        ],
        "data_types": "Deals, Persons, Organizations, Activities, Pipelines"
    },
    "freshdesk_ingestion": {
        "title": "Freshdesk Ingestion",
        "description": "Ingest support data from Freshdesk (tickets, contacts, agents) using dlt's Freshdesk source.",
        "use_cases": [
            "Support Metrics: Track ticket volume and resolution",
            "Customer Satisfaction: Monitor CSAT scores",
            "Agent Performance: Analyze response times",
            "SLA Compliance: Ensure service levels are met",
            "Issue Analytics: Identify common problems"
        ],
        "data_types": "Tickets, Contacts, Agents, Companies, Conversations"
    },
    "mongodb_ingestion": {
        "title": "MongoDB Ingestion",
        "description": "Ingest document data from MongoDB collections using dlt's MongoDB source.",
        "use_cases": [
            "Data Warehouse: Move MongoDB data to analytical database",
            "Backup: Create MongoDB data backups",
            "Analytics: Enable SQL analytics on MongoDB data",
            "Migration: Move data between MongoDB instances",
            "Reporting: Create business intelligence reports"
        ],
        "data_types": "Collections, Documents (any MongoDB schema)"
    },
    "google_sheets_ingestion": {
        "title": "Google Sheets Ingestion",
        "description": "Ingest spreadsheet data from Google Sheets using dlt's Google Sheets source.",
        "use_cases": [
            "Manual Data: Ingest manually-maintained spreadsheets",
            "Reference Data: Import lookup tables and configurations",
            "Reporting: Combine spreadsheet data with other sources",
            "Data Entry: Ingest user-submitted data",
            "Prototyping: Quick data pipeline for testing"
        ],
        "data_types": "Sheet data, cell values, formatting"
    },
    "google_analytics_ingestion": {
        "title": "Google Analytics 4 Ingestion",
        "description": "Ingest web analytics data from Google Analytics 4 using dlt's GA4 source.",
        "use_cases": [
            "Website Analytics: Track page views, sessions, users",
            "Conversion Tracking: Monitor goals and e-commerce",
            "User Behavior: Analyze user journeys and funnels",
            "Traffic Sources: Attribution and campaign performance",
            "Custom Reporting: Build custom dashboards"
        ],
        "data_types": "Events, Sessions, Users, Conversions, Traffic Sources"
    },
    "matomo_ingestion": {
        "title": "Matomo Ingestion",
        "description": "Ingest web analytics data from Matomo (formerly Piwik) using dlt's Matomo source.",
        "use_cases": [
            "Privacy-Focused Analytics: GDPR-compliant web tracking",
            "Website Performance: Track visits, actions, goals",
            "E-commerce: Monitor transactions and conversions",
            "Campaign Attribution: Analyze marketing effectiveness",
            "Custom Dimensions: Track business-specific metrics"
        ],
        "data_types": "Visits, Actions, Goals, E-commerce, Custom Dimensions"
    },
    "personio_ingestion": {
        "title": "Personio Ingestion",
        "description": "Ingest HR data from Personio (employees, attendance, time-off) using dlt's Personio source.",
        "use_cases": [
            "HR Analytics: Track headcount, turnover, diversity",
            "Workforce Planning: Forecast hiring needs",
            "Attendance Tracking: Monitor time-off and absences",
            "Compensation Analysis: Analyze salary and benefits",
            "Compliance: HR data for audits and reporting"
        ],
        "data_types": "Employees, Attendance, Time Off, Absences, Projects"
    },
    "workable_ingestion": {
        "title": "Workable Ingestion",
        "description": "Ingest recruiting data from Workable (candidates, jobs, applications) using dlt's Workable source.",
        "use_cases": [
            "Recruiting Metrics: Track time-to-hire, source effectiveness",
            "Pipeline Analytics: Monitor candidate progression",
            "Hiring Performance: Analyze recruiter productivity",
            "Source Attribution: Identify best candidate sources",
            "Diversity Tracking: Monitor inclusive hiring practices"
        ],
        "data_types": "Candidates, Jobs, Applications, Stages, Offers"
    }
}

def generate_readme(component_name: str, info: Dict[str, Any]) -> str:
    """Generate a comprehensive README for an ingestion component."""

    template = f"""# {info['title']} Component

{info['description']}

## Overview

This component uses [dlt (data load tool)](https://dlthub.com) to extract data from {info['title'].replace(' Ingestion', '')}.

dlt handles:
- ✅ API authentication and rate limiting
- ✅ Automatic pagination and incremental loading
- ✅ Schema evolution and data type inference
- ✅ Retry logic and error handling

## Use Cases

{chr(10).join(f"- **{uc.split(':')[0]}**:{uc.split(':')[1]}" for uc in info['use_cases'])}

## Data Types

**Available Resources:**
{info['data_types']}

## Output Modes

This component supports two output modes:

### 1. DataFrame Mode (Default)
Returns data as a pandas DataFrame for downstream processing in Dagster Designer.

```yaml
type: dagster_component_templates.{component_name.replace('_', ' ').title().replace(' ', '')}Component
attributes:
  asset_name: {component_name.replace('_ingestion', '_data')}
  # ... authentication parameters ...
```

### 2. Database Persistence Mode
Persist data directly to a database (Snowflake, BigQuery, Postgres, DuckDB, etc.) using dlt's destination capabilities.

**Option A: Persist Only**
```yaml
type: dagster_component_templates.{component_name.replace('_', ' ').title().replace(' ', '')}Component
attributes:
  asset_name: {component_name.replace('_ingestion', '_data')}
  destination: "snowflake"  # or bigquery, postgres, duckdb, etc.
  destination_config: "snowflake://user:pass@account/database/schema"
  persist_and_return: false  # Only persist, don't return DataFrame
  # ... authentication parameters ...
```

**Option B: Persist AND Return DataFrame**
```yaml
type: dagster_component_templates.{component_name.replace('_', ' ').title().replace(' ', '')}Component
attributes:
  asset_name: {component_name.replace('_ingestion', '_data')}
  destination: "snowflake"
  destination_config: "snowflake://user:pass@account/database/schema"
  persist_and_return: true  # Persist to DB AND return DataFrame
  # ... authentication parameters ...
```

## Configuration Parameters

### Database Destination Parameters (Optional)

- **`destination`** (string, optional): dlt destination name
  - Supported: `snowflake`, `bigquery`, `postgres`, `redshift`, `duckdb`, `motherduck`, `databricks`, `synapse`, `clickhouse`, and [more](https://dlthub.com/docs/dlt-ecosystem/destinations)
  - Default: Uses in-memory DuckDB and returns DataFrame

- **`destination_config`** (string, optional): Destination configuration
  - Format depends on destination (connection string, JSON config, or credentials file path)
  - Required if `destination` is set
  - Examples:
    - Postgres: `postgresql://user:pass@host:5432/database`
    - Snowflake: `snowflake://user:pass@account/database/schema`
    - BigQuery: Path to service account JSON or credentials dict

- **`persist_and_return`** (boolean, optional): Persistence behavior
  - `false` (default): Only persist to database, return metadata DataFrame
  - `true`: Persist to database AND return full DataFrame
  - Only applies when `destination` is set

### Standard Parameters

- **`asset_name`** (string, required): Name for the output asset
- **`description`** (string, optional): Asset description
- **`group_name`** (string, optional): Asset group for organization
- **`include_sample_metadata`** (boolean, optional): Include data preview in metadata (default: true)

### Source-Specific Parameters

See `schema.json` for complete list of authentication and configuration parameters specific to {info['title'].replace(' Ingestion', '')}.

## Destination Examples

### Snowflake

```yaml
attributes:
  destination: "snowflake"
  destination_config: |
    {{
      "credentials": {{
        "database": "analytics",
        "password": "${{SNOWFLAKE_PASSWORD}}",
        "username": "dlt_user",
        "host": "account.snowflakecomputing.com",
        "warehouse": "transforming",
        "role": "dlt_role"
      }}
    }}
  persist_and_return: false
```

### BigQuery

```yaml
attributes:
  destination: "bigquery"
  destination_config: "/path/to/service-account.json"
  persist_and_return: false
```

### Postgres

```yaml
attributes:
  destination: "postgres"
  destination_config: "postgresql://user:password@localhost:5432/analytics"
  persist_and_return: false
```

### DuckDB (Local File)

```yaml
attributes:
  destination: "duckdb"
  destination_config: "/path/to/analytics.duckdb"
  persist_and_return: true  # Can return DataFrame from local DB
```

## When to Use Each Mode

### Use DataFrame Mode When:
- Building data pipelines in Dagster Designer
- Chaining transformations (standardizers, analytics)
- Need to process data before storing
- Want visual workflow composition

### Use Database Persistence When:
- Direct data warehouse loading
- High-volume data (millions+ rows)
- Long-term storage and querying
- BI tool integration (Tableau, Looker, etc.)
- Production data pipelines

### Use Persist + Return When:
- Need both warehouse copy AND downstream processing
- Debugging/monitoring workflows
- Hybrid architectures (some data to warehouse, some to next step)

## Performance Considerations

- **DataFrame Mode**: Holds data in memory, suitable for up to ~10M rows
- **Persist Only**: Streams directly to destination, handles billions of rows
- **Persist + Return**: Loads twice (destination + memory), use selectively

## Authentication

Refer to `schema.json` and the [dlt documentation](https://dlthub.com/docs) for authentication requirements specific to {info['title'].replace(' Ingestion', '')}.

Common patterns:
- API keys via environment variables
- OAuth tokens
- Service account credentials
- Connection strings

## Dependencies

- `dlt[{component_name.replace('_ingestion', '')}]` - Installs dlt with {info['title'].replace(' Ingestion', '')} source
- `pandas>=1.5.0` - For DataFrame operations

## Notes

- **Incremental Loading**: dlt automatically tracks state for incremental loads
- **Schema Evolution**: dlt handles schema changes automatically
- **Rate Limiting**: dlt respects API rate limits automatically
- **Retries**: Built-in retry logic for transient failures
- **Destinations**: See [dlt destinations](https://dlthub.com/docs/dlt-ecosystem/destinations) for full list
- **Credentials**: Use environment variables for sensitive values (e.g., `${{VAR_NAME}}`)

## Learn More

- [dlt Documentation](https://dlthub.com/docs)
- [dlt {info['title'].replace(' Ingestion', '')} Source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/{component_name.replace('_ingestion', '')})
- [dlt Destinations](https://dlthub.com/docs/dlt-ecosystem/destinations)
"""

    return template.strip()

def main():
    """Generate READMEs for all ingestion components."""
    assets_dir = Path("/Users/ericthomas/dagster_components/dagster-component-templates/assets")

    for component_name, info in COMPONENT_INFO.items():
        readme_path = assets_dir / component_name / "README.md"

        # Skip if README already exists
        if readme_path.exists():
            print(f"⚠️  {component_name}: README already exists, skipping")
            continue

        # Generate README
        readme_content = generate_readme(component_name, info)
        readme_path.write_text(readme_content + "\n")
        print(f"✅ {component_name}: README created")

    print("\n✅ All ingestion READMEs generated!")

if __name__ == "__main__":
    main()
