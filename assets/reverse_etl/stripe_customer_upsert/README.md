# `StripeCustomerUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **Stripe customers**.

Stripe doesn't have a native "upsert" endpoint (unlike HubSpot), and neither email nor any custom field is globally unique on Stripe customers — you can technically have multiple customers with the same email in one account. Two lookup strategies, chosen per your data's guarantees:

1. **`match_by: email`** (default) — uses `customers.list?email=X`. Immediately consistent (list index is real-time). Best when your data guarantees one customer per email.
2. **`match_by: metadata`** — uses `customers/search` with `metadata['dagster_key']:'X'`. More precise but Stripe's Search API is eventually consistent (indexing lag can be 30s+ after create). Best when multiple customers legitimately share an email.

Every managed customer gets `metadata.dagster_key = <key_column value>` stamped on it either way, for observability and fallback lookup.

## When to use

- Sync computed customer data (plan tier, health score, CRM fields) from a warehouse INTO Stripe customer records so billing/finance sees it in context.

## Pairs with

- **`stripe`** resource — connection (required).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `resource_key` | optional (default `stripe`) | Resource key registered by StripeResourceComponent. |
| `key_column` | required | Upstream column holding a stable unique key. |
| `email_column` | required | Column holding the customer email. |
| `name_column` | optional | Column holding the customer name. |
| `description_column` | optional | Column holding the customer description. |
| `extra_metadata_columns` | optional | Additional columns written into the customer's metadata dict. |
| `metadata_key_field` | optional (default `dagster_key`) | Name of the metadata field used to store the dedup key. |
| `match_by` | optional (default `email`) | 'email' or 'metadata'. |
| `batch_size` | optional (default `100`) | Max upstream rows per run. |

## Example
```yaml
type: dagster_component_templates.StripeCustomerUpsertComponent
attributes:
  asset_name: stripe_customers_mirror
  upstream_asset_key: customers_seed
  resource_key: stripe
  key_column: customer_id
  email_column: email
  name_column: full_name
  description_column: notes
  extra_metadata_columns: [source, plan_tier]
```
