# E-commerce Standardizer Component

Standardize e-commerce data across platforms (Shopify, Stripe, WooCommerce) into a unified schema.

## Overview

Different e-commerce platforms structure transaction, product, and customer data differently. This component normalizes e-commerce data into a consistent format for unified analytics.

**Supported Platforms:**
- Shopify
- Stripe
- WooCommerce

**Supported Resource Types:**
- Orders
- Products
- Customers

## Use Cases

- **Multi-Store Analytics**: Analyze sales across multiple stores/platforms
- **Platform Migration**: Maintain historical data when switching platforms
- **Revenue Reporting**: Unified financial reporting across all channels
- **Customer Insights**: Cross-platform customer behavior analysis
- **Inventory Management**: Combined product catalog management

## Input Requirements

Raw e-commerce data with platform-specific schemas:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `_resource_type` | string | ✓ | resource_type, object_type | Resource type (orders, products, customers) |
| `id` | string | ✓ | order_id, product_id, customer_id | Record identifier |
| `created_at` | datetime | | order_date, created, date | Record creation date |

**Compatible Upstream Components:**
- `shopify_ingestion`
- `stripe_ingestion`

## Output Schema

Standardized e-commerce data with unified fields:

| Column | Type | Description |
|--------|------|-------------|
| `resource_type` | string | orders, products, customers |
| `platform` | string | Source platform (shopify, stripe, woocommerce) |
| `id` | string | Unique record identifier |
| `customer_id` | string | Customer identifier |
| `email` | string | Customer email address |
| `total` | number | Order total/product price |
| `status` | string | Order status (pending, processing, completed, cancelled) |
| `currency` | string | Currency code (USD, EUR, etc.) |
| `created_at` | datetime | Record creation timestamp |
| `updated_at` | datetime | Last update timestamp |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for standardized output asset
- **`platform`** (string): Source e-commerce platform
  - `shopify`
  - `stripe`
  - `woocommerce`
- **`resource_type`** (string): Type of e-commerce resource
  - `orders`
  - `products`
  - `customers`

### Optional Parameters

- **`source_asset`** (string): Upstream asset with raw data (auto-set via lineage)
- **`order_id_field`** (string): Custom field for order ID (auto-detected)
- **`customer_id_field`** (string): Custom field for customer ID (auto-detected)
- **`total_field`** (string): Custom field for total amount (auto-detected)
- **`status_field`** (string): Custom field for status (auto-detected)
- **`filter_status`** (string): Filter by status (comma-separated)
- **`filter_date_from`** (string): Start date filter (YYYY-MM-DD)
- **`filter_date_to`** (string): End date filter (YYYY-MM-DD)
- **`convert_currency`** (string): Convert all amounts to this currency
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `ecommerce`)
- **`include_sample_metadata`** (boolean): Include data preview (default: true)

## Example Configuration

### Shopify Orders Standardization

```yaml
type: dagster_component_templates.EcommerceStandardizerComponent
attributes:
  asset_name: standardized_shopify_orders
  platform: shopify
  resource_type: orders
  description: Standardized Shopify order data
  group_name: ecommerce
```

### Stripe Payments Standardization

```yaml
type: dagster_component_templates.EcommerceStandardizerComponent
attributes:
  asset_name: standardized_stripe_charges
  platform: stripe
  resource_type: orders
  filter_status: "succeeded"
  convert_currency: "USD"
  description: Successful Stripe charges in USD
```

### Multi-Platform Orders

```yaml
# shopify_orders.yaml
type: dagster_component_templates.EcommerceStandardizerComponent
attributes:
  asset_name: std_shopify_orders
  platform: shopify
  resource_type: orders
  convert_currency: "USD"

---
# stripe_orders.yaml
type: dagster_component_templates.EcommerceStandardizerComponent
attributes:
  asset_name: std_stripe_orders
  platform: stripe
  resource_type: orders
  convert_currency: "USD"

---
# Combine downstream
type: dagster_component_templates.DataFrameCombinerComponent
attributes:
  asset_name: all_orders
  source_assets: ["std_shopify_orders", "std_stripe_orders"]
```

## Platform-Specific Mappings

### Shopify Orders
- `id` → `id`
- `customer.id` → `customer_id`
- `customer.email` → `email`
- `total_price` → `total`
- `financial_status` → `status`
- `currency` → `currency`
- `created_at` → `created_at`
- `updated_at` → `updated_at`

### Stripe Charges/PaymentIntents
- `id` → `id`
- `customer` → `customer_id`
- `receipt_email` → `email`
- `amount / 100` → `total` (converted from cents)
- `status` → `status`
- `currency` → `currency`
- `created` → `created_at`

### WooCommerce Orders
- `id` → `id`
- `customer_id` → `customer_id`
- `billing.email` → `email`
- `total` → `total`
- `status` → `status`
- `currency` → `currency`
- `date_created` → `created_at`
- `date_modified` → `updated_at`

## How It Works

1. **Resource Type Detection**: Identifies whether data is orders, products, or customers
2. **Field Mapping**: Maps platform-specific fields to standardized schema
3. **Amount Conversion**: Converts amounts (Stripe uses cents, others use decimal)
4. **Status Normalization**: Standardizes status values across platforms
5. **Currency Handling**: Optionally converts all amounts to single currency
6. **Platform Tagging**: Adds source platform for tracking

## Revenue Metrics

With standardized data, you can easily calculate:

### Total Revenue
```sql
SELECT SUM(total) FROM standardized_orders
WHERE status IN ('completed', 'succeeded')
```

### Average Order Value
```sql
SELECT AVG(total) FROM standardized_orders
WHERE status = 'completed'
```

### Revenue by Platform
```sql
SELECT platform, SUM(total) as revenue
FROM standardized_orders
WHERE status = 'completed'
GROUP BY platform
```

### Monthly Revenue Trend
```sql
SELECT DATE_TRUNC('month', created_at), SUM(total)
FROM standardized_orders
WHERE status = 'completed'
GROUP BY 1
ORDER BY 1
```

## Best Practices

### Currency Normalization
- Convert all amounts to single currency (e.g., USD)
- Use exchange rates for the transaction date
- Document currency conversion methodology

### Status Standardization
- Map platform statuses to standard values:
  - `completed` / `succeeded` → completed
  - `pending` / `processing` → pending
  - `cancelled` / `refunded` → cancelled

### Data Quality
- Validate order totals are positive
- Check for duplicate order IDs
- Handle refunds/cancellations appropriately
- Ensure timestamps are in consistent timezone

## Common Use Cases

### Revenue Dashboard
```
Shopify → Standardizer →
Stripe → Standardizer → All Orders → Revenue Dashboard
WooCommerce → Standardizer →
```

### Customer Lifetime Value
```
Standardized Orders → Group by Customer → Calculate LTV → Customer Segmentation
```

### Product Performance
```
Standardized Products + Orders → Sales Analysis → Best Sellers Report
```

## Key Metrics to Track

### Order Metrics
- Total orders by platform
- Average order value
- Order status distribution
- Refund/return rate

### Customer Metrics
- Unique customers by platform
- Repeat customer rate
- Customer acquisition by channel

### Product Metrics
- Top-selling products
- Revenue per product
- Inventory turnover

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Currency Conversion**: Requires exchange rate data for multi-currency conversion
- **Order States**: Different platforms have different order lifecycle states
- **Refunds**: Handle refunds as separate transactions or negative orders
- **Performance**: Handles millions of transactions efficiently
- **Incremental**: Works well with daily/hourly incremental loads
- **Tax Handling**: Can include tax breakdowns if available in source data
