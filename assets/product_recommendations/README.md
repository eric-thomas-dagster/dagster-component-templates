# Product Recommendations Component

Generate product recommendations using collaborative filtering, popularity rankings, and bundle analysis. Power product pages, shopping carts, and email campaigns with data-driven recommendations.

## Overview

Product recommendations drive cross-sell and upsell opportunities by suggesting relevant items to customers. This component supports three proven approaches:

- **Collaborative Filtering**: "Customers who bought X also bought Y"
- **Popular Products**: Bestsellers and trending items
- **Frequently Bought Together**: Product bundles with high co-purchase rates

## Use Cases

- **Product Page Recommendations**: Show related items on product detail pages
- **Shopping Cart Upsells**: Suggest complementary products at checkout
- **Email Campaigns**: Personalized product suggestions
- **Homepage Personalization**: Feature trending and popular items
- **Post-Purchase Follow-up**: Recommend complementary purchases
- **Inventory Optimization**: Identify which products to stock together

## Input Requirements

Order/transaction item data:

| Column | Type | Required | Alternatives | Description |
|--------|------|----------|--------------|-------------|
| `product_id` | string | ✓ | item_id, sku, productId | Unique product identifier |
| `order_id` | string | ✓ | transaction_id, orderId | Order/transaction identifier |
| `customer_id` | string | | user_id, customerId | Customer identifier (optional) |
| `date` | datetime | | order_date, transaction_date, created_at | Transaction timestamp (optional) |
| `quantity` | number | | qty, amount, count | Quantity purchased (optional, defaults to 1) |
| `product_name` | string | | name, title, item_name | Product name for readable output (optional) |
| `category` | string | | product_category, item_category | Product category (optional) |

**Compatible Upstream Components:**
- `ecommerce_standardizer`
- `shopify_ingestion`
- `stripe_ingestion`
- Any component outputting order line items

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | string | Source product |
| `recommended_product_id` | string | Recommended product |
| `recommendation_rank` | number | Rank (1 = top recommendation) |
| `co_occurrence_count` | number | Times purchased together (collaborative) |
| `support` | number | % of orders with product A that also have B (collaborative) |
| `bundle_frequency` | number | Bundle purchase count (frequently_bought_together) |
| `lift` | number | Co-purchase lift vs. random (frequently_bought_together) |
| `num_orders` | number | Total orders containing product (popular) |
| `source_product_name` | string | Source product name (if available) |
| `recommended_product_name` | string | Recommended product name (if available) |
| `recommended_product_category` | string | Category (if available) |

## Configuration

### Required Parameters

- **`asset_name`** (string): Name for output asset (e.g., `product_recommendations`)

### Optional Parameters

- **`source_asset`** (string): Upstream order items data (auto-set via lineage)
- **`recommendation_type`** (string): Type of recommendations (default: `collaborative`)
  - `collaborative` - Customers who bought X also bought Y
  - `popular` - Bestselling products
  - `frequently_bought_together` - Product bundles
- **`num_recommendations`** (number): Recommendations per product (default: 10)
- **`min_co_occurrence`** (number): Minimum co-purchases required (default: 3)
- **`lookback_days`** (number): Only use recent data (optional)
- **`customer_id_field`** (string): Custom column name (auto-detected)
- **`product_id_field`** (string): Custom column name (auto-detected)
- **`order_id_field`** (string): Custom column name (auto-detected)
- **`date_field`** (string): Custom column name (auto-detected)
- **`quantity_field`** (string): Custom column name (auto-detected)
- **`product_name_field`** (string): Custom column name (auto-detected)
- **`category_field`** (string): Custom column name (auto-detected)
- **`description`** (string): Asset description
- **`group_name`** (string): Asset group (default: `product_analytics`)

## Example Configuration

### Collaborative Filtering (Most Common)

```yaml
type: dagster_component_templates.ProductRecommendationsComponent
attributes:
  asset_name: product_recommendations
  recommendation_type: collaborative
  num_recommendations: 10
  min_co_occurrence: 5
  description: Collaborative filtering recommendations
```

### Popular Products

```yaml
type: dagster_component_templates.ProductRecommendationsComponent
attributes:
  asset_name: popular_product_recommendations
  recommendation_type: popular
  num_recommendations: 20
  description: Trending and bestselling products
```

### Frequently Bought Together (Bundles)

```yaml
type: dagster_component_templates.ProductRecommendationsComponent
attributes:
  asset_name: product_bundles
  recommendation_type: frequently_bought_together
  num_recommendations: 5
  min_co_occurrence: 10
  description: Product bundle recommendations
```

## Recommendation Types Explained

### Collaborative Filtering
**"Customers who bought X also bought Y"**

Finds products frequently purchased together based on co-occurrence in orders.

**Best for:**
- General product recommendations
- Cross-sell opportunities
- Discovering related products

**Metrics:**
- `co_occurrence_count`: Number of orders containing both products
- `support`: % of product A orders that also contain product B

### Popular Products
**Bestsellers and Trending Items**

Recommends the most popular products across the entire catalog.

**Best for:**
- Homepage features
- New customer recommendations
- Cold start (products with no purchase history)

**Metrics:**
- `num_orders`: Total orders containing the product
- `total_quantity`: Total units sold
- `popularity_rank`: Rank by order count

### Frequently Bought Together
**Product Bundles**

Identifies strong product combinations with statistical lift analysis.

**Best for:**
- Shopping cart bundles ("Buy these together")
- Promotional packages
- Inventory planning

**Metrics:**
- `bundle_frequency`: Times purchased together
- `lift`: How much more likely than random co-purchase
- Lift > 1.5 indicates strong relationship

## Best Practices

### Choosing Recommendation Type

| Scenario | Recommended Type | Why |
|----------|------------------|-----|
| Product detail page | Collaborative | Shows related items customers actually buy |
| Shopping cart | Frequently Bought Together | Strong bundles, higher lift |
| Homepage | Popular | No personalization context needed |
| New products | Popular | No co-purchase history yet |
| Email campaigns | Collaborative | Personalized to past purchases |

### Parameter Tuning

**min_co_occurrence:**
- **Low volume (< 1,000 orders/month)**: 2-3
- **Medium volume (1,000-10,000)**: 3-5
- **High volume (> 10,000)**: 5-10

**num_recommendations:**
- **Product pages**: 4-8 items
- **Email campaigns**: 3-5 items
- **Homepage carousels**: 10-20 items

**lookback_days:**
- **Fast-moving inventory**: 30-90 days
- **Seasonal products**: 90-180 days
- **Stable catalog**: Use all data (no limit)

### Data Quality

1. **Clean Product IDs**: Ensure consistent product identifiers
2. **Remove Returns**: Filter out returned/refunded items
3. **Filter Test Orders**: Remove internal test transactions
4. **Deduplicate**: Handle duplicate line items in same order

## Integration Examples

### Product Page API

```python
# Load recommendations
recs_df = context.load_asset_value("product_recommendations")

def get_recommendations(product_id, limit=5):
    """Get top N recommendations for a product."""
    product_recs = recs_df[recs_df['product_id'] == product_id].head(limit)

    return product_recs[[
        'recommended_product_id',
        'recommended_product_name',
        'recommendation_rank',
        'co_occurrence_count'
    ]].to_dict('records')

# Example
recs = get_recommendations('PROD_123', limit=5)
print(f"Recommendations for PROD_123: {recs}")
```

### Email Campaign Generator

```python
# Get customer's recent purchases
recent_purchases = orders_df[
    (orders_df['customer_id'] == 'CUST_456') &
    (orders_df['date'] >= pd.Timestamp.now() - pd.Timedelta(days=30))
]['product_id'].unique()

# Get recommendations for each purchased product
all_recommendations = []
for product in recent_purchases:
    recs = get_recommendations(product, limit=3)
    all_recommendations.extend(recs)

# Deduplicate and rank by co-occurrence
recommendations_df = pd.DataFrame(all_recommendations)
top_recs = recommendations_df.groupby('recommended_product_id').agg({
    'co_occurrence_count': 'max',
    'recommended_product_name': 'first'
}).sort_values('co_occurrence_count', ascending=False).head(5)

print("Email recommendations:")
print(top_recs)
```

### Bundle Pricing Strategy

```python
# Load bundle recommendations
bundles_df = context.load_asset_value("product_bundles")

# Find strongest bundles (high lift)
strong_bundles = bundles_df[bundles_df['lift'] >= 2.0].copy()

# Calculate bundle discount strategy
strong_bundles['suggested_discount_pct'] = (
    (strong_bundles['lift'] - 1) * 5  # 5% discount per lift point
).clip(upper=20)  # Max 20% discount

print("Bundle Discount Recommendations:")
print(strong_bundles[['product_id', 'recommended_product_id', 'lift', 'suggested_discount_pct']].head(10))
```

## Key Metrics to Track

### Recommendation Quality
- Click-through rate on recommendations
- Add-to-cart rate from recommendations
- Conversion rate from recommended products
- Average order value with vs. without recommendations

### Coverage
- % of products with recommendations
- Average number of recommendations per product
- % of orders with recommended products

### Business Impact
- Revenue from recommended products
- Incremental revenue (vs. baseline)
- Average order value lift
- Cross-sell rate

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.24.0`

## Notes

- **Update Frequency**: Run daily or weekly to reflect new purchase patterns
- **Cold Start**: New products won't have recommendations until purchase history builds
- **Seasonal Adjustment**: Consider using lookback_days for seasonal catalogs
- **Performance**: Handles millions of transactions efficiently
- **A/B Testing**: Compare recommendation types to optimize for your business
