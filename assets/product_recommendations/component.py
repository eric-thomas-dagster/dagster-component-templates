"""Product Recommendations Component.

Generate product recommendations using collaborative filtering (customers who bought X also bought Y)
and popularity-based algorithms.
"""

from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
    Output,
    MetadataValue,
)
from pydantic import Field


class ProductRecommendationsComponent(Component, Model, Resolvable):
    """Component for generating product recommendations.

    This component creates product recommendations using proven approaches:
    - **Collaborative Filtering**: "Customers who bought X also bought Y"
    - **Popular Products**: Trending and bestselling items
    - **Frequently Bought Together**: Product bundles and cross-sells
    - **Category-Based**: Similar products in same category

    The output can be used for:
    - Product page recommendations
    - Shopping cart upsells
    - Email campaigns
    - Homepage personalization

    Example:
        ```yaml
        type: dagster_component_templates.ProductRecommendationsComponent
        attributes:
          asset_name: product_recommendations
          source_asset: order_items
          recommendation_type: collaborative
          num_recommendations: 10
          min_co_occurrence: 3
          description: "Product recommendations"
          group_name: product_analytics
        ```
    """

    asset_name: str = Field(
        description="Name of the asset to create"
    )

    source_asset: Optional[str] = Field(
        default=None,
        description="Source asset with order/transaction data (set via lineage)"
    )

    recommendation_type: str = Field(
        default="collaborative",
        description="Type: collaborative, popular, frequently_bought_together"
    )

    num_recommendations: int = Field(
        default=10,
        description="Number of recommendations to generate per product"
    )

    min_co_occurrence: int = Field(
        default=3,
        description="Minimum times products must be purchased together (collaborative filtering)"
    )

    lookback_days: Optional[int] = Field(
        default=None,
        description="Only consider recent purchases (optional, use all data if not set)"
    )

    customer_id_field: Optional[str] = Field(
        default=None,
        description="Customer ID column (auto-detected if not specified)"
    )

    product_id_field: Optional[str] = Field(
        default=None,
        description="Product ID column (auto-detected if not specified)"
    )

    order_id_field: Optional[str] = Field(
        default=None,
        description="Order ID column (auto-detected if not specified)"
    )

    date_field: Optional[str] = Field(
        default=None,
        description="Transaction date column (auto-detected if not specified)"
    )

    quantity_field: Optional[str] = Field(
        default=None,
        description="Quantity purchased column (auto-detected if not specified)"
    )

    product_name_field: Optional[str] = Field(
        default=None,
        description="Product name column (optional, for readable output)"
    )

    category_field: Optional[str] = Field(
        default=None,
        description="Product category column (optional, for category-based recommendations)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default="product_analytics",
        description="Asset group for organization"
    )

    include_sample_metadata: bool = Field(
        default=True,
        description="Include sample data preview in metadata"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        source_asset = self.source_asset
        rec_type = self.recommendation_type
        num_recs = self.num_recommendations
        min_co_occur = self.min_co_occurrence
        lookback_days = self.lookback_days
        customer_id_field = self.customer_id_field
        product_id_field = self.product_id_field
        order_id_field = self.order_id_field
        date_field = self.date_field
        quantity_field = self.quantity_field
        product_name_field = self.product_name_field
        category_field = self.category_field
        description = self.description or f"Product recommendations ({rec_type})"
        group_name = self.group_name
        include_sample = self.include_sample_metadata

        # Set up dependencies
        upstream_keys = []
        if source_asset:
            upstream_keys.append(source_asset)

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
            deps=upstream_keys if upstream_keys else None,
        )
        def product_recommendations_asset(context: AssetExecutionContext, **kwargs) -> pd.DataFrame:
            """Asset that generates product recommendations."""

            # Load upstream data
            upstream_data = {}
            if upstream_keys and hasattr(context, 'load_asset_value'):
                for key in upstream_keys:
                    try:
                        value = context.load_asset_value(AssetKey(key))
                        upstream_data[key] = value
                        context.log.info(f"Loaded {len(value)} rows from {key}")
                    except Exception as e:
                        context.log.warning(f"Could not load {key}: {e}")
            else:
                upstream_data = kwargs

            if not upstream_data:
                context.log.warning("No upstream data available")
                return pd.DataFrame()

            # Get the source DataFrame
            df = list(upstream_data.values())[0]
            if not isinstance(df, pd.DataFrame):
                context.log.error("Source data is not a DataFrame")
                return pd.DataFrame()

            context.log.info(f"Processing {len(df)} transaction records for recommendations")

            # Auto-detect required columns
            def find_column(possible_names, custom_name=None):
                if custom_name and custom_name in df.columns:
                    return custom_name
                for name in possible_names:
                    if name in df.columns:
                        return name
                return None

            product_col = find_column(
                ['product_id', 'item_id', 'sku', 'productId', 'product'],
                product_id_field
            )
            order_col = find_column(
                ['order_id', 'transaction_id', 'orderId', 'transactionId'],
                order_id_field
            )

            # Validate required columns
            missing = []
            if not product_col:
                missing.append("product_id")
            if not order_col:
                missing.append("order_id")

            if missing:
                context.log.error(f"Missing required columns: {', '.join(missing)}")
                context.log.info(f"Available columns: {', '.join(df.columns)}")
                return pd.DataFrame()

            # Optional columns
            customer_col = find_column(
                ['customer_id', 'user_id', 'customerId', 'userId'],
                customer_id_field
            )
            date_col = find_column(
                ['date', 'order_date', 'transaction_date', 'created_at'],
                date_field
            )
            quantity_col = find_column(
                ['quantity', 'qty', 'amount', 'count'],
                quantity_field
            )
            name_col = find_column(
                ['product_name', 'name', 'title', 'item_name'],
                product_name_field
            )
            category_col = find_column(
                ['category', 'product_category', 'item_category'],
                category_field
            )

            context.log.info(f"Using columns - Product: {product_col}, Order: {order_col}")

            # Prepare data
            cols_to_use = [product_col, order_col]
            col_names = ['product_id', 'order_id']

            if customer_col:
                cols_to_use.append(customer_col)
                col_names.append('customer_id')
            if date_col:
                cols_to_use.append(date_col)
                col_names.append('date')
            if quantity_col:
                cols_to_use.append(quantity_col)
                col_names.append('quantity')
            if name_col:
                cols_to_use.append(name_col)
                col_names.append('product_name')
            if category_col:
                cols_to_use.append(category_col)
                col_names.append('category')

            orders_df = df[cols_to_use].copy()
            orders_df.columns = col_names

            # Apply lookback window if specified
            if lookback_days and 'date' in orders_df.columns:
                orders_df['date'] = pd.to_datetime(orders_df['date'], errors='coerce')
                cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
                orders_df = orders_df[orders_df['date'] >= cutoff_date]
                context.log.info(f"Applied {lookback_days}-day lookback window: {len(orders_df)} records")

            if 'quantity' not in orders_df.columns:
                orders_df['quantity'] = 1

            # Remove duplicates
            orders_df = orders_df.drop_duplicates()

            context.log.info(f"Generating {rec_type} recommendations for {orders_df['product_id'].nunique()} products")

            recommendations = []

            if rec_type == 'collaborative':
                # Collaborative filtering: "Customers who bought X also bought Y"

                # Get products purchased together in same order
                product_pairs = []

                for order_id in orders_df['order_id'].unique():
                    products = orders_df[orders_df['order_id'] == order_id]['product_id'].unique()

                    # Generate all pairs of products in this order
                    if len(products) > 1:
                        for i, prod1 in enumerate(products):
                            for prod2 in products[i+1:]:
                                product_pairs.append((prod1, prod2))
                                product_pairs.append((prod2, prod1))  # Both directions

                if not product_pairs:
                    context.log.warning("No product pairs found for collaborative filtering")
                    return pd.DataFrame()

                # Count co-occurrences
                pairs_df = pd.DataFrame(product_pairs, columns=['product_id', 'recommended_product_id'])
                pair_counts = pairs_df.groupby(['product_id', 'recommended_product_id']).size().reset_index(name='co_occurrence_count')

                # Filter by minimum co-occurrence
                pair_counts = pair_counts[pair_counts['co_occurrence_count'] >= min_co_occur]

                # Calculate support (percentage of orders containing product A that also contain product B)
                product_order_counts = orders_df.groupby('product_id')['order_id'].nunique().reset_index(name='product_order_count')
                pair_counts = pair_counts.merge(product_order_counts, on='product_id', how='left')
                pair_counts['support'] = (pair_counts['co_occurrence_count'] / pair_counts['product_order_count'] * 100).round(2)

                # Rank recommendations by co-occurrence count
                pair_counts['recommendation_rank'] = pair_counts.groupby('product_id')['co_occurrence_count'].rank(ascending=False, method='first').astype(int)

                # Keep top N recommendations per product
                recommendations = pair_counts[pair_counts['recommendation_rank'] <= num_recs].copy()

                context.log.info(f"Generated {len(recommendations)} collaborative recommendations")

            elif rec_type == 'popular':
                # Popular products: Most frequently purchased items

                # Calculate product popularity
                product_stats = orders_df.groupby('product_id').agg({
                    'order_id': 'nunique',
                    'quantity': 'sum'
                }).reset_index()
                product_stats.columns = ['product_id', 'num_orders', 'total_quantity']

                # Rank by number of orders
                product_stats['popularity_rank'] = product_stats['num_orders'].rank(ascending=False, method='first').astype(int)
                product_stats = product_stats.sort_values('popularity_rank')

                # For each product, recommend top popular products (excluding itself)
                all_products = product_stats['product_id'].unique()
                top_products = product_stats[product_stats['popularity_rank'] <= num_recs]['product_id'].tolist()

                for product_id in all_products:
                    # Recommend top products except the product itself
                    recs_for_product = [p for p in top_products if p != product_id][:num_recs]

                    for rank, rec_product in enumerate(recs_for_product, 1):
                        rec_data = product_stats[product_stats['product_id'] == rec_product].iloc[0]
                        recommendations.append({
                            'product_id': product_id,
                            'recommended_product_id': rec_product,
                            'recommendation_rank': rank,
                            'num_orders': rec_data['num_orders'],
                            'total_quantity': rec_data['total_quantity'],
                            'popularity_rank': rec_data['popularity_rank']
                        })

                recommendations = pd.DataFrame(recommendations)
                context.log.info(f"Generated {len(recommendations)} popularity-based recommendations")

            elif rec_type == 'frequently_bought_together':
                # Same as collaborative but with stricter filtering for bundles

                # Get products purchased together in same order
                product_pairs = []

                for order_id in orders_df['order_id'].unique():
                    products = orders_df[orders_df['order_id'] == order_id]['product_id'].unique()

                    if len(products) >= 2:  # Only consider orders with multiple items
                        for i, prod1 in enumerate(products):
                            for prod2 in products[i+1:]:
                                product_pairs.append((prod1, prod2))
                                product_pairs.append((prod2, prod1))

                if not product_pairs:
                    context.log.warning("No product pairs found")
                    return pd.DataFrame()

                pairs_df = pd.DataFrame(product_pairs, columns=['product_id', 'recommended_product_id'])
                pair_counts = pairs_df.groupby(['product_id', 'recommended_product_id']).size().reset_index(name='bundle_frequency')

                # Higher threshold for bundles
                bundle_threshold = max(min_co_occur * 2, 5)
                pair_counts = pair_counts[pair_counts['bundle_frequency'] >= bundle_threshold]

                # Calculate lift (how much more likely to buy together than random)
                total_orders = orders_df['order_id'].nunique()
                product_order_counts = orders_df.groupby('product_id')['order_id'].nunique()

                pair_counts = pair_counts.merge(
                    product_order_counts.rename('product_a_orders'),
                    left_on='product_id',
                    right_index=True
                )
                pair_counts = pair_counts.merge(
                    product_order_counts.rename('product_b_orders'),
                    left_on='recommended_product_id',
                    right_index=True
                )

                pair_counts['expected_co_occurrence'] = (
                    pair_counts['product_a_orders'] * pair_counts['product_b_orders'] / total_orders
                )
                pair_counts['lift'] = (pair_counts['bundle_frequency'] / pair_counts['expected_co_occurrence']).round(2)

                # Rank by lift (preference for strong associations)
                pair_counts['recommendation_rank'] = pair_counts.groupby('product_id')['lift'].rank(ascending=False, method='first').astype(int)

                recommendations = pair_counts[pair_counts['recommendation_rank'] <= num_recs].copy()

                context.log.info(f"Generated {len(recommendations)} bundle recommendations")

            else:
                context.log.error(f"Unknown recommendation type: {rec_type}")
                return pd.DataFrame()

            # Add product names if available
            if 'product_name' in orders_df.columns:
                product_names = orders_df[['product_id', 'product_name']].drop_duplicates()

                recommendations = recommendations.merge(
                    product_names.rename(columns={'product_name': 'source_product_name'}),
                    on='product_id',
                    how='left'
                )
                recommendations = recommendations.merge(
                    product_names.rename(columns={'product_id': 'recommended_product_id', 'product_name': 'recommended_product_name'}),
                    on='recommended_product_id',
                    how='left'
                )

            # Add category if available
            if 'category' in orders_df.columns:
                product_categories = orders_df[['product_id', 'category']].drop_duplicates()
                recommendations = recommendations.merge(
                    product_categories.rename(columns={'category': 'recommended_product_category'}),
                    left_on='recommended_product_id',
                    right_on='product_id',
                    how='left',
                    suffixes=('', '_drop')
                )
                # Clean up duplicate product_id column from merge
                recommendations = recommendations.drop(columns=[col for col in recommendations.columns if col.endswith('_drop')])

            # Sort by product and rank
            recommendations = recommendations.sort_values(['product_id', 'recommendation_rank'])

            context.log.info(f"Recommendation generation complete: {len(recommendations)} recommendations for {recommendations['product_id'].nunique()} products")

            # Log sample recommendations
            if len(recommendations) > 0:
                sample_product = recommendations['product_id'].iloc[0]
                sample_recs = recommendations[recommendations['product_id'] == sample_product].head(5)
                context.log.info(f"\nSample recommendations for product {sample_product}:")
                for _, row in sample_recs.iterrows():
                    rec_prod = row['recommended_product_id']
                    if 'recommended_product_name' in row:
                        rec_prod = f"{rec_prod} ({row['recommended_product_name']})"
                    context.log.info(f"  #{row['recommendation_rank']}: {rec_prod}")

            # Add metadata
            metadata = {
                "row_count": len(recommendations),
                "num_products_with_recommendations": recommendations['product_id'].nunique(),
                "recommendation_type": rec_type,
                "num_recommendations_per_product": num_recs,
                "min_co_occurrence": min_co_occur,
            }

            if lookback_days:
                metadata['lookback_days'] = lookback_days

            # Return with metadata
            if include_sample and len(recommendations) > 0:
                return Output(
                    value=recommendations,
                    metadata={
                        **metadata,
                        "sample": MetadataValue.md(recommendations.head(20).to_markdown(index=False)),
                        "preview": MetadataValue.dataframe(recommendations.head(20))
                    }
                )
            else:
                context.add_output_metadata(metadata)
                return recommendations

        return Definitions(assets=[product_recommendations_asset])
