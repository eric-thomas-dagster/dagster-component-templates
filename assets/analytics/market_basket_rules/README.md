# MarketBasketRules

Group rows into baskets (e.g. by transaction_id), one-hot encode item presence per basket, run apriori to find frequent itemsets, then derive association rules with support, confidence, lift. Output is one row per rule.

## Example

```yaml
type: dagster_component_templates.MarketBasketRulesComponent
attributes:
  asset_name: cart_rules
  upstream_asset_key: order_line_items
  basket_column: order_id
  item_column: product_name
  min_support: 0.02
  min_confidence: 0.4
  metric: lift
  group_name: market_basket
```


## Requirements

```
pandas
mlxtend
```
