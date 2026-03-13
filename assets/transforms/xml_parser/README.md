# XmlParser

Parse XML content stored in a DataFrame column into separate fields using XPath expressions. Each XPath expression maps to a new output column. Supports XML namespaces and handles malformed XML gracefully by returning null for failed rows.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `xml_column` | `str` | required | Column containing XML strings |
| `xpath_expressions` | `dict` | required | Mapping of `{output_column_name: xpath_expression}` |
| `namespace` | `Optional[dict]` | `null` | XML namespace mapping, e.g. `{ns: "http://..."}` |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Behavior

- Each key in `xpath_expressions` becomes a new column in the output DataFrame.
- Each value is an XPath expression evaluated against the parsed XML of that row.
- If the XPath returns multiple results, only the first match's text is used.
- If the XML is malformed or the XPath yields no results, the cell value is `null`.
- The original `xml_column` is preserved in the output.

## YAML Example

```yaml
type: dagster_component_templates.XmlParser
attributes:
  asset_name: parsed_product_xml
  upstream_asset_key: raw_product_feed
  xml_column: xml_data
  xpath_expressions:
    product_name: ".//name/text()"
    price: ".//price/text()"
    sku: ".//sku/text()"
  namespace: null
  group_name: transforms
```

### With namespace

```yaml
xpath_expressions:
  title: ".//ns:title/text()"
namespace:
  ns: "http://www.example.com/schema"
```

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.
