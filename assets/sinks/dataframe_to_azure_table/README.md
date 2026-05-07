# DataFrame → Azure Tables

Write DataFrame rows as entities to Azure Table Storage. NoSQL key-value store, way cheaper than Cosmos for simple structured data ($0.045/GB/mo + $0.00036 per 10K transactions).

## Companion: azure_table_reader

Use `azure_table_reader` to read entities back as a DataFrame.

## When to use vs Cosmos DB

| Need | Tables | Cosmos DB |
|---|---|---|
| Simple key-value, low cost | ✓ | $$$ |
| Global distribution | ✗ | ✓ |
| SQL queries, indexes | ✗ | ✓ |
| Schemaless | ✓ | ✓ |
| Use existing storage account | ✓ | ✗ |
