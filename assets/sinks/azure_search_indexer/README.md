# DataFrame → Azure AI Search

Push DataFrame rows as documents to an Azure AI Search index. Backbone of RAG, semantic search, enterprise knowledge bases. Each row = one document; the DataFrame must include the index's key field.

## Companion: azure_search_query

Use `azure_search_query` to run search queries (text / vector / semantic) and get results as a DataFrame.

## Actions

- `upload`: insert/replace entire doc
- `merge`: partial update (only specified fields)
- `mergeOrUpload`: insert if new, partial update if exists (default — safest)
- `delete`: remove docs by key
