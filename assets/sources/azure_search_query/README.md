# Azure AI Search Query

Query an Azure AI Search index and materialize results as a DataFrame. Supports keyword search, OData filters, semantic ranker, and (with index config) vector search.

## Search modes

- **Keyword**: default — `search_text='laptop'`, BM25 scoring
- **Filter only**: `search_text='*'` + `filter_query`
- **Semantic ranker**: requires Standard tier and a configured semantic config — pass `semantic_configuration_name`
- **Vector**: index must have a vector field; query via the SDK's `vector_queries` (use the resource pattern for full SDK access)
