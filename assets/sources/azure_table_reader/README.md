# Azure Tables Reader

Read entities from an Azure Table as a DataFrame. Supports OData filters for partition-scoped, RowKey-scoped, or property filtering.

## OData filter examples

```
# Partition-scoped (most efficient)
PartitionKey eq 'orders'

# Range query within a partition
PartitionKey eq 'orders' and RowKey gt '2026'

# Property filter (full table scan, slower)
category eq 'Electronics' and price gt 100.0
```
