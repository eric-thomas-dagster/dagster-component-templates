# PerFileProcessorJobComponent

List files in S3/GCS/ADLS matching a pattern, run a callable per file in parallel via DynamicOut. Optional archive-after-success.

## Dependencies
- `boto3`
- `google-cloud-storage`
- `azure-storage-blob`
- `azure-identity`

## See also
- [Schema](schema.json) · [Example](example.yaml)
