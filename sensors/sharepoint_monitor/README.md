# SharePoint Monitor

Monitors a SharePoint document library for new or modified files using the Microsoft Graph API.

Requires an Azure AD app registration with `Sites.Read.All` permission.

## Run config passed to job

```python
{
  "site_url": "https://myorg.sharepoint.com/sites/DataTeam",
  "library_name": "Reports",
  "file_name": "Q4_report.xlsx",
  "file_id": "abc123",
  "web_url": "https://myorg.sharepoint.com/...",
  "last_modified": "2026-03-12T10:00:00Z",
  "size": 204800
}
```
