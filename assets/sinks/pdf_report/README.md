# PdfReportComponent

Render an upstream DataFrame to a PDF report file. Drop-in for Alteryx's
**Render** / **Portfolio Composer** reporting tools.

## Use Cases

- Daily / weekly tabular reports emailed downstream
- Customer-facing PDF deliverables (one PDF per partition / customer)
- Audit-trail snapshots of asset state at a point in time
- Replacing Alteryx Reporting tool chains in migrated workflows

## Two rendering modes (`template:` field)

| Mode | Backend | Best for |
|---|---|---|
| `table` (default) | `reportlab` | Plain tabular dump; multi-page; no extra deps |
| `template_html` | `jinja2` + `weasyprint` | Branded / multi-section / chart-heavy reports |

## Example — table mode

```yaml
type: dagster_community_components.PdfReportComponent
attributes:
  asset_name: weekly_sales_report
  upstream_asset_key: weekly_sales_summary
  file_path: /reports/weekly_sales.pdf
  title: Weekly Sales Summary
  template: table
  rows_per_page: 40
  page_size: LETTER
  group_name: reports
```

## Example — Jinja2 HTML template

```yaml
type: dagster_community_components.PdfReportComponent
attributes:
  asset_name: monthly_kpi_pdf
  upstream_asset_key: monthly_kpis
  file_path: s3://reports/monthly_kpis.pdf
  title: October KPI Roundup
  template: template_html
  html_template: ./templates/monthly_kpis.html.j2
  page_size: A4
```

The template gets `title`, `df` (pandas DataFrame), `rows` (list of dicts),
and `columns` (list) in its render context.

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | Source DataFrame |
| Output | `str` | The written file path (also surfaced in materialization metadata) |

## Notes

- **Cloud storage**: `file_path` accepts any `fsspec`-readable URL —
  `s3://`, `gs://`, `abfs://`. For Dagster+ Cloud / Hybrid / Serverless
  deployments, use a cloud URL (local paths don't survive container restarts).
- `columns` (optional) filters the DataFrame columns before render.
- `rows_per_page` chunks long tables across pages with header repetition.
- `page_size` accepts `LETTER`, `A4`, `LEGAL`, `TABLOID`.
- Default install: only `reportlab` is required (table mode). For
  `template_html`, also install `jinja2` + `weasyprint` (which has
  system-library deps — see weasyprint docs).
