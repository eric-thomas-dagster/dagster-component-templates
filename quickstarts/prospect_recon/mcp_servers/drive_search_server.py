"""Stdio MCP server exposing Drive + Notion search/read tools to the research
step's `tool_use_loop` (prospect_recon_research), so the LLM can pick its own
query variants (company name, domain-minus-TLD, attendee surnames, opportunity
name, "discovery"/"MEDDIC" + company name) the way the real routine's
instructions describe, rather than a hardcoded query list.

Auth, same convention as google_drive_ingestion / notion_resource:
  GOOGLE_APPLICATION_CREDENTIALS -- service-account JSON path (Drive)
  NOTION_TOKEN                   -- Notion integration token

Run standalone for a smoke test: `python3 drive_search_server.py` (stdio transport).
"""
import os

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("prospect-recon-drive-notion-search")


def _drive_service():
    import json

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set")
    with open(cred_path) as fh:
        creds_dict = json.load(fh)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _notion_client():
    import notion_client

    token = os.environ.get("NOTION_TOKEN")
    if not token:
        raise RuntimeError("NOTION_TOKEN not set")
    return notion_client.Client(auth=token)


@mcp.tool()
def search_drive(query: str, max_results: int = 10) -> list[dict]:
    """Search Google Drive by text (Drive's native `q` syntax, e.g.
    "fullText contains 'AcmeCorp'" or "name contains 'discovery'"). Returns
    {id, name, mimeType, modifiedTime, webViewLink} per hit -- call
    read_drive_file(file_id) to get a promising hit's actual content.
    Try several query variants: the company name, the domain minus its TLD,
    attendee surnames, an opportunity name, and terms like "discovery",
    "disco notes", "MEDDIC", "technical qualification" combined with the
    company name -- a single search is rarely enough.
    """
    svc = _drive_service()
    resp = svc.files().list(
        q=f"({query}) and trashed=false",
        fields="files(id, name, mimeType, modifiedTime, webViewLink)",
        pageSize=max_results,
    ).execute()
    return resp.get("files", [])


@mcp.tool()
def read_drive_file(file_id: str, max_bytes: int = 200_000) -> str:
    """Read a Drive file's text content by id. Works for plain text / CSV /
    JSON directly; PDFs and Office files are also readable (only Drive's
    *search* is blind to non-Google-native formats, not reads). Native Google
    Docs/Sheets/Slides are exported to their plain-text equivalent."""
    import io

    from googleapiclient.http import MediaIoBaseDownload

    svc = _drive_service()
    meta = svc.files().get(fileId=file_id, fields="mimeType, name").execute()
    mime = meta.get("mimeType", "")
    export_map = {
        "application/vnd.google-apps.document": "text/plain",
        "application/vnd.google-apps.spreadsheet": "text/csv",
        "application/vnd.google-apps.presentation": "text/plain",
    }
    if mime in export_map:
        data = svc.files().export(fileId=file_id, mimeType=export_map[mime]).execute()
        return data.decode("utf-8", errors="replace")[:max_bytes]

    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, svc.files().get_media(fileId=file_id))
    done = False
    while not done:
        _, done = downloader.next_chunk()
    raw = buf.getvalue()[:max_bytes]
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return f"[binary file {meta.get('name')!r}, {mime}, {len(raw)} bytes read -- not text-decodable]"


@mcp.tool()
def search_notion(query: str, max_results: int = 10) -> list[dict]:
    """Search Notion pages/databases by free text (not used by the real routine
    today -- AEs currently only drop prep material in Drive -- but kept available
    in case an AE starts using Notion for discovery notes the same way). Returns
    {id, title, url} per hit -- call read_notion_page(page_id) for its content."""
    client = _notion_client()
    results = client.search(query=query, page_size=max_results).get("results", [])
    out = []
    for r in results:
        title = ""
        props = r.get("properties") or {}
        for prop in props.values():
            if prop.get("type") == "title":
                title = "".join(t.get("plain_text", "") for t in prop.get("title", []))
                break
        out.append({"id": r.get("id"), "title": title, "url": r.get("url")})
    return out


@mcp.tool()
def read_notion_page(page_id: str) -> str:
    """Read a Notion page's content rendered as markdown."""
    client = _notion_client()
    return client.pages.retrieve_markdown(page_id=page_id)


if __name__ == "__main__":
    mcp.run(transport="stdio")
