"""The receipt-table block inlined in component.py must stay byte-identical to the canonical block (no shared module: components are
self-contained). GOLDEN is the canonical text itself; any edit to one copy must be made to every copy and to this golden."""
from .conftest import load_component_module

GOLDEN = r'''# BEGIN receipt-warehouse-ddl (canonical; inlined verbatim, byte-identical, by every component that creates or reads the receipt tables -- drift-tested)
RECEIPT_TABLE_NAMES = ("context_receipts", "context_receipt_items", "context_receipt_excluded", "context_receipt_events", "context_item_scores")


def receipt_table(dataset: str, name: str, prefix: str = "") -> str:
    """Fully qualified, quoted table reference. `dataset` is `project.dataset` (or just `dataset` to use the client's project)."""
    if name not in RECEIPT_TABLE_NAMES:
        raise ValueError(f"unknown receipt table {name!r}; valid: {RECEIPT_TABLE_NAMES}")
    return f"`{dataset}.{prefix}{name}`"


def receipt_table_ddl(dataset: str, prefix: str = "", dialect: str = "bigquery") -> list:
    """CREATE TABLE IF NOT EXISTS statements for the receipt tables (the same columns as the SQLite store, per resources/context_receipt_store/SPEC.md).
    Timestamps are TIMESTAMP (ISO-8601 UTC in, TIMESTAMP out); event payloads stay JSON text in `payload_json` (read with JSON_VALUE)."""
    if dialect != "bigquery":
        raise NotImplementedError(f"receipt warehouse tables are implemented for dialect='bigquery' only (got {dialect!r})")
    t = lambda n: receipt_table(dataset, n, prefix)  # noqa: E731
    return [
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipts')} (
  receipt_id STRING NOT NULL, created_at TIMESTAMP NOT NULL, context_name STRING NOT NULL, context_version STRING, policy_version STRING,
  caller_id STRING, purpose STRING, query_hash STRING, eligible_count INT64, excluded_count INT64, dagster_asset_key STRING,
  dagster_run_id STRING, trace_id STRING, mcp_session_id STRING, mcp_request_id STRING)""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_items')} (
  receipt_id STRING NOT NULL, item_id STRING NOT NULL, source_asset_key STRING, source_version STRING, source_partition STRING,
  source_code_version STRING, rank INT64, score FLOAT64, selection STRING)""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_excluded')} (
  receipt_id STRING NOT NULL, item_id STRING NOT NULL, reason STRING NOT NULL, source_asset_key STRING, source_version STRING,
  source_partition STRING, source_code_version STRING)""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_receipt_events')} (
  event_id STRING NOT NULL, receipt_id STRING NOT NULL, event_type STRING NOT NULL, occurred_at TIMESTAMP NOT NULL, payload_json STRING NOT NULL)""",
        f"""CREATE TABLE IF NOT EXISTS {t('context_item_scores')} (
  item_id STRING NOT NULL, retrievals INT64 NOT NULL, outcome_weight FLOAT64 NOT NULL, outcome_sum FLOAT64 NOT NULL, prior FLOAT64 NOT NULL,
  updated_at TIMESTAMP NOT NULL)""",
    ]
# END receipt-warehouse-ddl
'''

BEGIN, END = "# BEGIN receipt-warehouse-ddl", "# END receipt-warehouse-ddl"


def _block_of(text: str) -> str:
    start = text.index(BEGIN)
    end = text.index(END, start)
    return text[start:text.index("\n", end) + 1]


def test_inlined_block_is_byte_identical_to_canonical_golden():
    mod = load_component_module()
    source = open(mod.__file__, encoding="utf-8").read()
    assert source.count(BEGIN) == 1 and source.count(END) == 1
    assert _block_of(source) == GOLDEN


def test_block_is_what_the_component_uses():
    mod = load_component_module()
    assert mod.receipt_table("proj.ds", "context_receipts", "p_") == "`proj.ds.p_context_receipts`"
