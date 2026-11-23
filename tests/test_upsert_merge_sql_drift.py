"""The "upsert-merge-sql" block (build_merge_sql) is inlined byte-identical into every
component whose destination platform supports a real standard-SQL MERGE INTO statement for
if_exists='upsert': dataframe_to_snowflake, dataframe_to_redshift, and (as one dialect
branch inside build_dialect_upsert_sql) sql_to_database_asset. dataframe_to_databricks is
NOT in this set -- it needs 3-part catalog.schema.table identifiers and backtick quoting,
genuinely different from this block's single schema+table pair, so it has its own
`_databricks_merge_sql` instead. Kept in sync via a real AST comparison (not a text-diff of
surrounding BEGIN/END markers) so a fix to the MERGE shape in one component lands
everywhere else that shares it."""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent

COMPONENTS = [
    "assets/sinks/dataframe_to_snowflake",
    "assets/sinks/dataframe_to_redshift",
    "assets/ingestion/sql_to_database_asset",
]
SHARED_FUNCS = ("build_merge_sql",)


def _module_funcs(rel: str) -> dict:
    path = ROOT / rel / "component.py"
    src = path.read_text()
    tree = ast.parse(src)
    out = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in SHARED_FUNCS:
            out[node.name] = ast.get_source_segment(src, node)
    return out


def test_every_component_defines_the_full_shared_set():
    for rel in COMPONENTS:
        funcs = _module_funcs(rel)
        missing = set(SHARED_FUNCS) - set(funcs)
        assert not missing, f"{rel} is missing upsert-merge-sql function(s): {missing}"


def test_components_agree_with_each_other():
    ref_rel = COMPONENTS[0]
    ref_funcs = _module_funcs(ref_rel)
    for rel in COMPONENTS[1:]:
        funcs = _module_funcs(rel)
        for name in SHARED_FUNCS:
            assert funcs[name] == ref_funcs[name], f"{rel}.{name} drifted from {ref_rel}'s copy"
