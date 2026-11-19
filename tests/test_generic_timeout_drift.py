"""The generic timeout wrapper (_run_with_timeout) is inlined (components are self-contained
-- no shared module) into every component that enforces timeout_seconds via a background
thread rather than a native library timeout. This keeps the copies byte-identical so a fix
(e.g. the real shutdown(wait=True)-blocks-forever bug this helper was written to avoid)
lands everywhere."""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
COMPONENTS = [
    "assets/infrastructure/s3_copy_object_asset",
    "assets/infrastructure/s3_delete_objects_asset",
    "assets/infrastructure/s3_upload_file_asset",
    "assets/infrastructure/trigger_job_asset",
    "assets/infrastructure/pipes_python_asset",
    "assets/infrastructure/conditional_asset",
    "assets/infrastructure/runtime_partition_asset",
    "assets/infrastructure/loop_gate_asset",
    "assets/data-warehouse/snowflake_sql_asset",
    "assets/data-warehouse/snowflake_copy_into_asset",
    "assets/dbt/enriched_dbt_cloud_workspace",
    "assets/data-warehouse/sql_asset",
    "assets/infrastructure/python_callable_asset",
]
BEGIN = "# BEGIN generic timeout wrapper"
END = "# END generic timeout wrapper"


def _block(rel):
    src = (ROOT / rel / "component.py").read_text()
    assert BEGIN in src and END in src, f"{rel}: generic timeout wrapper block missing"
    return src[src.index(BEGIN): src.index(END) + len(END)]


def test_generic_timeout_wrapper_copies_are_identical():
    blocks = {rel: _block(rel) for rel in COMPONENTS}
    ref_rel = COMPONENTS[0]
    for rel, blk in blocks.items():
        assert blk == blocks[ref_rel], f"{rel} drifted from {ref_rel}'s timeout wrapper block"
