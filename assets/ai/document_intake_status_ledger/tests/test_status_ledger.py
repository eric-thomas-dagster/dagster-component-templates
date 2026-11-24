"""DocumentIntakeStatusLedgerComponent: the resource's record/read/summary behavior, plus a real
dg.materialize() round trip through the summary asset (and its optional stuck-processing check)."""
import json

import dagster as dg
import pytest

from .conftest import load_component_module


@pytest.fixture
def mod():
    return load_component_module()


@pytest.fixture
def ledger(mod, tmp_path):
    return mod.DocumentIntakeStatusLedger(ledger_path=str(tmp_path / "ledger.jsonl"))


def test_record_and_get_round_trip(ledger):
    h = ledger.record_status("etag-1", "discovered", source="s3://bucket", identifier="key1")
    assert h == "etag-1"
    rec = ledger.get_status("etag-1")
    assert rec["status"] == "discovered" and rec["source"] == "s3://bucket" and rec["identifier"] == "key1"
    assert rec["detail"] is None and rec["ts"]


def test_get_status_of_unknown_key_is_none(ledger):
    assert ledger.get_status("nope") is None


def test_fallback_identity_hash_when_no_content_hash_given(mod, ledger):
    h = ledger.record_status(None, "discovered", source="s3://bucket", identifier="deep/path.pdf")
    assert h == mod.compute_identity_hash("s3://bucket", "deep/path.pdf")
    assert ledger.get_status(h)["status"] == "discovered"


def test_record_status_without_hash_or_source_identifier_raises(ledger):
    with pytest.raises(ValueError, match="content_hash is required"):
        ledger.record_status(None, "discovered")


def test_invalid_status_raises_a_clear_error(ledger):
    with pytest.raises(ValueError, match="status must be one of"):
        ledger.record_status("etag-1", "bogus_status")


def test_latest_write_wins_when_same_key_recorded_twice(ledger):
    ledger.record_status("etag-1", "discovered")
    ledger.record_status("etag-1", "processing")
    ledger.record_status("etag-1", "extracted")
    rec = ledger.get_status("etag-1")
    assert rec["status"] == "extracted"
    # the file itself is append-only -- all three lines are still on disk
    assert sum(1 for _ in open(ledger.ledger_path)) == 3


def test_list_by_status_filters_to_the_latest_status_only(ledger):
    ledger.record_status("etag-1", "discovered")
    ledger.record_status("etag-2", "discovered")
    ledger.record_status("etag-2", "processing")  # etag-2 no longer counts as discovered
    ledger.record_status("etag-3", "failed", detail="bad pdf")
    discovered = {r["content_hash"] for r in ledger.list_by_status("discovered")}
    assert discovered == {"etag-1"}
    failed = ledger.list_by_status("failed")
    assert len(failed) == 1 and failed[0]["detail"] == "bad pdf"


def test_list_by_status_invalid_status_raises(ledger):
    with pytest.raises(ValueError, match="status must be one of"):
        ledger.list_by_status("bogus_status")


def test_stuck_after_minutes_filters_to_old_processing_entries(ledger):
    ledger.record_status("fresh", "processing")
    ledger.record_status("stale", "processing")
    # backdate "stale"'s entry directly on disk, past its real write time
    lines = [json.loads(line) for line in open(ledger.ledger_path)]
    for rec in lines:
        if rec["content_hash"] == "stale":
            rec["ts"] = "2000-01-01T00:00:00+00:00"
    with open(ledger.ledger_path, "w") as f:
        for rec in lines:
            f.write(json.dumps(rec) + "\n")

    stuck = ledger.list_by_status("processing", stuck_after_minutes=30)
    assert {r["content_hash"] for r in stuck} == {"stale"}
    not_stuck = ledger.list_by_status("processing")
    assert {r["content_hash"] for r in not_stuck} == {"fresh", "stale"}


def test_summary_counts_and_failed_cap(ledger):
    ledger.record_status("e1", "discovered")
    ledger.record_status("e2", "processing")
    ledger.record_status("e3", "extracted")
    ledger.record_status("e4", "reviewed")
    for i in range(5):
        ledger.record_status(f"f{i}", "failed", detail=f"error {i}")
    data = ledger.summary(recent_failed_cap=3)
    assert data["counts"] == {"discovered": 1, "processing": 1, "extracted": 1, "failed": 5, "reviewed": 1}
    assert data["total"] == 9
    assert len(data["recent_failed"]) == 3
    # most recently written failures come first
    assert data["recent_failed"][0]["content_hash"] == "f4"


def test_summary_default_cap_comes_from_the_resource_field(mod, tmp_path):
    ledger = mod.DocumentIntakeStatusLedger(ledger_path=str(tmp_path / "l.jsonl"), recent_failed_cap=2)
    for i in range(5):
        ledger.record_status(f"f{i}", "failed", detail=str(i))
    assert len(ledger.summary()["recent_failed"]) == 2


def materialize(comp, defs=None):
    defs = defs or comp.build_defs(None)
    return dg.materialize(defs.assets, resources=defs.resources, raise_on_error=False)


def test_intake_summary_asset_materializes_real_counts(mod, tmp_path):
    ledger_path = tmp_path / "batch.jsonl"
    comp = mod.DocumentIntakeStatusLedgerComponent(ledger_name="batch", ledger_path=str(ledger_path))
    defs = comp.build_defs(None)
    # seed the ledger the same way a downstream pipeline would, through the registered resource
    resource = defs.resources["document_intake_status_ledger"]
    resource.record_status("e1", "discovered")
    resource.record_status("e2", "processing")
    resource.record_status("e3", "extracted")
    resource.record_status("e4", "failed", detail="OCR timeout")

    result = materialize(comp, defs)
    assert result.success
    md = result.get_asset_materialization_events()[0].materialization.metadata
    assert md["count_discovered"].value == 1
    assert md["count_processing"].value == 1
    assert md["count_extracted"].value == 1
    assert md["count_failed"].value == 1
    assert md["count_reviewed"].value == 0
    assert md["total_count"].value == 4
    assert md["recent_failed"].value[0]["detail"] == "OCR timeout"
    assert result.output_for_node("batch_intake_summary")["total"] == 4


def test_stuck_check_passes_when_unset_and_fails_when_a_file_is_stuck(mod, tmp_path):
    ledger_path = tmp_path / "batch.jsonl"
    comp_no_check = mod.DocumentIntakeStatusLedgerComponent(ledger_name="batch", ledger_path=str(ledger_path))
    assert materialize(comp_no_check).get_asset_check_evaluations() == []

    comp = mod.DocumentIntakeStatusLedgerComponent(ledger_name="batch", ledger_path=str(ledger_path), stuck_after_minutes=30)
    defs = comp.build_defs(None)
    resource = defs.resources["document_intake_status_ledger"]
    resource.record_status("stuck1", "processing")
    lines = [json.loads(line) for line in open(ledger_path)]
    for rec in lines:
        rec["ts"] = "2000-01-01T00:00:00+00:00"
    with open(ledger_path, "w") as f:
        for rec in lines:
            f.write(json.dumps(rec) + "\n")

    result = materialize(comp, defs)
    (ev,) = result.get_asset_check_evaluations()
    assert not ev.passed and ev.severity == dg.AssetCheckSeverity.WARN
    assert ev.metadata["stuck_count"].value == 1
    assert ev.metadata["stuck_content_hashes"].value == ["stuck1"]


def test_bad_stuck_severity_raises_at_build_defs(mod, tmp_path):
    comp = mod.DocumentIntakeStatusLedgerComponent(
        ledger_name="batch", ledger_path=str(tmp_path / "l.jsonl"), stuck_after_minutes=30, stuck_severity="LOUD",
    )
    with pytest.raises(ValueError, match="stuck_severity"):
        comp.build_defs(None)


def test_loose_field_shapes_are_accepted(mod, tmp_path):
    c = mod.DocumentIntakeStatusLedgerComponent(
        ledger_name="batch", ledger_path=str(tmp_path / "l.jsonl"),
        kinds="ledger, status", owners="team:ai", asset_tags={"n": 1},
    )
    assert c.kinds == ["ledger", "status"] and c.owners == ["team:ai"] and c.asset_tags == {"n": "1"}
