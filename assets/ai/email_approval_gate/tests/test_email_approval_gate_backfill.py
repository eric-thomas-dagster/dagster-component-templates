"""email_approval_gate's backfill_policy was missing entirely (field + wiring). Before this fix,
`raw_key = context.partition_key if context.has_partition_key else "default"` silently collapsed any
partition-key RANGE onto the literal "default" sidecar-state key, so two different backfill ranges (or a
range and an ad-hoc unpartitioned run) would collide on the exact same approval-state file. Now a range
builds a joined key instead. Real dg.materialize() run (not direct invocation); only _send_email is mocked."""
import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _build(monkeypatch, tmp_path, calls, backfill_policy=None):
    mod = load_component_module()
    monkeypatch.setenv("SMTP_USER", "u")
    monkeypatch.setenv("SMTP_PASS", "p")

    def fake_send_email(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(mod, "_send_email", fake_send_email)

    comp = mod.EmailApprovalGateComponent(
        asset_name="gate",
        upstream_asset_key="upstream_x",
        approval_dir=str(tmp_path),
        smtp_server="smtp.example.com",
        smtp_username_env_var="SMTP_USER",
        smtp_password_env_var="SMTP_PASS",
        from_email="bot@example.com",
        to_emails=["approver@example.com"],
        imap_server="imap.example.com",
        imap_username_env_var="SMTP_USER",
        imap_password_env_var="SMTP_PASS",
        imap_mailbox="INBOX",
        approver_allowlist=["approver@example.com"],
        backfill_policy=backfill_policy,
        partition_type="daily",
        partition_start="2024-01-01",
    )
    defs = comp.build_defs(None)
    posted = next(a for a in defs.assets if hasattr(a, "node_def") and "posted" in str(a.node_def.name))
    return posted


def _upstream():
    @dg.asset(name="upstream_x", partitions_def=PARTS, backfill_policy=dg.BackfillPolicy.single_run())
    def upstream_x():
        return "content"
    return upstream_x


def test_backfill_policy_single_run_sets_a_real_backfill_policy(monkeypatch, tmp_path):
    posted = _build(monkeypatch, tmp_path, [], backfill_policy="single_run")
    assert posted.backfill_policy == dg.BackfillPolicy.single_run()


def test_single_run_over_a_range_sends_one_email_with_a_joined_key(monkeypatch, tmp_path):
    calls = []
    posted = _build(monkeypatch, tmp_path, calls, backfill_policy="single_run")
    result = dg.materialize(
        [posted, _upstream()],
        tags={
            "dagster/asset_partition_range_start": "2024-01-01",
            "dagster/asset_partition_range_end": "2024-01-02",
        },
        resources={"io_manager": dg.InMemoryIOManager()},
    )
    assert result.success
    assert len(calls) == 1
    assert "2024-01-01" in calls[0]["subject"] and "2024-01-02" in calls[0]["subject"]
    assert "default" not in calls[0]["subject"]
    sidecar_files = list(tmp_path.iterdir())
    assert len(sidecar_files) == 1
    assert "2024-01-01" in sidecar_files[0].name and "2024-01-02" in sidecar_files[0].name


def test_single_partition_still_uses_its_own_key_not_default(monkeypatch, tmp_path):
    calls = []
    posted = _build(monkeypatch, tmp_path, calls, backfill_policy="single_run")
    result = dg.materialize([posted, _upstream()], partition_key="2024-01-01")
    assert result.success
    assert calls[0]["subject"].count("2024-01-01") >= 1
    assert "default" not in calls[0]["subject"]
