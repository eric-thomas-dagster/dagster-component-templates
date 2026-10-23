"""teams_approval_gate's backfill_policy was missing entirely (field + wiring). Before this fix,
`raw_key = context.partition_key if context.has_partition_key else "default"` silently collapsed any
partition-key RANGE onto the literal "default" sidecar-state key, so two different backfill ranges would
collide on the exact same posted-message state file. Now a range builds a joined key instead. Real
dg.materialize() run (not direct invocation); only the Graph API auth + post call are mocked."""
import dagster as dg

from .conftest import load_component_module

PARTS = dg.DailyPartitionsDefinition("2024-01-01")


def _build(monkeypatch, tmp_path, calls, backfill_policy=None):
    mod = load_component_module()
    monkeypatch.setattr(mod, "_get_teams_bearer", lambda tenant, client_id, secret: "fake-bearer")

    def fake_post(bearer, team_id, channel_id, html):
        calls.append(html)
        return {"message_id": "m1", "web_url": "https://teams.example/m1"}

    monkeypatch.setattr(mod, "_post_teams_message", fake_post)

    comp = mod.TeamsApprovalGateComponent(
        asset_name="gate",
        upstream_asset_key="upstream_x",
        approval_dir=str(tmp_path),
        teams_tenant_id_env_var="TEAMS_TENANT",
        teams_client_id_env_var="TEAMS_CLIENT",
        teams_client_secret_env_var="TEAMS_SECRET",
        teams_team_id="T1",
        teams_channel_id="C1",
        approver_allowlist=["alice@example.com"],
        required_approvers=1,
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


def test_single_run_over_a_range_posts_one_message_with_a_joined_key(monkeypatch, tmp_path):
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
    sidecar_files = list(tmp_path.iterdir())
    assert any("2024-01-01" in f.name and "2024-01-02" in f.name for f in sidecar_files), (
        f"expected the sidecar state file to be keyed by the joined range, got {[f.name for f in sidecar_files]}"
    )
