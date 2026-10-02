"""Committed regression tests for FigmentStakingRewardsIngestionComponent.

The real `requests` network call is never made here -- `_fetch_rewards`
(the one external, paid-API boundary) is monkeypatched wholesale, while
request-body construction, response flattening, and pagination following
are all exercised for real.
"""
import dagster as dg
import pytest

from .conftest import FakeFigmentResource, load_component_module, metadata_for, output_value


@pytest.fixture()
def mod():
    return load_component_module()


def _materialize(component, resource):
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    return dg.materialize([asset_def], resources={component.resource_name: resource})


# --- _build_rewards_body (pure) ----------------------------------------------

def test_build_body_ethereum_includes_filters(mod):
    body = mod._build_rewards_body(
        "ethereum", "daily", "2026-06-01T00:00:00Z", "2026-07-01T00:00:00Z",
        ["0xabc"], ["0xdef"], None, None,
    )
    assert body == {
        "time_rollup": "daily",
        "start": "2026-06-01T00:00:00Z",
        "end": "2026-07-01T00:00:00Z",
        "pubkeys": ["0xabc"],
        "withdrawal_addresses": ["0xdef"],
    }


def test_build_body_solana_requires_accounts(mod):
    with pytest.raises(ValueError, match="accounts"):
        mod._build_rewards_body("solana", None, None, None, None, None, None, None)


def test_build_body_solana_caps_at_50_accounts(mod):
    accounts = [f"acct{i}" for i in range(51)]
    with pytest.raises(ValueError, match="50"):
        mod._build_rewards_body("solana", None, None, None, None, None, accounts, None)


def test_build_body_solana_valid(mod):
    body = mod._build_rewards_body("solana", None, None, None, None, None, ["acct1", "acct2"], None)
    assert body == {"accounts": ["acct1", "acct2"]}


def test_build_body_extra_query_params_merged(mod):
    body = mod._build_rewards_body("ethereum", None, None, None, None, None, None, {"custom_field": "x"})
    assert body == {"custom_field": "x"}


# --- _flatten_rewards_payload (pure) ------------------------------------------

def test_flatten_bare_list(mod):
    rows = mod._flatten_rewards_payload([{"epoch": 1}, {"epoch": 2}], "ethereum")
    assert len(rows) == 2
    assert all(r["network"] == "ethereum" for r in rows)


def test_flatten_dict_with_data_key(mod):
    rows = mod._flatten_rewards_payload({"data": [{"epoch": 1}]}, "ethereum")
    assert rows == [{"epoch": 1, "network": "ethereum"}]


def test_flatten_dict_with_rewards_key(mod):
    rows = mod._flatten_rewards_payload({"rewards": [{"amount": 5}]}, "solana")
    assert rows == [{"amount": 5, "network": "solana"}]


def test_flatten_dict_no_list_key_wraps_as_single_row(mod):
    rows = mod._flatten_rewards_payload({"total_reward": 123}, "ethereum")
    assert rows == [{"total_reward": 123, "network": "ethereum"}]


# --- config-time validation ----------------------------------------------------

def test_empty_network_raises(mod):
    with pytest.raises(ValueError, match="network"):
        mod.FigmentStakingRewardsIngestionComponent(asset_name="x", network="").build_defs(context=None)


def test_solana_without_accounts_raises_at_build_defs(mod):
    with pytest.raises(ValueError, match="accounts"):
        mod.FigmentStakingRewardsIngestionComponent(
            asset_name="x", network="solana"
        ).build_defs(context=None)


# --- end-to-end ingestion ------------------------------------------------------

def test_ethereum_end_to_end(mod, monkeypatch):
    captured_bodies = []

    def _fake_fetch(client, base_url, network, body):
        captured_bodies.append(body)
        return {"data": [{"epoch": 100, "amount_gwei": 1234}]}

    monkeypatch.setattr(mod, "_fetch_rewards", _fake_fetch)

    component = mod.FigmentStakingRewardsIngestionComponent(
        asset_name="figment_eth_rewards",
        network="ethereum",
        time_rollup="epoch",
        pubkeys=["0xabc"],
    )
    result = _materialize(component, FakeFigmentResource())
    assert result.success

    df = output_value(result, "figment_eth_rewards")
    assert len(df) == 1
    assert df.iloc[0]["amount_gwei"] == 1234
    assert df.iloc[0]["network"] == "ethereum"

    out = metadata_for(result, "figment_eth_rewards")
    assert out["row_count"] == 1
    assert out["network"] == "ethereum"
    assert captured_bodies[0]["pubkeys"] == ["0xabc"]


def test_solana_end_to_end(mod, monkeypatch):
    def _fake_fetch(client, base_url, network, body):
        assert network == "solana"
        assert body == {"accounts": ["stake1"]}
        return [{"stake_account": "stake1", "net_reward": 42}]

    monkeypatch.setattr(mod, "_fetch_rewards", _fake_fetch)

    component = mod.FigmentStakingRewardsIngestionComponent(
        asset_name="figment_sol_rewards",
        network="solana",
        accounts=["stake1"],
    )
    result = _materialize(component, FakeFigmentResource())
    assert result.success
    df = output_value(result, "figment_sol_rewards")
    assert len(df) == 1
    assert df.iloc[0]["net_reward"] == 42


def test_pagination_follows_next_page_until_absent(mod, monkeypatch):
    pages = [
        {"data": [{"epoch": 1}], "next_page": "cursor-2"},
        {"data": [{"epoch": 2}], "next_page": None},
    ]

    def _fake_fetch(client, base_url, network, body):
        return pages.pop(0)

    monkeypatch.setattr(mod, "_fetch_rewards", _fake_fetch)

    component = mod.FigmentStakingRewardsIngestionComponent(
        asset_name="figment_eth_rewards",
        network="ethereum",
    )
    result = _materialize(component, FakeFigmentResource())
    assert result.success
    out = metadata_for(result, "figment_eth_rewards")
    assert out["pages_fetched"] == 2
    assert out["row_count"] == 2


def test_max_pages_caps_pagination(mod, monkeypatch):
    call_count = {"n": 0}

    def _fake_fetch(client, base_url, network, body):
        call_count["n"] += 1
        return {"data": [{"epoch": call_count["n"]}], "next_page": "always-more"}

    monkeypatch.setattr(mod, "_fetch_rewards", _fake_fetch)

    component = mod.FigmentStakingRewardsIngestionComponent(
        asset_name="figment_eth_rewards",
        network="ethereum",
        max_pages=3,
    )
    result = _materialize(component, FakeFigmentResource())
    assert result.success
    out = metadata_for(result, "figment_eth_rewards")
    assert out["pages_fetched"] == 3
    assert call_count["n"] == 3


def test_include_aggregate_summary_ethereum_issues_second_call(mod, monkeypatch):
    calls = []

    def _fake_fetch(client, base_url, network, body):
        calls.append(dict(body))
        if body.get("time_rollup") == "all_time":
            return {"total_reward": 999}
        return {"data": [{"epoch": 1}]}

    monkeypatch.setattr(mod, "_fetch_rewards", _fake_fetch)

    component = mod.FigmentStakingRewardsIngestionComponent(
        asset_name="figment_eth_rewards",
        network="ethereum",
        include_aggregate_summary=True,
    )
    result = _materialize(component, FakeFigmentResource())
    assert result.success
    assert len(calls) == 2
    assert calls[1]["time_rollup"] == "all_time"
    out = metadata_for(result, "figment_eth_rewards")
    assert out["aggregate_summary"] == {"total_reward": 999}


def test_include_aggregate_summary_skipped_for_non_ethereum(mod, monkeypatch):
    calls = []

    def _fake_fetch(client, base_url, network, body):
        calls.append(dict(body))
        return [{"net_reward": 1}]

    monkeypatch.setattr(mod, "_fetch_rewards", _fake_fetch)

    component = mod.FigmentStakingRewardsIngestionComponent(
        asset_name="figment_sol_rewards",
        network="solana",
        accounts=["stake1"],
        include_aggregate_summary=True,
    )
    result = _materialize(component, FakeFigmentResource())
    assert result.success
    assert len(calls) == 1  # no second all_time call for solana


def test_no_rewards_returns_empty_dataframe(mod, monkeypatch):
    def _fake_fetch(client, base_url, network, body):
        return {"data": []}

    monkeypatch.setattr(mod, "_fetch_rewards", _fake_fetch)

    component = mod.FigmentStakingRewardsIngestionComponent(
        asset_name="figment_eth_rewards",
        network="ethereum",
    )
    result = _materialize(component, FakeFigmentResource())
    assert result.success
    out = metadata_for(result, "figment_eth_rewards")
    assert out["row_count"] == 0
