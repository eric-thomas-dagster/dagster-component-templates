"""Facebook Ads Workspace Component.

Auto-enumerates Facebook Ad accounts + campaigns via the Marketing API.
Emits one Dagster asset per (ad_account, campaign). Materializing runs
an insights query against the campaign.
"""
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import dagster as dg

try:
    from dagster.components.component.state_backed_component import StateBackedComponent
    from dagster.components.utils.defs_state import (
        DefsStateConfig,
        DefsStateConfigArgs,
        ResolvedDefsStateConfig,
    )
    _HAS_STATE_BACKED = True
except ImportError:
    StateBackedComponent = None  # type: ignore
    DefsStateConfig = None  # type: ignore
    DefsStateConfigArgs = None  # type: ignore
    ResolvedDefsStateConfig = Any  # type: ignore
    _HAS_STATE_BACKED = False


@dataclass
class FacebookAdsCampaignSelector(dg.Resolvable):
    by_name: Optional[List[str]] = None
    by_pattern: Optional[List[str]] = None
    exclude_by_name: Optional[List[str]] = None
    exclude_by_pattern: Optional[List[str]] = None

    def matches(self, name: str) -> bool:
        import fnmatch
        if self.exclude_by_name and name in self.exclude_by_name:
            return False
        if self.exclude_by_pattern and any(fnmatch.fnmatch(name, p) for p in self.exclude_by_pattern):
            return False
        if not self.by_name and not self.by_pattern:
            return True
        if self.by_name and name in self.by_name:
            return True
        if self.by_pattern and any(fnmatch.fnmatch(name, p) for p in self.by_pattern):
            return True
        return False


def _enumerate_fb(access_token, account_ids, api_version, verify_ssl) -> dict:
    import requests
    session = requests.Session()
    session.verify = verify_ssl
    headers = {"Authorization": f"Bearer {access_token}"}

    out: dict = {"accounts": []}
    if not account_ids:
        return out
    for acct in account_ids:
        acct_id = acct if acct.startswith("act_") else f"act_{acct}"
        campaigns: list = []
        try:
            r = session.get(
                f"https://graph.facebook.com/{api_version}/{acct_id}/campaigns",
                headers=headers, params={"fields": "id,name,status,objective"},
                timeout=60,
            )
            r.raise_for_status()
            for c in (r.json() or {}).get("data", []):
                cid = c.get("id")
                cname = c.get("name")
                if cid and cname:
                    campaigns.append({"id": cid, "name": cname, "status": c.get("status")})
        except Exception:  # noqa: BLE001
            pass
        out["accounts"].append({"id": acct_id, "campaigns": campaigns})
    return out


if _HAS_STATE_BACKED:

    @dataclass
    class FacebookAdsWorkspaceComponent(StateBackedComponent, dg.Resolvable):
        access_token_env_var: str
        account_ids: List[str] = field(default_factory=list)  # e.g. ["1234567890"] or ["act_1234567890"]
        api_version: str = "v18.0"
        verify_ssl: bool = True

        campaign_selector: Optional[FacebookAdsCampaignSelector] = None
        date_preset: str = "last_30d"
        insights_fields: List[str] = field(default_factory=lambda: [
            "campaign_name", "impressions", "clicks", "spend", "reach", "ctr", "cpc",
        ])

        group_name: Optional[str] = None
        asset_key_prefix: List[str] = field(default_factory=lambda: ["facebook_ads"])
        compute_kind: str = "facebook_ads"

        defs_state: Optional[ResolvedDefsStateConfig] = None

        @property
        def defs_state_config(self) -> "DefsStateConfig":
            _key = f"FacebookAdsWorkspace[{hashlib.sha256(self.access_token_env_var.encode()).hexdigest()[:12]}]"
            return DefsStateConfig.from_args(self.defs_state, default_key=_key)

        def write_state_to_path(self, state_path: Path) -> None:
            import os
            tok = os.environ.get(self.access_token_env_var, "")
            snapshot = _enumerate_fb(tok, self.account_ids, self.api_version, self.verify_ssl)
            if self.campaign_selector is not None:
                for a in snapshot["accounts"]:
                    a["campaigns"] = [c for c in a["campaigns"] if self.campaign_selector.matches(c["name"])]
            state_path.write_text(json.dumps(snapshot, indent=2))

        def build_defs_from_state(
            self, context: dg.ComponentLoadContext, state_path: Optional[Path],
        ) -> dg.Definitions:
            if state_path is None or not state_path.exists():
                return dg.Definitions()
            state = json.loads(state_path.read_text())
            assets = []
            for a in state.get("accounts", []):
                for c in a.get("campaigns", []):
                    assets.append(self._build_asset(a["id"], c["id"], c["name"]))
            return dg.Definitions(assets=assets)

        def _build_asset(self, acct_id: str, campaign_id: str, campaign_name: str):
            _self = self
            safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in campaign_name)[:60] or campaign_id
            key = dg.AssetKey([*self.asset_key_prefix, acct_id, safe])

            @dg.asset(
                key=key, group_name=self.group_name, compute_kind=self.compute_kind,
                metadata={
                    "fb_ad_account": dg.MetadataValue.text(acct_id),
                    "fb_campaign_id": dg.MetadataValue.text(campaign_id),
                    "fb_campaign_name": dg.MetadataValue.text(campaign_name),
                },
            )
            def _asset(context: dg.AssetExecutionContext):
                import os
                try:
                    import pandas as pd
                    import requests
                except ImportError as e:
                    raise Exception("pandas or requests library not installed") from e

                tok = os.environ.get(_self.access_token_env_var, "")
                session = requests.Session()
                session.verify = _self.verify_ssl
                headers = {"Authorization": f"Bearer {tok}"}

                url = f"https://graph.facebook.com/{_self.api_version}/{campaign_id}/insights"
                r = session.get(url, headers=headers, params={
                    "fields": ",".join(_self.insights_fields),
                    "date_preset": _self.date_preset,
                    "level": "campaign",
                }, timeout=60)
                r.raise_for_status()
                rows = (r.json() or {}).get("data", [])
                df = pd.DataFrame(rows)
                context.add_output_metadata({
                    "row_count": len(df),
                    "fb_campaign": campaign_name,
                    "date_preset": _self.date_preset,
                })
                return df

            return _asset

else:
    class FacebookAdsWorkspaceComponent:  # type: ignore
        def __init__(self, *args, **kwargs):
            raise ImportError("FacebookAdsWorkspaceComponent requires Dagster with StateBackedComponent support.")
