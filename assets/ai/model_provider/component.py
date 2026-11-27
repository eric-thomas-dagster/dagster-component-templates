"""ModelProviderComponent -- declare an LLM provider's credential and surface its health.

Companion to the agent components (openai_agent / anthropic_agent / litellm_agent /
gemini_agent): those read their API key fresh from an env var at call time and never
validate it ahead of a real agent run. This component makes that validation a first-class,
inspectable Dagster object -- an observable source asset reporting which models the
provider offers, plus two asset checks: a free api-key-presence check (always on) and an
opt-in live-connectivity check that makes one real models-list API call using the key.

Engineering-agents feedback round 3, item 6: "a 'model provider' component (key env var +
presence check + optional test-call asset check + models offered)". Built on LiteLLM
(already a dependency of litellm_agent) rather than one bespoke HTTP client per provider --
`litellm.get_valid_models()` gives the static per-provider model catalog for free, and the
internal `ProviderConfigManager`/`provider_config.get_models()` building blocks it uses
itself give a real, provider-native models-list call for the opt-in live check.
"""

import os
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    ObserveResult,
    Resolvable,
    observable_source_asset,
)
from pydantic import Field, model_validator


# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=(), strs=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    for k in strs:  # str (or Dict[str, str]) fields: a numeric-looking YAML scalar resolves to int/float/bool
        v = out.get(k)
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (int, float)):
            out[k] = str(v)
        elif isinstance(v, dict):
            out[k] = {str(a): (("true" if b is True else "false") if isinstance(b, bool) else (str(b) if isinstance(b, (int, float)) else b)) for a, b in v.items()}
    return out
# END field-shape normalization


# litellm's own `BedrockModelInfo.get_models()` is a permanent no-op returning [] --
# confirmed by reading litellm's source (it never issues bedrock an HTTP call there; auth
# there is AWS SigV4, not a bearer api_key). Without this, an empty result would be
# misreported as "verified" for bedrock when no call was actually made.
_LIVE_CHECK_STUB_PROVIDERS = frozenset({"bedrock"})


def models_offered_for_provider(provider: str, declared_models: Optional[List[str]] = None) -> List[str]:
    """Models this provider offers -- no network call, no credential needed.

    A `declared_models` override lets the component author curate the exact subset they
    intend to expose; otherwise falls back to LiteLLM's own static per-provider model
    catalog (`litellm.get_valid_models(custom_llm_provider=...)`, which -- passed an
    explicit provider -- returns the catalog unconditionally, independent of whether any
    credential env var is set)."""
    if declared_models:
        return list(declared_models)
    try:
        import litellm
    except ImportError:
        return []
    try:
        return list(litellm.get_valid_models(custom_llm_provider=provider))
    except Exception:
        return []


def run_live_connectivity_check(provider: str, api_key: Optional[str], api_base: Optional[str]) -> Dict[str, Any]:
    """Make one real models-list API call to `provider` using `api_key`.

    Returns {"supported", "ok", "models", "error"}. Reuses LiteLLM's own
    `ProviderConfigManager`/`provider_config.get_models()` building blocks directly rather
    than the top-level `litellm.get_valid_models(check_provider_endpoint=True)`, which
    swallows every exception down to an empty list -- callers here want the real auth/
    connectivity error surfaced, not a silent empty result.

    `supported=False` means this provider has no live models-list endpoint plumbed into
    this litellm version (confirmed for e.g. `bedrock`, `azure`, `groq`, `cohere`,
    `mistral`, and anything `vertex_ai`-style that raises NotImplementedError) -- a status,
    not a failure: `ok` stays True so an unsupported provider doesn't fail the check for
    something the user can't fix."""
    try:
        from litellm.types.utils import LlmProviders
        from litellm.utils import ProviderConfigManager
    except ImportError:
        raise ImportError(
            "model_provider's test_call option requires the 'litellm' package. "
            "Install with: pip install litellm"
        )

    if provider in _LIVE_CHECK_STUB_PROVIDERS:
        return {"supported": False, "ok": True, "models": [], "error": None}

    try:
        provider_enum = LlmProviders(provider)
    except ValueError:
        return {"supported": False, "ok": True, "models": [], "error": None}

    provider_config = ProviderConfigManager.get_provider_model_info(model=None, provider=provider_enum)
    if provider_config is None:
        return {"supported": False, "ok": True, "models": [], "error": None}

    try:
        models = provider_config.get_models(api_key=api_key, api_base=api_base)
    except NotImplementedError:
        return {"supported": False, "ok": True, "models": [], "error": None}
    except Exception as e:
        return {"supported": True, "ok": False, "models": [], "error": str(e)}

    return {"supported": True, "ok": True, "models": list(models), "error": None}


class ModelProviderComponent(Component, Model, Resolvable):
    """Declare an LLM provider credential and surface its health as a Dagster asset.

    Example:
        ```yaml
        type: dagster_community_components.ModelProviderComponent
        attributes:
          asset_name: openai_provider
          provider: openai
          api_key_env_var: OPENAI_API_KEY
          test_call: true
        ```
    """

    asset_name: str = Field(description="Output Dagster asset name.")
    provider: str = Field(
        description=(
            "LiteLLM provider id, e.g. openai, anthropic, gemini, azure, bedrock, groq, "
            "mistral, cohere, vertex_ai -- same vocabulary litellm_agent's `model` field "
            "implies (e.g. 'gemini/gemini-2.5-flash' -> provider 'gemini')."
        ),
    )
    api_key_env_var: str = Field(
        description=(
            "Env var holding this provider's API key. Only its presence is checked and "
            "only its name is ever stored in this component's config -- the value is "
            "read fresh from the environment at check time, never templated into a typed "
            "field or logged."
        ),
    )
    api_base_env_var: Optional[str] = Field(
        default=None,
        description="Env var holding a custom API base URL (proxies, self-hosted gateways, Azure endpoints).",
    )
    models: Optional[List[str]] = Field(
        default=None,
        description=(
            "Curated list of model ids this provider is offered for. When unset, falls "
            "back to LiteLLM's own static model catalog for `provider`."
        ),
    )
    test_call: bool = Field(
        default=False,
        description=(
            "When true, adds a second asset check that makes one real models-list API "
            "call to the provider using the live key, to verify it actually authenticates "
            "-- not just that the env var is set. Costs one real network call per check "
            "run; silently reported as 'not supported' rather than failed for providers "
            "with no live models-list endpoint in this litellm version."
        ),
    )
    severity: str = Field(default="ERROR", description="Check severity when a check fails: 'WARN' or 'ERROR'.")
    group_name: Optional[str] = Field(default="model_providers", description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description shown in the Dagster catalog.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Key-value catalog tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (auto-includes 'llm').")
    deps: Optional[List[str]] = Field(default=None, description="Upstream asset keys this asset depends on.")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=("models", "owners", "kinds", "deps"), csv=(), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label="Model Provider", editable=True)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        api_key_env_var = self.api_key_env_var
        api_base_env_var = self.api_base_env_var
        declared_models = list(self.models) if self.models else None
        test_call = self.test_call
        severity = AssetCheckSeverity.WARN if self.severity.upper() == "WARN" else AssetCheckSeverity.ERROR
        asset_key = AssetKey.from_user_string(asset_name)
        kinds = set(self.kinds) if self.kinds else set()
        kinds.add("llm")

        @observable_source_asset(
            key=asset_key,
            description=self.description or f"Model provider '{provider}' -- credential health and models offered.",
            group_name=self.group_name,
            kinds=kinds,
            owners=self.owners,
            tags=self.asset_tags,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _model_provider_asset(context) -> ObserveResult:
            models_offered = models_offered_for_provider(provider, declared_models)
            api_key_present = bool(os.environ.get(api_key_env_var))
            context.log.info(
                f"model_provider:{provider} -- api_key_present={api_key_present} "
                f"models_offered={len(models_offered)}"
            )
            return ObserveResult(
                metadata={
                    "provider": provider,
                    "api_key_env_var": api_key_env_var,
                    "api_key_present": api_key_present,
                    "models_offered": MetadataValue.json(models_offered),
                    "models_offered_count": len(models_offered),
                }
            )

        @dg.asset_check(
            asset=asset_key,
            name=f"{asset_name}_api_key_present",
            description=f"Whether {api_key_env_var} is set in the environment.",
        )
        def _api_key_present_check(context: dg.AssetCheckExecutionContext) -> AssetCheckResult:
            present = bool(os.environ.get(api_key_env_var))
            return AssetCheckResult(
                passed=present,
                severity=severity,
                description=(
                    f"{api_key_env_var} is set." if present else f"{api_key_env_var} is not set."
                ),
                metadata={"api_key_env_var": api_key_env_var, "api_key_present": present},
            )

        checks = [_api_key_present_check]

        if test_call:
            @dg.asset_check(
                asset=asset_key,
                name=f"{asset_name}_live_connectivity",
                description=f"Whether a real models-list call to the live '{provider}' API succeeds using {api_key_env_var}.",
            )
            def _live_connectivity_check(context: dg.AssetCheckExecutionContext) -> AssetCheckResult:
                result = run_live_connectivity_check(
                    provider,
                    os.environ.get(api_key_env_var),
                    os.environ.get(api_base_env_var) if api_base_env_var else None,
                )
                if not result["supported"]:
                    description = (
                        f"Provider '{provider}' has no live models-list endpoint in this "
                        f"litellm version -- only the api-key-presence check applies."
                    )
                elif result["ok"]:
                    description = f"Authenticated and listed {len(result['models'])} models from the live '{provider}' API."
                else:
                    description = f"Live connectivity check failed: {result['error']}"
                return AssetCheckResult(
                    passed=result["ok"],
                    severity=severity,
                    description=description,
                    metadata={
                        "supported": result["supported"],
                        "models_found": len(result["models"]),
                        "models_sample": MetadataValue.json(result["models"][:20]),
                        "error": result["error"] or MetadataValue.null(),
                    },
                )

            checks.append(_live_connectivity_check)

        return Definitions(assets=[_model_provider_asset], asset_checks=checks)
