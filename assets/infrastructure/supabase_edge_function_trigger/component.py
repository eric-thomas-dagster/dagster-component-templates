"""SupabaseEdgeFunctionTriggerComponent — invoke a Supabase Edge Function.

Materializable asset that POSTs to
``https://<project-ref>.supabase.co/functions/v1/<function-name>``
with the configured payload. Uses the SupabaseResource client's
``.functions.invoke()`` helper so auth headers (Bearer + apikey) are
handled automatically.
"""

from typing import Any, Dict, List, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Output,
    Resolvable,
    asset,
)
from pydantic import Field


class SupabaseEdgeFunctionTriggerComponent(Component, Model, Resolvable):
    """Invoke a Supabase Edge Function as a materializable asset."""

    asset_key: str = Field(description="Output asset key, e.g. 'notifications/send_email'.")
    function_name: str = Field(
        description="Edge Function name (the slug deployed to /functions/v1/<function-name>).",
    )
    resource_name: str = Field(
        default="supabase_resource",
        description="Resource key of the SupabaseResource providing the client.",
    )
    payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="JSON body to POST to the edge function. If omitted, an empty body is sent.",
    )
    wait_for_result: bool = Field(
        default=True,
        description=(
            "Whether to wait for the function to complete and capture its response. "
            "Set false for fire-and-forget style invocations (still HTTP-synchronous — "
            "Supabase Edge Functions don't expose async invoke — but response body is ignored)."
        ),
    )
    headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Extra HTTP headers to forward to the edge function.",
    )
    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)
    kinds: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_key_str = self.asset_key
        function_name = self.function_name
        resource_name = self.resource_name
        payload = self.payload
        wait_for_result = self.wait_for_result
        extra_headers = dict(self.headers or {})

        _kinds = set(self.kinds or []) | {"supabase", "edge-function"}

        @asset(
            key=AssetKey.from_user_string(asset_key_str),
            description=self.description or f"Invoke Supabase Edge Function {function_name!r}.",
            group_name=self.group_name,
            kinds=_kinds,
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            required_resource_keys={resource_name},
        )
        def _asset(context: AssetExecutionContext) -> Output:
            supabase_resource = getattr(context.resources, resource_name)
            client = supabase_resource.get_client()

            body = payload if payload is not None else {}
            invoke_options: Dict[str, Any] = {"body": body}
            if extra_headers:
                invoke_options["headers"] = extra_headers

            context.log.info(
                f"Invoking Supabase Edge Function {function_name!r} "
                f"(wait_for_result={wait_for_result})"
            )

            try:
                # supabase-py 2.x: client.functions.invoke(name, invoke_options)
                # Returns bytes (raw body). Older versions returned dict; try both.
                raw = client.functions.invoke(function_name, invoke_options)
            except Exception as e:
                context.log.error(f"Edge function invocation failed: {e}")
                raise

            # Normalize response.
            response_text: str = ""
            response_json: Optional[Any] = None
            if isinstance(raw, (bytes, bytearray)):
                try:
                    response_text = raw.decode("utf-8", errors="replace")
                except Exception:
                    response_text = repr(raw)
            elif isinstance(raw, str):
                response_text = raw
            elif isinstance(raw, dict):
                response_json = raw
                response_text = str(raw)
            else:
                response_text = str(raw)

            if response_json is None and response_text:
                try:
                    import json
                    response_json = json.loads(response_text)
                except Exception:
                    response_json = None

            metadata: Dict[str, Any] = {
                "function": MetadataValue.text(function_name),
                "url_path": MetadataValue.text(f"/functions/v1/{function_name}"),
                "waited": MetadataValue.bool(wait_for_result),
            }
            if wait_for_result:
                if response_json is not None:
                    metadata["response"] = MetadataValue.json(response_json)
                elif response_text:
                    metadata["response_text"] = MetadataValue.text(response_text[:5000])

            return Output(
                value=response_json if response_json is not None else response_text,
                metadata=metadata,
            )

        return Definitions(assets=[_asset])
