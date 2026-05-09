"""TranslationApiAssetComponent — translate text columns via Cloud Translation v3.

Translates a column of strings from any source language (or auto-detected)
to a target language (or many target languages). Returns the upstream
DataFrame with one or more new translation columns.

Uses Cloud Translation API v3 ("Advanced") which supports glossaries +
custom models if you need them; for the simpler v2 ("Basic") shape, the
component still works with the default model.
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import pandas as pd

from dagster import (
    AssetExecutionContext,
    AssetIn,
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


class TranslationApiAssetComponent(Component, Model, Resolvable):
    """Translate a text column to one or more target languages."""

    asset_name: str = Field(description="Output asset name.")
    upstream_asset_key: str = Field(description="Upstream DataFrame asset key.")

    credentials: Optional[Dict[str, Any]] = Field(default=None)
    credentials_path: Optional[str] = Field(default=None, description="Falls back to GOOGLE_APPLICATION_CREDENTIALS.")

    project_id: Optional[str] = Field(default=None)
    location: str = Field(default="global", description="Translation location ('global' or a region for custom models).")

    text_column: str = Field(description="Column with text to translate.")
    target_languages: List[str] = Field(
        description="ISO codes to translate INTO, e.g. ['es', 'fr', 'de']. One output column per target.",
    )
    source_language: Optional[str] = Field(
        default=None,
        description="Source ISO code. If unset, the API auto-detects per row.",
    )
    output_prefix: str = Field(default="text_", description="Output columns named e.g. text_es / text_fr / text_de.")

    mime_type: str = Field(default="text/plain", description="text/plain or text/html.")

    batch_size: int = Field(default=64, description="Texts per Translation request (max 1024 per call).")
    rate_limit_delay: float = Field(default=0.0)
    max_retries: int = Field(default=3)

    description: Optional[str] = Field(default=None)
    group_name: Optional[str] = Field(default=None)
    deps: Optional[List[str]] = Field(default=None)
    tags: Optional[Dict[str, str]] = Field(default=None)
    owners: Optional[List[str]] = Field(default=None)

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        creds_dict = self.credentials
        if creds_dict is None:
            cred_path = self.credentials_path or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                with open(cred_path, "r") as fh:
                    creds_dict = json.load(fh)
        if creds_dict is None:
            raise ValueError("Provide credentials, credentials_path, or set GOOGLE_APPLICATION_CREDENTIALS.")

        asset_name = self.asset_name
        upstream_key = AssetKey.from_user_string(self.upstream_asset_key)
        project_id = self.project_id or creds_dict.get("project_id")
        location = self.location
        text_column = self.text_column
        target_languages = list(self.target_languages)
        source_language = self.source_language
        output_prefix = self.output_prefix
        mime_type = self.mime_type
        batch_size = self.batch_size
        rate_limit_delay = self.rate_limit_delay
        max_retries = self.max_retries

        @asset(
            name=asset_name,
            description=self.description or f"Translate {text_column} → {', '.join(target_languages)}.",
            group_name=self.group_name,
            kinds={"google", "translation", "ai"},
            tags=self.tags or None,
            owners=self.owners or None,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])] or None,
            ins={"upstream": AssetIn(key=upstream_key)},
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> Output:
            try:
                from google.cloud import translate_v3 as translate
                from google.oauth2 import service_account
            except ImportError:
                raise ImportError("pip install google-cloud-translate google-auth")

            if text_column not in upstream.columns:
                raise ValueError(f"text_column={text_column!r} not in upstream: {list(upstream.columns)}")

            sa_creds = service_account.Credentials.from_service_account_info(creds_dict)
            client = translate.TranslationServiceClient(credentials=sa_creds)
            parent = f"projects/{project_id}/locations/{location}"

            df = upstream.copy().reset_index(drop=True)
            texts = df[text_column].astype(str).tolist()

            # One column per target language. Translate-API supports a single
            # target per request, so we loop over target_languages and batch
            # the texts within each call.
            for lang in target_languages:
                col = f"{output_prefix}{lang}"
                df[col] = pd.Series([None] * len(df), dtype=object)

                for batch_start in range(0, len(texts), batch_size):
                    batch_texts = texts[batch_start:batch_start + batch_size]
                    request = {
                        "parent": parent,
                        "contents": batch_texts,
                        "mime_type": mime_type,
                        "target_language_code": lang,
                    }
                    if source_language:
                        request["source_language_code"] = source_language

                    attempt = 0
                    last_err = None
                    resp = None
                    while attempt <= max_retries:
                        try:
                            resp = client.translate_text(request=request)
                            last_err = None
                            break
                        except Exception as e:
                            last_err = e
                            attempt += 1
                            if attempt > max_retries:
                                break
                            time.sleep((2 ** attempt) * 0.5)

                    if last_err is not None or resp is None:
                        err_str = str(last_err) if last_err else "no response"
                        if "PERMISSION_DENIED" in err_str:
                            context.log.error(
                                "Translation API: 403 PERMISSION_DENIED. Service account "
                                "needs roles/cloudtranslate.user. Translation API enabled?"
                            )
                        for j in range(len(batch_texts)):
                            df.at[batch_start + j, col] = None
                        continue

                    for j, t in enumerate(resp.translations):
                        df.at[batch_start + j, col] = t.translated_text
                    if rate_limit_delay > 0:
                        time.sleep(rate_limit_delay)

            preview_md = df.head(5).to_markdown(index=False) or ""
            return Output(
                value=df,
                metadata={
                    "rows":             MetadataValue.int(len(df)),
                    "target_languages": MetadataValue.json(target_languages),
                    "preview":          MetadataValue.md(preview_md),
                },
            )

        return Definitions(assets=[_asset])
