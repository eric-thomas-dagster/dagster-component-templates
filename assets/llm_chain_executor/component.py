"""LLM Chain Executor Asset Component.

Execute chains of LLM calls with context passing and templating.
"""

import os
import json
from typing import Optional, List, Dict, Any

from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    asset,
    Resolvable,
    Model,
)
from pydantic import Field


class LLMChainExecutorComponent(Component, Model, Resolvable):
    """Component for executing LLM chains.

    Accepts context from upstream assets via IO manager.

    Example:
        ```yaml
        type: dagster_component_templates.LLMChainExecutorComponent
        attributes:
          asset_name: chain_output
          provider: openai
          model: gpt-4
          api_key: ${OPENAI_API_KEY}
          chain_steps: '[{"prompt": "Summarize: {text}", "output_key": "summary"}, {"prompt": "Generate title for: {summary}", "output_key": "title"}]'
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    provider: str = Field(description="LLM provider")
    model: str = Field(description="Model name")
    chain_steps: str = Field(description="JSON array of chain steps")
    initial_context: Optional[str] = Field(default=None, description="Initial context JSON")
    temperature: float = Field(default=0.7, description="Temperature")
    api_key: Optional[str] = Field(default=None, description="API key (use ${VAR_NAME} for environment variable)")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        model = self.model
        chain_steps_str = self.chain_steps
        initial_context_str = self.initial_context
        temperature = self.temperature
        api_key = self.api_key
        description = self.description or f"Execute LLM chain with {provider}/{model}"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def llm_chain_executor_asset(ctx: AssetExecutionContext, **kwargs) -> Dict[str, Any]:
            """Execute LLM chain.

            Accepts context from upstream assets via IO manager.
            Compatible with: Any asset producing context data (dict, str, DataFrame).
            """

            chain_steps = json.loads(chain_steps_str)
            context_data = json.loads(initial_context_str) if initial_context_str else {}

            # Get context from upstream assets
            upstream_assets = {k: v for k, v in kwargs.items()}

            if upstream_assets:
                ctx.log.info(f"Received {len(upstream_assets)} upstream asset(s)")

                for key, value in upstream_assets.items():
                    if isinstance(value, dict):
                        context_data.update(value)
                        ctx.log.info(f"Merged context from '{key}' (dict)")
                    elif isinstance(value, str):
                        context_data['input'] = value
                        ctx.log.info(f"Using text from '{key}' as 'input'")
                    else:
                        context_data[key] = str(value)
                        ctx.log.info(f"Added '{key}' to context")

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    var_name = api_key.strip('${}')
                    ctx.log.error(f"Environment variable not set: {var_name}")
                    raise ValueError(f"Environment variable not set: {var_name}")

            ctx.log.info(f"Executing chain with {len(chain_steps)} steps")

            for i, step in enumerate(chain_steps):
                prompt_template = step['prompt']
                output_key = step.get('output_key', f'output_{i}')

                # Format prompt with context
                prompt = prompt_template.format(**context_data)

                ctx.log.info(f"Step {i+1}: {output_key}")

                # Call LLM
                if provider == "openai":
                    import openai
                    client = openai.OpenAI(api_key=expanded_api_key)
                    response = client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                        temperature=temperature
                    )
                    output = response.choices[0].message.content
                elif provider == "anthropic":
                    import anthropic
                    client = anthropic.Anthropic(api_key=expanded_api_key)
                    message = client.messages.create(
                        model=model,
                        max_tokens=4096,
                        temperature=temperature,
                        messages=[{"role": "user", "content": prompt}]
                    )
                    output = message.content[0].text
                else:
                    raise ValueError(f"Unsupported provider: {provider}")

                context_data[output_key] = output

            ctx.add_output_metadata({"num_steps": len(chain_steps), "final_keys": list(context_data.keys())})

            return context_data

        return Definitions(assets=[llm_chain_executor_asset])
