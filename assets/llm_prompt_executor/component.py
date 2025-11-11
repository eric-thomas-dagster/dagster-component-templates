"""LLM Prompt Executor Asset Component.

Execute prompts against LLM APIs (OpenAI, Anthropic, etc.) and materialize responses as Dagster assets.
Supports multiple LLM providers, temperature control, and various response formats.
"""

import json
import os
from typing import Optional, Dict, Any
from enum import Enum

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


class LLMProvider(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    COHERE = "cohere"
    HUGGINGFACE = "huggingface"


class LLMPromptExecutorComponent(Component, Model, Resolvable):
    """Component for executing prompts against LLM APIs.

    This component receives prompts from upstream assets via IO managers and executes them
    against various LLM providers. The prompt should be passed from upstream assets using
    Dagster's IO manager pattern, not hardcoded in the configuration.

    Expected upstream data format:
        - String containing the prompt text
        - Dict with 'prompt' or 'text' key
        - Any string-like structure containing prompt data

    Example:
        ```yaml
        type: dagster_component_templates.LLMPromptExecutorComponent
        attributes:
          asset_name: product_description
          provider: openai
          model: gpt-4
          temperature: 0.7
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    provider: str = Field(
        description="LLM provider: 'openai', 'anthropic', 'cohere', or 'huggingface'"
    )

    model: str = Field(
        description="Model name (e.g., 'gpt-4', 'claude-3-5-sonnet-20241022', 'command-r-plus')"
    )

    system_prompt: Optional[str] = Field(
        default=None,
        description="System prompt to set context for the LLM"
    )

    temperature: float = Field(
        default=0.7,
        description="Temperature for response randomness (0.0-2.0)"
    )

    max_tokens: Optional[int] = Field(
        default=None,
        description="Maximum tokens in the response"
    )

    api_key: Optional[str] = Field(
        default=None,
        description="API key (use ${VAR_NAME} for environment variable, e.g., ${OPENAI_API_KEY})"
    )

    response_format: str = Field(
        default="text",
        description="Response format: 'text', 'json', or 'markdown'"
    )

    json_schema: Optional[str] = Field(
        default=None,
        description="JSON schema for structured output (JSON string)"
    )

    streaming: bool = Field(
        default=False,
        description="Whether to use streaming responses"
    )

    cache_responses: bool = Field(
        default=False,
        description="Whether to cache LLM responses"
    )

    cache_path: Optional[str] = Field(
        default=None,
        description="Path to cache file"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        provider = self.provider
        model = self.model
        system_prompt = self.system_prompt
        temperature = self.temperature
        max_tokens = self.max_tokens
        api_key = self.api_key
        response_format = self.response_format
        json_schema_str = self.json_schema
        streaming = self.streaming
        cache_responses = self.cache_responses
        cache_path = self.cache_path
        description = self.description or f"Execute LLM prompt using {provider}/{model}"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def llm_prompt_asset(context: AssetExecutionContext, **kwargs):
            """Asset that executes an LLM prompt.

            Receives prompt from upstream assets via IO managers. The upstream asset
            should return prompt text that will be passed through kwargs.

            Expected formats:
                - String: 'Your prompt text here'
                - Dict: {'prompt': 'text'} or {'text': 'text'}
                - Any string or dict containing prompt data
            """

            # Extract prompt from upstream assets via kwargs
            prompt = None

            # Look through kwargs for upstream asset data
            for key, value in kwargs.items():
                context.log.debug(f"Received upstream input from {key}: {type(value)}")

                if isinstance(value, str):
                    # Direct string input
                    prompt = value
                    break
                elif isinstance(value, dict):
                    # Check for common prompt keys
                    if 'prompt' in value:
                        prompt = value['prompt']
                        break
                    elif 'text' in value:
                        prompt = value['text']
                        break
                    # If it's just a dict, try to convert to string
                    elif len(value) > 0:
                        prompt = str(value)
                        break

            if not prompt:
                raise ValueError(
                    "No prompt received from upstream assets. "
                    "Ensure an upstream asset provides prompt data via IO manager."
                )

            context.log.info(f"Received prompt: {prompt[:100]}...")

            # Expand environment variables in prompt if needed
            prompt = os.path.expandvars(prompt)

            # Expand environment variables in API key (supports ${VAR_NAME} pattern)
            expanded_api_key = None
            if api_key:
                expanded_api_key = os.path.expandvars(api_key)
                if expanded_api_key == api_key and api_key.startswith('${'):
                    # Variable wasn't expanded, environment variable not set
                    var_name = api_key.strip('${}')
                    context.log.error(f"Environment variable not set: {var_name}")
                    raise ValueError(f"Environment variable not set: {var_name}")

            # Parse JSON schema if provided
            schema_dict = None
            if json_schema_str:
                try:
                    schema_dict = json.loads(json_schema_str)
                except json.JSONDecodeError as e:
                    context.log.error(f"Invalid JSON schema: {e}")
                    raise

            context.log.info(f"Executing prompt with {provider}/{model}")
            context.log.info(f"Temperature: {temperature}")

            response_text = None

            # Execute based on provider
            if provider == "openai":
                try:
                    import openai
                    client = openai.OpenAI(api_key=expanded_api_key)

                    messages = []
                    if system_prompt:
                        messages.append({"role": "system", "content": system_prompt})
                    messages.append({"role": "user", "content": prompt})

                    kwargs = {
                        "model": model,
                        "messages": messages,
                        "temperature": temperature,
                    }

                    if max_tokens:
                        kwargs["max_tokens"] = max_tokens

                    if response_format == "json" and schema_dict:
                        kwargs["response_format"] = {
                            "type": "json_schema",
                            "json_schema": schema_dict
                        }
                    elif response_format == "json":
                        kwargs["response_format"] = {"type": "json_object"}

                    if streaming:
                        stream = client.chat.completions.create(stream=True, **kwargs)
                        chunks = []
                        for chunk in stream:
                            if chunk.choices[0].delta.content:
                                chunks.append(chunk.choices[0].delta.content)
                        response_text = "".join(chunks)
                    else:
                        response = client.chat.completions.create(**kwargs)
                        response_text = response.choices[0].message.content

                except ImportError:
                    raise ImportError("OpenAI package not installed. Install with: pip install openai")
                except Exception as e:
                    context.log.error(f"OpenAI API error: {e}")
                    raise

            elif provider == "anthropic":
                try:
                    import anthropic
                    client = anthropic.Anthropic(api_key=expanded_api_key)

                    kwargs = {
                        "model": model,
                        "max_tokens": max_tokens or 4096,
                        "temperature": temperature,
                        "messages": [{"role": "user", "content": prompt}]
                    }

                    if system_prompt:
                        kwargs["system"] = system_prompt

                    if streaming:
                        chunks = []
                        with client.messages.stream(**kwargs) as stream:
                            for text in stream.text_stream:
                                chunks.append(text)
                        response_text = "".join(chunks)
                    else:
                        message = client.messages.create(**kwargs)
                        response_text = message.content[0].text

                except ImportError:
                    raise ImportError("Anthropic package not installed. Install with: pip install anthropic")
                except Exception as e:
                    context.log.error(f"Anthropic API error: {e}")
                    raise

            elif provider == "cohere":
                try:
                    import cohere
                    client = cohere.Client(api_key=expanded_api_key)

                    kwargs = {
                        "model": model,
                        "message": prompt,
                        "temperature": temperature,
                    }

                    if max_tokens:
                        kwargs["max_tokens"] = max_tokens

                    if system_prompt:
                        kwargs["preamble"] = system_prompt

                    if streaming:
                        chunks = []
                        for event in client.chat_stream(**kwargs):
                            if hasattr(event, 'text'):
                                chunks.append(event.text)
                        response_text = "".join(chunks)
                    else:
                        response = client.chat(**kwargs)
                        response_text = response.text

                except ImportError:
                    raise ImportError("Cohere package not installed. Install with: pip install cohere")
                except Exception as e:
                    context.log.error(f"Cohere API error: {e}")
                    raise

            elif provider == "huggingface":
                try:
                    from huggingface_hub import InferenceClient
                    client = InferenceClient(token=expanded_api_key)

                    messages = []
                    if system_prompt:
                        messages.append({"role": "system", "content": system_prompt})
                    messages.append({"role": "user", "content": prompt})

                    kwargs = {
                        "model": model,
                        "messages": messages,
                        "temperature": temperature,
                    }

                    if max_tokens:
                        kwargs["max_tokens"] = max_tokens

                    if streaming:
                        chunks = []
                        for message in client.chat_completion(stream=True, **kwargs):
                            if message.choices[0].delta.content:
                                chunks.append(message.choices[0].delta.content)
                        response_text = "".join(chunks)
                    else:
                        response = client.chat_completion(**kwargs)
                        response_text = response.choices[0].message.content

                except ImportError:
                    raise ImportError("Hugging Face package not installed. Install with: pip install huggingface_hub")
                except Exception as e:
                    context.log.error(f"Hugging Face API error: {e}")
                    raise

            else:
                raise ValueError(f"Unsupported provider: {provider}")

            context.log.info(f"Successfully generated response ({len(response_text)} characters)")

            # Format response
            result = response_text

            if response_format == "json":
                try:
                    result = json.loads(response_text)
                except json.JSONDecodeError:
                    context.log.warning("Response is not valid JSON, returning as text")
                    result = response_text

            # Cache if requested
            if cache_responses and cache_path:
                context.log.info(f"Caching response to {cache_path}")
                with open(cache_path, 'w') as f:
                    if isinstance(result, dict):
                        json.dump(result, f, indent=2)
                    else:
                        f.write(str(result))

            # Add metadata
            metadata = {
                "provider": provider,
                "model": model,
                "temperature": temperature,
                "response_length": len(response_text),
                "streaming": streaming,
            }

            if max_tokens:
                metadata["max_tokens"] = max_tokens

            context.add_output_metadata(metadata)

            return result

        return Definitions(assets=[llm_prompt_asset])
