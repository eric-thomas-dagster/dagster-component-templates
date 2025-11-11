"""Conversation Memory Asset Component.

Manage conversation history and context for multi-turn LLM interactions.
"""

import os
import json
from typing import Optional, List, Dict

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


class ConversationMemoryComponent(Component, Model, Resolvable):
    """Component for managing conversation memory.

    This component receives messages from upstream assets via IO managers and maintains
    conversation history in a local file. Messages should be passed from upstream assets
    using Dagster's IO manager pattern.

    Expected upstream data format:
        - Dict with 'user_message' and/or 'assistant_message' keys
        - Dict with 'messages' list containing message objects
        - Any dict-like structure containing message data

    Example:
        ```yaml
        type: dagster_component_templates.ConversationMemoryComponent
        attributes:
          asset_name: conversation_state
          memory_file: /path/to/memory.json
          max_messages: 10
        ```
    """

    asset_name: str = Field(description="Name of the asset")
    memory_file: str = Field(description="Path to memory file")
    max_messages: int = Field(default=10, description="Max messages to keep")
    include_system_prompt: bool = Field(default=True, description="Include system prompt")
    system_prompt: str = Field(default="You are a helpful assistant.", description="System prompt")
    description: Optional[str] = Field(default=None, description="Asset description")
    group_name: Optional[str] = Field(default=None, description="Asset group")

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        memory_file = self.memory_file
        max_messages = self.max_messages
        include_system_prompt = self.include_system_prompt
        system_prompt = self.system_prompt
        description = self.description or "Manage conversation memory"
        group_name = self.group_name

        @asset(
            name=asset_name,
            description=description,
            group_name=group_name,
        )
        def conversation_memory_asset(ctx: AssetExecutionContext, **kwargs):
            """Manage conversation memory.

            Receives messages from upstream assets via IO managers. The upstream asset
            should return message data that will be passed through kwargs.

            Expected formats:
                - {'user_message': 'text', 'assistant_message': 'text'}
                - {'messages': [{'role': 'user', 'content': 'text'}, ...]}
                - Any dict with message content
            """

            # Load existing memory
            if os.path.exists(memory_file):
                with open(memory_file, 'r') as f:
                    memory = json.load(f)
            else:
                memory = {"messages": []}

            # Add system prompt if needed
            if include_system_prompt and (not memory["messages"] or memory["messages"][0].get("role") != "system"):
                memory["messages"].insert(0, {"role": "system", "content": system_prompt})

            # Extract messages from upstream assets via kwargs
            user_message = None
            assistant_message = None

            # Look through kwargs for upstream asset data
            for key, value in kwargs.items():
                ctx.log.debug(f"Received upstream input from {key}: {type(value)}")

                if isinstance(value, dict):
                    # Check for direct message keys
                    if 'user_message' in value:
                        user_message = value['user_message']
                    if 'assistant_message' in value:
                        assistant_message = value['assistant_message']

                    # Check for messages list
                    if 'messages' in value and isinstance(value['messages'], list):
                        for msg in value['messages']:
                            if isinstance(msg, dict):
                                role = msg.get('role')
                                content = msg.get('content')
                                if role and content:
                                    if role == 'user':
                                        user_message = content
                                    elif role == 'assistant':
                                        assistant_message = content

            # Add new messages
            if user_message:
                memory["messages"].append({"role": "user", "content": user_message})
                ctx.log.info(f"Added user message: {user_message[:50]}...")

            if assistant_message:
                memory["messages"].append({"role": "assistant", "content": assistant_message})
                ctx.log.info(f"Added assistant message: {assistant_message[:50]}...")

            # Trim to max messages (keep system prompt)
            if len(memory["messages"]) > max_messages + 1:
                system_msg = memory["messages"][0] if memory["messages"][0].get("role") == "system" else None
                memory["messages"] = memory["messages"][-(max_messages):]
                if system_msg and memory["messages"][0].get("role") != "system":
                    memory["messages"].insert(0, system_msg)

            # Save memory
            os.makedirs(os.path.dirname(memory_file), exist_ok=True)
            with open(memory_file, 'w') as f:
                json.dump(memory, f, indent=2)

            ctx.log.info(f"Memory updated: {len(memory['messages'])} messages")

            ctx.add_output_metadata({"num_messages": len(memory['messages'])})

            return memory

        return Definitions(assets=[conversation_memory_asset])
