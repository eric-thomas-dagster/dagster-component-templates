# Conversation Memory Asset

Manage conversation history and context for multi-turn LLM interactions using Dagster's IO manager pattern. Accepts messages from upstream assets and maintains conversation state - just connect message-producing assets!

## Overview

This component manages conversation history automatically passed from upstream assets via IO managers. Simply draw connections in the visual editor - no configuration needed for dependencies!

**Compatible with:**
- User Input Components (output: dict with user_message)
- LLM Prompt Executor (output: string for assistant messages)
- Message Queue Consumers (output: dict with messages)
- Any component that outputs message data

Perfect for:
- Chatbot conversation tracking
- Multi-turn AI interactions
- Conversation state management
- Context-aware LLM applications

## Features

- **Visual Dependencies**: Draw connections, no manual configuration
- **JSON Storage**: Simple file-based storage
- **Automatic Trimming**: Keep only recent messages
- **System Prompt**: Persistent system instructions
- **Message History**: Full conversation context
- **Easy Integration**: Works with any LLM component

## Input and Output

### Input Requirements

This component accepts messages from upstream assets via IO managers. The input can be:
- **Dict with 'user_message' key**: `{"user_message": "Hello!"}`
- **Dict with 'assistant_message' key**: `{"assistant_message": "Hi there!"}`
- **Dict with 'messages' list**: `{"messages": [{"role": "user", "content": "Hello"}]}`
- **String**: Treated as user message

The component appends new messages to the conversation history.

### Output Format

The component outputs a dictionary with complete conversation memory:
```python
{
  "messages": [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Hello!"},
    {"role": "assistant", "content": "Hi there!"}
  ]
}
```

## Configuration

### Required
- **asset_name** (string) - Name of the asset
- **memory_file** (string) - Path to memory JSON file (e.g., `./conversation.json`)

### Optional
- **max_messages** (integer) - Max messages to keep (default: `10`)
- **include_system_prompt** (boolean) - Include system prompt in memory (default: `true`)
- **system_prompt** (string) - System prompt text (default: `"You are a helpful assistant."`)
- **description** (string) - Asset description
- **group_name** (string) - Asset group

## Examples

### Basic Usage
```yaml
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: chat_memory
  memory_file: ./conversation.json
  user_message: "Hello, how are you?"
  max_messages: 10
```

### With Custom System Prompt
```yaml
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: support_memory
  memory_file: ./support_chat.json
  system_prompt: "You are a helpful customer support assistant."
  max_messages: 20
```

## Example Pipelines with IO Manager Pattern

### Pipeline 1: Simple Chatbot

```yaml
# Step 1: Capture user input
type: dagster_component_templates.UserInputComponent
attributes:
  asset_name: user_input
  input_type: text

# Step 2: Update conversation memory (automatically receives user_input)
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: conversation_state
  memory_file: ./chat_memory.json
  max_messages: 10
  system_prompt: "You are a helpful assistant."

# Step 3: Generate LLM response using conversation history
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: assistant_response
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}

# Step 4: Update memory with assistant response
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: updated_conversation
  memory_file: ./chat_memory.json
  max_messages: 10
```

**Visual Connections:**
```
user_input → conversation_state → assistant_response → updated_conversation
```

### Pipeline 2: Customer Support Bot

```yaml
# Step 1: Receive customer message from API
type: dagster_component_templates.RestApiFetcherComponent
attributes:
  asset_name: customer_message
  api_url: https://api.example.com/messages/latest
  output_format: dict

# Step 2: Load conversation memory
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: support_history
  memory_file: ./support/customer_${CUSTOMER_ID}.json
  max_messages: 20
  system_prompt: "You are a knowledgeable customer support agent. Be helpful and professional."

# Step 3: Generate contextual response
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: support_response
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  api_key: ${ANTHROPIC_API_KEY}
  temperature: 0.7
```

**Visual Connections:**
```
customer_message → support_history → support_response
```

### Pipeline 3: Multi-Turn Q&A System

```yaml
# Step 1: Read question from file
type: dagster_component_templates.TextFileReaderComponent
attributes:
  asset_name: user_question
  file_path: /data/questions/latest.txt

# Step 2: Update conversation with question
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: qa_memory
  memory_file: ./qa_session.json
  max_messages: 15
  system_prompt: "You are an expert Q&A assistant. Provide detailed, accurate answers."

# Step 3: Generate answer using LLM chain
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: detailed_answer
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  chain_steps: '[
    {"prompt": "Analyze the question: {user_question}", "output_key": "analysis"},
    {"prompt": "Provide a comprehensive answer based on: {analysis}", "output_key": "answer"},
    {"prompt": "Suggest 3 follow-up questions based on: {answer}", "output_key": "follow_ups"}
  ]'
```

**Visual Connections:**
```
user_question → qa_memory → detailed_answer
```

### Pipeline 4: Conversation with Memory Persistence

```yaml
# Step 1: Get user input from database
type: dagster_component_templates.DatabaseQueryComponent
attributes:
  asset_name: latest_user_message
  query: "SELECT message_text, user_id FROM messages ORDER BY created_at DESC LIMIT 1"
  connection_string: ${DATABASE_URL}

# Step 2: Load user-specific conversation
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: user_conversation
  memory_file: ./conversations/user_${user_id}_memory.json
  max_messages: 25
  system_prompt: "You are a personalized AI assistant. Remember context from previous conversations."

# Step 3: Generate personalized response
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: personalized_response
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  temperature: 0.8

# Step 4: Save response to database
type: dagster_component_templates.DatabaseInsertComponent
attributes:
  asset_name: saved_response
  table_name: messages
  connection_string: ${DATABASE_URL}
```

**Visual Connections:**
```
latest_user_message → user_conversation → personalized_response → saved_response
```

### Pipeline 5: Streaming Conversation with Context

```yaml
# Step 1: Stream messages from queue
type: dagster_component_templates.MessageQueueConsumerComponent
attributes:
  asset_name: incoming_messages
  queue_url: ${QUEUE_URL}
  message_format: json

# Step 2: Maintain conversation state
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: streaming_memory
  memory_file: ./stream_conversation.json
  max_messages: 30
  system_prompt: "You are a real-time conversation assistant."

# Step 3: Generate streaming response
type: dagster_component_templates.LLMPromptExecutorComponent
attributes:
  asset_name: streaming_response
  provider: openai
  model: gpt-4
  api_key: ${OPENAI_API_KEY}
  streaming: true
  temperature: 0.7
```

**Visual Connections:**
```
incoming_messages → streaming_memory → streaming_response
```

### Pipeline 6: Context-Aware Document Analysis

```yaml
# Step 1: Extract document text
type: dagster_component_templates.DocumentTextExtractorComponent
attributes:
  asset_name: document_content
  file_path: /data/docs/report.pdf

# Step 2: Add to conversation as context
type: dagster_component_templates.ConversationMemoryComponent
attributes:
  asset_name: analysis_context
  memory_file: ./doc_analysis_memory.json
  max_messages: 5
  system_prompt: "You are analyzing a document. Remember previous questions and answers about this document."

# Step 3: Run analysis chain with memory
type: dagster_component_templates.LLMChainExecutorComponent
attributes:
  asset_name: document_insights
  provider: anthropic
  model: claude-3-5-sonnet-20241022
  api_key: ${ANTHROPIC_API_KEY}
  chain_steps: '[
    {"prompt": "What is the main topic of this document? {document_content}", "output_key": "topic"},
    {"prompt": "What are the key findings related to {topic}?", "output_key": "findings"},
    {"prompt": "Based on the conversation history, what new questions should we explore?", "output_key": "questions"}
  ]'
```

**Visual Connections:**
```
document_content → analysis_context → document_insights
```

## Memory Format

The conversation memory is stored as JSON:
```json
{
  "messages": [
    {"role": "system", "content": "You are a helpful assistant."},
    {"role": "user", "content": "Hello!"},
    {"role": "assistant", "content": "Hi! How can I help?"},
    {"role": "user", "content": "What's the weather?"},
    {"role": "assistant", "content": "I don't have real-time weather data..."}
  ]
}
```

## Message Trimming

When the number of messages exceeds `max_messages`, the oldest user/assistant pairs are removed, but the system prompt is always preserved.

## Use with LLM Components

The conversation memory output can be passed directly to LLM components, which will use the message history to maintain context across turns.

## Requirements
None (uses Python standard library)

## License
MIT License
