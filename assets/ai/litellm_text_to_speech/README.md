# LiteLLM Text-to-Speech

Multi-provider TTS via LiteLLM. Same DataFrame-in → audio-files-out shape as `cloud_text_to_speech_asset`, but routes to OpenAI / ElevenLabs / Azure / etc.

```yaml
type: dagster_component_templates.LitellmTextToSpeechComponent
attributes:
  asset_name: announcements_audio
  upstream_asset_key: announcements
  text_column: text
  model: openai/tts-1
  voice: nova
  api_key_env_var: OPENAI_API_KEY
```

## Providers + voices

| Provider | Model | Voice ids | Notes |
|---|---|---|---|
| OpenAI | `openai/tts-1` | `alloy`, `echo`, `fable`, `onyx`, `nova`, `shimmer` | Cheapest at $15/1M chars. 6 voices. |
| OpenAI HD | `openai/tts-1-hd` | (same 6) | $30/1M chars. Slower. Cleaner pronunciation. |
| OpenAI | `openai/gpt-4o-mini-tts` | (newer) | Prompt-steerable: `voice="cheerful and concise"`. |
| ElevenLabs | `elevenlabs/eleven_multilingual_v2` | Your library + presets | Best quality, voice cloning. ~$0.18/1K chars. |
| ElevenLabs Turbo | `elevenlabs/eleven_turbo_v2_5` | (same library) | Lower latency, slightly lower quality. |
| Azure OpenAI | `azure/<deployment>` | OpenAI voices | When you have an Azure tenant. |

Set the matching `api_key_env_var` (`OPENAI_API_KEY`, `ELEVENLABS_API_KEY`, `AZURE_OPENAI_API_KEY`).

## Output

Adds 2 columns to the upstream DataFrame:
- `audio_path` — filesystem path to the generated audio file
- `tts_error` — error string per row (None on success)

## When to pick this vs. `cloud_text_to_speech_asset`

| Pattern | Use |
|---|---|
| You're on GCP, want WaveNet / Neural2 voices, want a single SA-auth path | `cloud_text_to_speech_asset` |
| You want OpenAI / ElevenLabs / multi-provider A/B | `litellm_text_to_speech` |
| Speech-to-speech translation chain in a GCP project | `cloud_text_to_speech_asset` (matches the rest of your GCP stack) |

## Cost

Wildly varies by provider:
- OpenAI tts-1: $15/1M chars
- OpenAI tts-1-hd: $30/1M chars
- ElevenLabs: $5/1M chars on the cheapest tier; >$100/1M on top tiers (price-per-tier varies)

## Sister components

- `cloud_text_to_speech_asset` — native Google TTS
- `litellm_audio_transcription` — opposite direction (audio → text)
- `litellm_image_generation` — same multi-provider pattern for images
