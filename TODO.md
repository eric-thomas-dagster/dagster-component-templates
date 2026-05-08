# TODO

Open work tracked across the registry. Closed items get deleted, not crossed
out — git log is the history.

## Examples to write

Per the project pattern (`dagster-community-components-cli/examples/setup_*.sh`
+ `examples/<demo>.md`), every component should have at least one walkthrough
that materializes it end-to-end. The following changed components currently
have no example:

- **`moderation_scorer`** — needs a walkthrough that scores a batch of
  user-generated text and demonstrates the threshold/flag output. Likely
  pairs naturally with `synthetic_data_generator` (support_tickets schema)
  or a slim "user comments" synthetic source.
- **`text_moderator`** — same shape as `moderation_scorer`; could share an
  example (`setup_content_moderation_demo.sh`?) that runs both side-by-side
  and contrasts their output.
- **`ollama_inference_asset`** — needs an example that doesn't require
  OpenAI credentials. Should document the local Ollama install step
  (e.g. `brew install ollama && ollama pull llama3.2:3b`) and run a
  small inference batch. Validation level `live` requires Ollama running
  on the target machine.

## Partition shape rework

Tracked separately as Phase 1 of the partition-handling improvements.
See conversation context, not yet broken into sub-tasks here.
