# Part of Speech Tagger Component

Tag parts of speech in text using spaCy. Annotates each row with POS information in three configurable output modes.

## Overview

The Part of Speech Tagger Component uses spaCy to assign grammatical roles (noun, verb, adjective, etc.) to every token in a text column. Choose between adding a single tags column, expanding to one row per token, or aggregating POS counts per row.

## Features

- **Three Output Modes**: `tags_column`, `expanded`, `counts`
- **Token Text Pairing**: Return `(token, POS)` tuples or just POS tags
- **Dependency Relations**: `expanded` mode includes syntactic dependency labels
- **Multi-language**: Any spaCy-compatible language model
- **Null-safe**: Handles missing text gracefully

## Use Cases

1. **Feature Engineering**: POS counts as ML features
2. **Grammar Analysis**: Understand linguistic structure
3. **NLP Preprocessing**: Filter by word type before further processing
4. **Content Analysis**: Compare writing styles across documents

## Prerequisites

- `spacy>=3.0.0`, `pandas>=1.5.0`
- A downloaded spaCy model, e.g. `python -m spacy download en_core_web_sm`

## Configuration

### tags_column mode (default)

```yaml
type: dagster_component_templates.PartOfSpeechTaggerComponent
attributes:
  asset_name: pos_tagged_text
  upstream_asset_key: raw_text
  text_column: body
  output_mode: tags_column
  output_column: pos_tags
  include_token_text: true
```

Output adds a `pos_tags` column with values like `[("The", "DET"), ("dog", "NOUN"), ...]`.

### expanded mode

```yaml
type: dagster_component_templates.PartOfSpeechTaggerComponent
attributes:
  asset_name: tokens_expanded
  upstream_asset_key: raw_text
  text_column: body
  output_mode: expanded
```

Output: one row per token with `token`, `pos_tag`, `dep` columns added.

### counts mode

```yaml
type: dagster_component_templates.PartOfSpeechTaggerComponent
attributes:
  asset_name: pos_counts
  upstream_asset_key: raw_text
  text_column: body
  output_mode: counts
```

Output adds columns like `pos_NOUN`, `pos_VERB`, `pos_ADJ`, etc. with counts per row.

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `text_column` | string | required | Column with text |
| `output_mode` | string | `tags_column` | Output mode |
| `output_column` | string | `pos_tags` | Column name for tags_column mode |
| `language` | string | `en` | spaCy language model |
| `include_token_text` | bool | `true` | Include token text with POS tag |
| `group_name` | string | `None` | Asset group |

## spaCy POS Tags Reference

| Tag | Meaning |
|-----|---------|
| `NOUN` | Noun |
| `VERB` | Verb |
| `ADJ` | Adjective |
| `ADV` | Adverb |
| `DET` | Determiner |
| `PRON` | Pronoun |
| `PROPN` | Proper Noun |
| `PUNCT` | Punctuation |
| `NUM` | Numeral |

## Troubleshooting

- **OSError (model not found)**: Run `python -m spacy download en_core_web_sm`
- **ImportError**: Run `pip install spacy`
- **Slow performance**: Use a smaller spaCy model (`en_core_web_sm` vs `en_core_web_trf`)
