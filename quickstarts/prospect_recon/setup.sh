#!/usr/bin/env bash
# Fire up Prospect Recon locally. Unlike the three context_engineering_pipeline
# walkthroughs in dagster-community-components-cli, this project already lives
# in this repo -- there's nothing to scaffold from a remote zip. This script
# just does the mechanical setup (env file, deps) and stops before `dg dev` so
# you can fill in real credentials first; it never launches with an empty .env.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

if [ -f .env ]; then
  echo ".env already exists -- leaving it alone."
else
  cp .env.example .env
  echo "Created .env from .env.example."
fi

if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found -- install it first: https://docs.astral.sh/uv/getting-started/installation/" >&2
  exit 1
fi

uv sync

cat <<'EOF'

Next:
  1. Fill in real credentials in .env (see the comment on each var --
     GITHUB_TOKEN, GOOGLE_APPLICATION_CREDENTIALS, TAVILY_API_KEY, NOTION_TOKEN,
     OPENAI_API_KEY, SLACK_BOT_TOKEN -- and point PROSPECT_RECON_DEMO_FACTORY_REPO
     at a scratch/fork repo for your first few runs, since a real candidate
     triggers a real commit+PR+merge).
  2. set -a && source .env && set +a
  3. uv run dg dev

Every run makes real LLM calls and, if a candidate is found, a real commit
against PROSPECT_RECON_DEMO_FACTORY_REPO -- see README.md's cost/safety note.
EOF
