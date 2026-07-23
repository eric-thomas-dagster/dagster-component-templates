.PHONY: help test ruff pyright check fix validate-manifest infer-produces regen-readme-fields

help:
	@echo "Available targets:"
	@echo "  make test              — run pytest (subset that doesn't need optional SDKs)"
	@echo "  make ruff              — run ruff lint on the shipped package + tools"
	@echo "  make pyright           — run pyright type check on the shipped package + tools"
	@echo "  make check             — run ruff + pyright (matches upstream community-integrations 'make check')"
	@echo "  make fix               — auto-fix ruff issues"
	@echo "  make validate-manifest — L1 validation: import every component + call build_defs"
	@echo "  make infer-produces    — regenerate the 'produces' metadata on manifest entries"
	@echo "  make regen-readme-fields — sync each component README's Fields section from its Field() declarations"

test:
	@if [ -d tests ]; then \
	  uv run pytest tests/; \
	else \
	  echo "no tests/ dir yet — using validate_manifest.py as the L1 test suite"; \
	  $(MAKE) validate-manifest; \
	fi

ruff:
	uvx ruff check dagster_community_components tools

pyright:
	uvx pyright dagster_community_components tools

check: ruff pyright

fix:
	uvx ruff check --fix dagster_community_components tools
	uvx ruff format dagster_community_components tools

validate-manifest:
	uv run python tools/validate_manifest.py

infer-produces:
	uv run python tools/infer_produces.py

regen-readme-fields:
	uv run python tools/regen_readme_fields.py
