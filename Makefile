.PHONY: help test ruff pyright check fix validate-manifest infer-produces regen-readme-fields lint-sensors

help:
	@echo "Available targets:"
	@echo "  make test              — run pytest across every component's own tests/ dir (repo-wide, not just top-level tests/)"
	@echo "  make ruff              — run ruff lint on the shipped package + tools"
	@echo "  make pyright           — run pyright type check on the shipped package + tools"
	@echo "  make check             — run ruff + pyright + lint-sensors (matches upstream community-integrations 'make check')"
	@echo "  make lint-sensors      — fail if any sensor yields AssetMaterialization directly (silent-drop bug)"
	@echo "  make fix               — auto-fix ruff issues"
	@echo "  make validate-manifest — L1 validation: import every component + call build_defs"
	@echo "  make infer-produces    — regenerate the 'produces' metadata on manifest entries"
	@echo "  make regen-readme-fields — sync each component README's Fields section from its Field() declarations"

test:
	@if find . -type d -name tests \
	    -not -path '*/node_modules/*' -not -path '*/.git/*' \
	    -not -path '*/.venv/*' -not -path '*/venv/*' -not -path '*/site-packages/*' \
	    -print0 | grep -qz .; then \
	  find . -type d -name tests \
	    -not -path '*/node_modules/*' -not -path '*/.git/*' \
	    -not -path '*/.venv/*' -not -path '*/venv/*' -not -path '*/site-packages/*' \
	    -print0 | xargs -0 uv run pytest --continue-on-collection-errors; \
	else \
	  echo "no tests/ dirs yet — using validate_manifest.py as the L1 test suite"; \
	  $(MAKE) validate-manifest; \
	fi

ruff:
	uvx ruff check dagster_community_components tools

pyright:
	uvx pyright dagster_community_components tools

check: ruff pyright lint-sensors

lint-sensors:
	uv run python tools/lint_sensor_pattern.py

fix:
	uvx ruff check --fix dagster_community_components tools
	uvx ruff format dagster_community_components tools

validate-manifest:
	uv run python tools/validate_manifest.py

infer-produces:
	uv run python tools/infer_produces.py

regen-readme-fields:
	uv run python tools/regen_readme_fields.py
