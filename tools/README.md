# tools/

Helper scripts used to build the `dagster-community-components` PyPI package.

## `check_component_registry.py`

Verifies every `component.py`'s `*Component` class is registered in
`dagster_community_components/__init__.py`'s `_CLASS_PATHS` dict -- the
actual discovery mechanism `dg list components` and the Dagster UI's
Components tab use (`manifest.json` is a separate, unrelated catalog).

Additive-only: `--fix` only ever appends missing entries, it never
touches the file's other hand-curated exports (decorator functions, bare
aliases, etc.).

Re-run this whenever:
- You add a new component to the registry
- You rename a class

```bash
python3 tools/check_component_registry.py          # report only
python3 tools/check_component_registry.py --fix    # append missing entries
```

## `migrate_from_ingestion_platform.py`

Audits an existing Fivetran or Airbyte account and shows, per connector,
both paths into this project: keep it orchestrated as-is via the
existing `fivetran_assets`/`airbyte_assets` components, or convert it to
a native dlt-based `*_ingestion` component where one already exists here.
Connectors with no native equivalent are collected into a gap report --
real, demand-driven signal for which vendor to build next.

```bash
export FIVETRAN_API_KEY=... FIVETRAN_API_SECRET=...
python3 tools/migrate_from_ingestion_platform.py --platform fivetran

export AIRBYTE_CLIENT_ID=... AIRBYTE_CLIENT_SECRET=...
python3 tools/migrate_from_ingestion_platform.py --platform airbyte
```

## Releasing a new version of `dagster-community-components`

```bash
# 1. Make sure the registry is up to date (see check_component_registry.py above)
python3 tools/check_component_registry.py --fix

# 2. Bump the version in BOTH places
#      pyproject.toml                          → [project] version = "0.2.0"
#      dagster_community_components/__init__.py → __version__ = "0.2.0"
#    (or run the bump script if/when one exists)

# 3. Build wheel + sdist
uv build

# 4. Upload to PyPI (one-time: configure ~/.pypirc with an API token)
uv publish
```

## Verifying a built wheel locally

```bash
uv build
uv venv /tmp/dcc-test
uv pip install --python /tmp/dcc-test/bin/python dist/dagster_community_components-*.whl
/tmp/dcc-test/bin/python -c "
from dagster_community_components import OneHotEncodingComponent
print(OneHotEncodingComponent)
"
```

A successful import should show `<class 'OneHotEncodingComponent'>`.
