# tools/

Helper scripts used to build the `dagster-community-components` PyPI package.

## `generate_init.py`

Regenerates `dagster_community_components/__init__.py` by scanning every
`component.py` referenced in `manifest.json`, parsing it via AST to find
public `*Component` / `*Asset` / `*Resource` classes (or any class inheriting
from `Component` / `Resolvable` / `StateBackedComponent`), and writing a
lazy-loading map of class name → component file path.

Re-run this whenever:
- You add a new component to the registry
- You rename a class
- You change the structure of a component file

```bash
python tools/generate_init.py
```

The output is deterministic — re-runs only produce a diff if something
actually changed.

## Releasing a new version of `dagster-community-components`

```bash
# 1. Regenerate the lazy-import index (so new components are picked up)
python tools/generate_init.py

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
