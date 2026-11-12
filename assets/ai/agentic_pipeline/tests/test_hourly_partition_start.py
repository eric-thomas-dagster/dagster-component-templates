"""partition_type='hourly' used to pass partition_start straight through to Dagster's HourlyPartitionsDefinition,
which requires 'YYYY-MM-DD-HH:MM' and otherwise raised a raw strptime ValueError from deep inside Dagster -- the
component's own field description already warned about this; now a bare 'YYYY-MM-DD' is accepted (treated as
midnight) and anything else raises a clear, component-level error naming the expected format."""
import dagster as dg
import pytest

SUMM = {"id": "summ", "op": "llm_call", "model": "gpt-4o-mini", "api_key_env_var": "OPENAI_API_KEY", "prompt_template": "{text}"}


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "k")


def comp(mod, **kw):
    return mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi {partition_key}"}, steps=[dict(SUMM)],
        outputs={"assets": ["summ"]}, partition_type="hourly", **kw,
    )


def test_bare_date_is_treated_as_midnight(mod):
    defs = comp(mod, partition_start="2026-01-01").build_defs(None)
    pdef = next(iter(defs.assets)).partitions_def
    assert isinstance(pdef, dg.HourlyPartitionsDefinition)
    first_key = pdef.get_partition_keys()[0]
    assert first_key == "2026-01-01-00:00"


def test_full_hourly_format_still_works(mod):
    defs = comp(mod, partition_start="2026-01-01-06:00").build_defs(None)
    pdef = next(iter(defs.assets)).partitions_def
    first_key = pdef.get_partition_keys()[0]
    assert first_key == "2026-01-01-06:00"


def test_an_unparseable_start_raises_a_clear_component_level_error(mod):
    with pytest.raises(ValueError, match="requires partition_start in 'YYYY-MM-DD-HH:MM' format"):
        comp(mod, partition_start="not-a-date").build_defs(None)


def test_bare_date_also_works_on_a_partition_dimensions_axis(mod):
    comp2 = mod.AgenticPipelineComponent(
        asset_name_prefix="p", source={"kind": "literal", "text": "hi {partition_key}"}, steps=[dict(SUMM)],
        outputs={"assets": ["summ"]},
        partition_dimensions=[{"name": "h", "type": "hourly", "start": "2026-01-01"}, {"name": "region", "type": "static", "values": ["us", "eu"]}],
    )
    defs = comp2.build_defs(None)
    pdef = next(iter(defs.assets)).partitions_def
    assert isinstance(pdef, dg.MultiPartitionsDefinition)
