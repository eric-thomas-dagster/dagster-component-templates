"""Module-level target functions for RuntimePartitionAssetComponent tests -- referenced via
dotted 'module.path:function_name' strings, so they must be real, importable module-level
functions."""
CALLS = {}


def reset():
    CALLS.clear()


def choose_region():
    """key_source='callable_return': returns (key, payload)."""
    CALLS["called"] = True
    return "us-east", {"rows": 42}


def choose_region_bad_shape():
    return "not-a-tuple"


def plain_compute():
    """key_source in ('run_tag', 'run_config'): just returns the payload."""
    CALLS["called"] = True
    return {"rows": 7}


def wants_upstream(upstream_a):
    CALLS["upstream_a"] = upstream_a
    return f"region-{upstream_a}", {"rows": upstream_a}
