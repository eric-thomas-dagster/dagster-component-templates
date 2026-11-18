"""Module-level target functions for DynamicFanoutAssetComponent tests -- referenced via
dotted 'module:function' strings, so they must be real, importable module-level functions."""
CALLS = {}


def reset():
    CALLS.clear()


def discover_static(**kwargs):
    CALLS["discover_kwargs"] = kwargs
    return [1, 2, 3]


def discover_from_upstream(upstream, **kwargs):
    CALLS["upstream"] = upstream
    return [x * 10 for x in upstream]


def double(item, **kwargs):
    CALLS.setdefault("processed", []).append(item)
    return item * 2


def total(results):
    CALLS["collected"] = list(results)
    return sum(results)
