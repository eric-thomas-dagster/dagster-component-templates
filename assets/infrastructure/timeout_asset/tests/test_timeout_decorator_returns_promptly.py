"""Real bug, found live while adding timeout_seconds elsewhere in this repo: @timeout used
`with ThreadPoolExecutor() as executor:`, whose __exit__ calls shutdown(wait=True) -- which
blocks until the background call finishes ANYWAY, even after future.result(timeout=...)
already raised. A 0.05s timeout against a 1s sleep took >1s to actually return (confirmed
live before fixing), silently defeating the timeout for a call that hangs indefinitely --
the exact scenario a timeout exists for. Fixed: shutdown(wait=False).
"""
import time

import dagster as dg

from .conftest import load_component_module


def test_timeout_decorator_returns_at_the_deadline_not_after_the_slow_call_finishes():
    mod = load_component_module()

    @dg.asset
    @mod.timeout(0.1)
    def slow(context):
        time.sleep(2)
        return 1

    start = time.monotonic()
    result = dg.materialize([slow], resources={"io_manager": dg.InMemoryIOManager()}, raise_on_error=False)
    elapsed = time.monotonic() - start

    assert not result.success
    assert elapsed < 1.5  # well under the 2s sleep -- confirms shutdown(wait=False) took effect
