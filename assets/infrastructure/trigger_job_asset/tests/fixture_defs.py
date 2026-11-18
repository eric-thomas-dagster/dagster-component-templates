import dagster as dg


@dg.asset
def a():
    return 1


@dg.asset(deps=[a])
def b():
    return 2


@dg.asset
def boom():
    raise ValueError("deliberate failure for a test")


@dg.asset
def slow():
    import time
    time.sleep(1)
    return 1


named_job = dg.define_asset_job(name="named_job", selection=dg.AssetSelection.assets(a, b))
slow_job = dg.define_asset_job(name="slow_job", selection=dg.AssetSelection.assets(slow))

defs = dg.Definitions(assets=[a, b, boom, slow], jobs=[named_job, slow_job])
