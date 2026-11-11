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


named_job = dg.define_asset_job(name="named_job", selection=dg.AssetSelection.assets(a, b))

defs = dg.Definitions(assets=[a, b, boom], jobs=[named_job])
