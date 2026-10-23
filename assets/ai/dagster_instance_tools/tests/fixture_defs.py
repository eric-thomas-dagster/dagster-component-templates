import dagster as dg

PART = dg.StaticPartitionsDefinition(["a", "b", "c"])


@dg.asset
def ok():
    return 1


@dg.asset(deps=[ok])
def bad(context):
    context.log.info("calling api with Authorization: Bearer sk-live-FAKEFAKEFAKEFAKE1234567890")
    context.log.info("connecting postgres://svc:hunter2pass@db.internal/prod")
    context.log.warning("retrying; api_key=AKIAABCDEFGHIJKLMNOP and password: 'p@ssw0rd!'")
    raise ValueError("upstream API rejected request: HTTP 401 token=abc123SECRETVALUE\nsecond line detail")


@dg.asset(partitions_def=PART)
def part(context):
    if context.partition_key == "b":
        raise RuntimeError("partition b exploded")
    return 1


@dg.asset(deps=[ok], check_specs=[dg.AssetCheckSpec("row_count_positive", asset="chk")], key="chk")
def chk():
    yield dg.MaterializeResult(check_results=[dg.AssetCheckResult(check_name="row_count_positive", passed=False, description="0 rows")])


defs = dg.Definitions(assets=[ok, bad, part, chk])
