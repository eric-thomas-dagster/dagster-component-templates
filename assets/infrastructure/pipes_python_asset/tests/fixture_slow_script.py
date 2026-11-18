"""A real dagster-pipes-aware script that sleeps before reporting -- used to test
timeout_seconds against a real subprocess, not a mock."""
import time

from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as pipes:
    time.sleep(1)
    pipes.report_asset_materialization(metadata={"done": True})
