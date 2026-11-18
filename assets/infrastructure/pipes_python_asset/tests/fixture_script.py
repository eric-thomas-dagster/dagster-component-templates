"""A real dagster-pipes-aware script, used as the `script_path` target in tests -- the same
convention this repo's other Pipes components' fixture scripts use."""
import sys

from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as pipes:
    pipes.log.info(f"fixture_script running under {sys.executable}")
    pipes.report_asset_materialization(metadata={
        "args": sys.argv[1:],
        "extras_echo": pipes.extras.get("echo"),
        "python_path": sys.executable,
    })
