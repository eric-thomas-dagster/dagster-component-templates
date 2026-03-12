# SSHAssetComponent

Run shell commands on a remote host via SSH as a first-class Dagster asset. Uses [`paramiko`](https://www.paramiko.org/) for pure-Python SSH connectivity — no system `ssh` binary required.

## Use cases

**Legacy systems** — Many organizations have production data sources running on bare-metal servers or older operating systems that are not containerized and cannot be directly integrated into a modern orchestrator. `SSHAssetComponent` lets you trigger extraction or transformation scripts on those machines and gate downstream Dagster assets on their success.

**HPC clusters** — High-performance computing environments (SLURM, PBS, LSF) are often accessed exclusively via SSH. Use this component to submit jobs, poll for completion, or stage input files before a compute-intensive pipeline step.

**On-premise servers** — Enterprise environments with on-premise infrastructure that sits behind a firewall can be reached over an SSH bastion. Configure the host, credentials, and the commands to run, and Dagster tracks every execution in the event log.

**Pre-pipeline configuration steps** — Some pipelines require a remote server to be in a known state before data jobs run: packages installed, config files deployed, processes restarted. Model those steps as assets with `deps` pointing to them so Dagster enforces the correct execution order.

## Prerequisites

Install `paramiko` in the environment that runs the Dagster worker:

```bash
pip install "paramiko>=3.0.0"
```

The remote host must be reachable from the worker over TCP on the configured `port` (default 22).

## Quick start

```yaml
type: dagster_component_templates.SSHAssetComponent
attributes:
  asset_name: prepare_etl_server
  host_env_var: ETL_HOST
  username_env_var: ETL_USER
  private_key_env_var: ETL_SSH_KEY
  commands:
    - "cd /opt/etl && git pull origin main"
    - "pip install -r requirements.txt"
    - "python scripts/prepare_data.py"
  group_name: infrastructure
  description: Prepare ETL server before pipeline run
```

Set the required environment variables in your Dagster deployment before materializing:

```bash
export ETL_HOST=10.0.1.42
export ETL_USER=deploy
export ETL_SSH_KEY="$(cat ~/.ssh/id_rsa)"
```

## Authentication options

### Private key content (recommended for secrets managers)

Store the full PEM content of your private key in a secret and expose it as an environment variable:

```yaml
private_key_env_var: MY_SSH_PRIVATE_KEY
```

The component reads the environment variable at runtime, parses the key in memory (RSA, Ed25519, and ECDSA are supported), and never writes the key to a predictable path. If the key format requires a temporary file, it is written with `0600` permissions and deleted immediately after the SSH session closes — even on failure.

### Private key file path

If the private key is already present on disk (e.g. mounted as a Kubernetes secret volume):

```yaml
private_key_path_env_var: SSH_KEY_PATH
# export SSH_KEY_PATH=/run/secrets/id_rsa
```

When both `private_key_path_env_var` and `private_key_env_var` are set, the file path takes precedence.

### Password (fallback)

For hosts that only support password authentication:

```yaml
password_env_var: SSH_PASSWORD
```

Password authentication is less secure than key-based auth. Use it only when key-based options are not available.

## Running commands

`commands` is an ordered list of shell commands executed sequentially on the remote host. Each command opens a new channel, so environment variables set in one command are not inherited by the next. Use `working_dir` to establish a consistent starting directory:

```yaml
working_dir: /opt/etl
commands:
  - "git pull origin main"
  - "pip install -r requirements.txt"
  - "python scripts/prepare_data.py --date={{ run_date }}"
```

The component prepends `cd <working_dir> && ` to each command automatically.

### Failure handling

By default (`fail_on_non_zero: true`) the asset raises immediately when any command returns a non-zero exit code, marking the Dagster run as failed. Disable this to collect all exit codes and decide downstream:

```yaml
fail_on_non_zero: false
```

Exit codes for all commands are included in the asset's materialization metadata so you can inspect them in the Dagster UI.

### Timeout

Each command is bounded by `timeout_seconds` (default 300). Long-running HPC jobs may need a higher value:

```yaml
timeout_seconds: 3600
```

## SFTP file transfer

### Upload a config file before commands run

```yaml
sftp_upload:
  local_path: /tmp/job_config.json
  remote_path: /opt/etl/config/job_config.json
```

The upload happens before any command in `commands` is executed, so commands can reference the file immediately.

### Download results after commands complete

```yaml
sftp_download:
  remote_path: /opt/etl/output/summary.csv
  local_path: /tmp/etl_summary.csv
```

The download happens after all commands succeed (or after the last command when `fail_on_non_zero: false`). Both upload and download can be used together in a single component instance.

## Streaming logs to the Dagster UI

With `capture_output: true` (default), every line of stdout from each command is forwarded to the Dagster run log as an `INFO` event in real time. This means you can monitor long-running remote scripts from the Dagster UI without SSH-ing into the box yourself. stderr lines are forwarded as `WARNING` events so they stand out.

Disable output capture for commands that produce very high volumes of output you do not need:

```yaml
capture_output: false
```

## known_hosts and automation

By default `known_hosts_check: false`, which applies `paramiko.AutoAddPolicy` — the SSH client accepts any host key without prompting. This is intentional for automated contexts (CI/CD, scheduled pipelines) where there is no interactive terminal to confirm a fingerprint, and where the host is considered trusted by network policy.

For security-sensitive environments where you want to enforce host key verification:

```yaml
known_hosts_check: true
```

The component then calls `client.load_system_host_keys()` and rejects connections to hosts whose key is not in the system `known_hosts` file. Ensure the remote host's fingerprint is pre-populated before the first run.

## Asset dependencies

Use `deps` to express ordering constraints with other Dagster assets:

```yaml
attributes:
  asset_name: prepare_etl_server
  deps:
    - server_inventory   # asset that confirms the server is online
  commands:
    - "python /opt/etl/prepare.py"
```

Dagster will refuse to materialize `prepare_etl_server` until `server_inventory` is up to date, and will refuse to materialize any downstream asset that depends on `prepare_etl_server` until it succeeds.

## Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `host_env_var` | `str` | required | Env var with hostname or IP |
| `username_env_var` | `str` | required | Env var with SSH username |
| `commands` | `list[str]` | required | Shell commands to run in order |
| `private_key_env_var` | `str` | `None` | Env var holding PEM key content |
| `private_key_path_env_var` | `str` | `None` | Env var holding path to key file |
| `password_env_var` | `str` | `None` | Env var holding SSH password |
| `port` | `int` | `22` | SSH port |
| `known_hosts_check` | `bool` | `false` | Enforce system known_hosts validation |
| `working_dir` | `str` | `None` | Remote directory prepended to each command |
| `timeout_seconds` | `int` | `300` | Per-command timeout |
| `fail_on_non_zero` | `bool` | `true` | Raise on non-zero exit code |
| `capture_output` | `bool` | `true` | Stream stdout to Dagster logs |
| `sftp_upload` | `dict` | `None` | `{local_path, remote_path}` upload before commands |
| `sftp_download` | `dict` | `None` | `{remote_path, local_path}` download after commands |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group |
| `description` | `str` | `None` | Asset description in Dagster UI |
| `deps` | `list[str]` | `None` | Upstream asset keys |
