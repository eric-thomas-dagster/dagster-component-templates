# AnsibleAssetComponent

Run an Ansible playbook as a Dagster asset so that infrastructure provisioning and server configuration happen as a first-class, observable step inside your data pipeline.

## Use case

Dagster pipelines often depend on infrastructure being in a known state before data jobs run — packages installed, config files deployed, firewall rules applied, services restarted. `AnsibleAssetComponent` lets you:

- **Gate downstream assets** on a successful Ansible run via `deps`.
- **Stream playbook output** line-by-line to the Dagster event log so operators can see task-level progress in the UI without SSHing into a box.
- **Surface a PLAY RECAP summary** (ok / changed / unreachable / failed counts) as Dagster asset metadata, giving you a per-run audit trail.
- **Run on a schedule or sensor trigger**, integrating infrastructure state changes into the same observability surface as your data quality checks.

## Prerequisites

`ansible-playbook` must be installed in the environment that executes the Dagster run worker:

```bash
pip install ansible
# or
brew install ansible
```

Confirm it is on your PATH (or set `ansible_bin` to an absolute path).

## Quick start

```yaml
type: dagster_component_templates.AnsibleAssetComponent
attributes:
  asset_name: configure_data_servers
  playbook: "{{ project_root }}/ansible/configure_etl_servers.yml"
  inventory: "{{ project_root }}/ansible/inventory/production"
  extra_vars:
    env: production
    db_host: db.internal.company.com
  become: true
  group_name: infrastructure
  description: Configure ETL servers before the ETL pipeline runs
```

## Inventory formats

Ansible accepts several inventory formats via the `inventory` field:

| Format | Example |
|---|---|
| Static file | `/ansible/inventory/hosts` |
| Directory (multiple files) | `/ansible/inventory/production/` |
| Dynamic script | `/ansible/inventory/aws_ec2.py` |
| Inline host list | `"web1.example.com,web2.example.com,"` (note trailing comma) |
| localhost only | `"localhost,"` |

When `inventory` is omitted the component falls back to whatever is configured in `ansible.cfg` or `ANSIBLE_INVENTORY`.

## Passing variables

### Inline key/value pairs

```yaml
extra_vars:
  env: production
  deploy_user: ubuntu
```

This becomes `--extra-vars 'env=production deploy_user=ubuntu'`.

### Vars file

```yaml
extra_vars_file: "{{ project_root }}/ansible/vars/production.yml"
```

This becomes `--extra-vars @/path/to/production.yml`.

Both can be combined. Inline values take precedence over the file.

## Vault / secrets management

### Static password file

```yaml
vault_password_file: /run/secrets/vault_pass
```

### Password from an environment variable

```yaml
vault_password_env_var: ANSIBLE_VAULT_PASSWORD
```

The component reads the environment variable at runtime, writes its value to a
secure temporary file (mode `0600`), passes it as `--vault-password-file`, and
deletes the file immediately after the playbook exits — even on failure.

Set the secret in your Dagster deployment (e.g. Kubernetes secret, AWS Secrets
Manager via `EnvVar`):

```yaml
# dagster.yaml / instance config
env_vars:
  - ANSIBLE_VAULT_PASSWORD
```

## SSH key setup

If your SSH private key lives in a secret store rather than on disk:

```yaml
ssh_private_key_env_var: ANSIBLE_SSH_PRIVATE_KEY
```

The component writes the key to a temp file with `0600` permissions, sets
`ANSIBLE_PRIVATE_KEY_FILE` in the subprocess environment, and deletes the file
after the run. The key is never written to a predictable path.

## Dry runs with check_mode

Before applying changes in production, validate the playbook with:

```yaml
check_mode: true
diff_mode: true
```

`--check` prevents any changes; `--diff` shows what would change. Use this in a
separate asset (e.g. `configure_servers_dry_run`) to gate PRs or alert on
unexpected drift.

## Streaming logs to the Dagster UI

The component uses `subprocess.Popen` with line-by-line iteration so every line
of playbook output appears in the Dagster run log in real time. No polling
interval. Task-level `ok` / `changed` / `failed` lines are visible as they
happen, making it easy to diagnose failures without leaving the Dagster UI.

## Asset dependencies

Use `deps` to express that this asset must succeed before downstream data assets
run:

```yaml
attributes:
  asset_name: configure_data_servers
  deps:
    - raw_server_inventory   # another asset that produces the host list
  playbook: ...
```

Dagster will refuse to materialize `configure_data_servers` until
`raw_server_inventory` is up to date, and will refuse to materialize any asset
that depends on `configure_data_servers` until it succeeds.

## Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset key |
| `playbook` | `str` | required | Path to playbook YAML |
| `inventory` | `str` | `None` | Inventory path or inline host list |
| `extra_vars` | `dict[str, str]` | `None` | `--extra-vars k=v ...` |
| `extra_vars_file` | `str` | `None` | `--extra-vars @file` |
| `tags` | `list[str]` | `None` | `--tags` |
| `skip_tags` | `list[str]` | `None` | `--skip-tags` |
| `limit` | `str` | `None` | `--limit` host pattern |
| `become` | `bool` | `false` | `--become` (sudo) |
| `become_user` | `str` | `None` | `--become-user` |
| `vault_password_file` | `str` | `None` | `--vault-password-file` |
| `vault_password_env_var` | `str` | `None` | Env var holding vault password |
| `ssh_private_key_env_var` | `str` | `None` | Env var holding SSH private key |
| `check_mode` | `bool` | `false` | `--check` dry run |
| `diff_mode` | `bool` | `false` | `--diff` |
| `verbosity` | `int` | `0` | `-v` to `-vvvv` (0–4) |
| `ansible_bin` | `str` | `"ansible-playbook"` | Path to binary |
| `working_dir` | `str` | `None` | Subprocess cwd |
| `env_vars` | `dict[str, str]` | `None` | Extra subprocess env vars |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group |
| `description` | `str` | `None` | Asset description |
| `deps` | `list[str]` | `None` | Upstream asset keys |

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset key for this component. |
| `playbook` | `str` | Path to the Ansible playbook YAML file. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `limit` | `str` | — | Further limit the hosts targeted by this play (--limit). |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `tags` | `list[str]` | — | Limit execution to tasks with these Ansible tags (--tags). |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group name. |
| `description` | `str` | — | Human-readable description surfaced in the Dagster UI. |
| `deps` | `list[str]` | — | Upstream asset keys this asset depends on. |
| `owners` | `List[str]` | — | Asset owners — team names ('team:analytics') or email addresses. |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned. |
| `partition_start` | `str` | — | Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types. |
| `partition_date_column` | `str` | — | Column used to filter the upstream DataFrame to the current date partition key. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer'. |
| `partition_static_column` | `str` | — | Column used to filter the upstream DataFrame to the current static partition value. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on failure. Defines a RetryPolicy when set. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `inventory` | `str` | — | Inventory file path, directory, or comma-separated host list (e.g. 'host1,host2,'). When None, ansible-playbook falls back to the inventory configured in ansible.cfg. |
| `extra_vars` | `dict[str, str]` | — | Key/value pairs passed as --extra-vars 'k=v k2=v2 ...'. |
| `extra_vars_file` | `str` | — | Path to a vars file passed as --extra-vars @<file>. |
| `skip_tags` | `list[str]` | — | Skip tasks with these Ansible tags (--skip-tags). |
| `become` | `bool` | `false` | Enable privilege escalation (--become / sudo). |
| `become_user` | `str` | — | User to become when privilege escalation is active (--become-user). |
| `vault_password_file` | `str` | — | Path to a file containing the Ansible Vault password (--vault-password-file). |
| `vault_password_env_var` | `str` | — | Name of an environment variable whose value is the Vault password. The component writes the value to a secure temporary file and passes it as --vault-password-file, then deletes it after the run. |
| `ssh_private_key_env_var` | `str` | — | Name of an environment variable containing an SSH private key (PEM). The component writes it to a temp file with 0600 permissions and sets ANSIBLE_PRIVATE_KEY_FILE for the subprocess. |
| `check_mode` | `bool` | `false` | Run in check (dry-run) mode — no changes are made (--check). |
| `diff_mode` | `bool` | `false` | Show diffs for files that are changed (--diff). |
| `verbosity` | `int` | `0` | Verbosity level 0-4. Translates to -v / -vv / -vvv / -vvvv. |
| `ansible_bin` | `str` | `"ansible-playbook"` | Path to the ansible-playbook binary. |
| `working_dir` | `str` | — | Working directory for the subprocess. Defaults to the current process cwd. |
| `env_vars` | `dict[str, str]` | — | Additional environment variables merged into the subprocess environment. |

<!-- FIELDS:END -->
