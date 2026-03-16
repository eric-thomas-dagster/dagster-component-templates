# Sftp Resource

Registers an SFTP resource for reading and writing files over SSH in your Dagster project.

## Install

```
pip install paramiko
```

## Configuration

```yaml
type: dagster_component_templates.SFTPResourceComponent
attributes:
  resource_key: sftp_resource
  host: sftp.example.com
  username: deploy
  port: 22                              # optional, default: 22
  password_env_var: SFTP_PASSWORD       # optional
  private_key_env_var: SFTP_PKEY_PEM   # optional
  private_key_path: /path/to/key.pem   # optional
```

## Auth

Provide exactly one of: `password_env_var`, `private_key_env_var` (PEM string stored in an env var), or `private_key_path` (path to a key file on disk). Key-based auth is recommended for production.

```
export SFTP_PASSWORD=<password>
# or
export SFTP_PKEY_PEM="$(cat ~/.ssh/id_rsa)"
```
