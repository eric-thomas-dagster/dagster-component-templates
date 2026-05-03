# SiemEventNormalizerComponent

Normalize heterogeneous audit-log events to a common schema (OCSF or ECS) before shipping to a SIEM.

## Dependencies
- `pandas`

## Authentication
Reads from environment variables — see the `*_env` fields in `component.py`.

## See also
- [Schema](schema.json)
- [Example config](example.yaml)
