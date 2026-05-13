# SAP RFC Resource

Register a connection to an SAP system over the classic RFC (Remote Function Call) protocol — for on-prem R/3 / ECC / S/4HANA installs that **don't expose OData**. Companion to [`sap_rfc_ingestion`](../../assets/ingestion/sap_rfc_ingestion/).

## When to use RFC vs OData vs HANA SQL

| Path | Use when |
|---|---|
| **`sap_rfc_*`** | On-prem R/3 / ECC / S/4HANA. BAPI / IDoc / custom Z-RFCs. Traditional ABAP integration |
| [`odata_ingestion`](../../assets/ingestion/odata_ingestion/) | S/4HANA Cloud + modern on-prem with Gateway OData services |
| [`sap_hana_ingestion`](../../assets/ingestion/sap_hana_ingestion/) | Direct HANA SQL access (HANA Cloud + HANA-backed on-prem) |

Most on-prem ECC customers only have RFC. S/4HANA Cloud has both OData (preferred) and RFC.

## Driver: pyrfc + SAP NW RFC SDK

`pyrfc` is the official SAP-supplied Python binding to the NetWeaver RFC SDK. The SDK itself is closed-source — free for SAP-licensed customers via SAP Support Portal.

### Install sequence

1. **Download** SAP NW RFC SDK 7.50 from https://launchpad.support.sap.com → Software Downloads → SAP NW RFC SDK 7.50 (requires SAP customer login)
2. **Extract** to `/opt/sap/nwrfcsdk` (or any path)
3. **Set env vars** *before* `pip install pyrfc`:

   ```bash
   export SAPNWRFC_HOME=/opt/sap/nwrfcsdk
   export LD_LIBRARY_PATH=$SAPNWRFC_HOME/lib:$LD_LIBRARY_PATH       # Linux
   export DYLD_LIBRARY_PATH=$SAPNWRFC_HOME/lib:$DYLD_LIBRARY_PATH   # macOS
   ```

4. **Install:** `pip install pyrfc`

For Docker / k8s deployments: bake the SDK into your image:

```dockerfile
FROM python:3.11-slim
COPY nwrfcsdk-linux64.tgz /tmp/
RUN mkdir -p /opt/sap && tar -xzf /tmp/nwrfcsdk-linux64.tgz -C /opt/sap/
ENV SAPNWRFC_HOME=/opt/sap/nwrfcsdk
ENV LD_LIBRARY_PATH=$SAPNWRFC_HOME/lib:$LD_LIBRARY_PATH
RUN pip install pyrfc dagster dagster-community-components ...
```

## Connection modes

### Direct application server

```yaml
ashost: sapecc01.acme.com
sysnr: "00"
client: "100"
```

### Message server (load-balanced)

```yaml
mshost: sapms01.acme.com
sysid: PRD
group: PUBLIC
client: "100"
# msserv: 3601   # optional; defaults to sapms<SYSID>
```

Use message server for production — connections balance across the available app servers automatically.

## Fields

| Field | Type | Required | Notes |
|---|---|---|---|
| `resource_key` | `str` | no | Default `sap_rfc` |
| `ashost` / `sysnr` | `str` | direct mode | App server host + 2-digit system number |
| `mshost` / `sysid` / `group` | `str` | LB mode | Message server + 3-char SID + logon group |
| `msserv` | `str` | no | Override message-server service name |
| `client` | `str` | yes | 3-digit SAP client (e.g. `'100'`) |
| `user` / `passwd_env_var` | `str` | yes | RFC service user |
| `lang` | `str` | no | Default `EN` |
| SNC fields | `str` | no | `snc_qop` / `snc_myname` / `snc_partnername` / `snc_lib` |
| `trace` | `int` | no | 0–3 |

## Usage from a custom asset

```python
@asset(required_resource_keys={"sap_ecc"})
def material_master(context):
    with context.resources.sap_ecc.connection() as conn:
        result = conn.call(
            "RFC_READ_TABLE",
            QUERY_TABLE="MARA",
            ROWCOUNT=1000,
            FIELDS=[{"FIELDNAME": f} for f in ["MATNR", "MTART", "MATKL", "MEINS"]],
        )
        # result['DATA'] is a list of {WA: "value|value|value"} dicts
        return parse_rfc_read_table_result(result)
```

For most use cases, use [`sap_rfc_ingestion`](../../assets/ingestion/sap_rfc_ingestion/) which wraps this resource with a typed-DataFrame output.

## SNC (Secure Network Communication)

For environments where TLS isn't enough, SAP supports SNC via SAP CryptoLib or Kerberos. Set `snc_*` fields when your SAP basis team requires it — usually mandatory in prod.

## Resource ping (smoke test)

```python
context.resources.sap_ecc.ping()   # calls RFC_PING; raises on failure
```

Run this from an asset check or a startup hook to catch credential / network issues before the real work starts.

## See also

- [`sap_rfc_ingestion`](../../assets/ingestion/sap_rfc_ingestion/) — wraps this resource for RFC/BAPI → DataFrame
- [`sap_hana_resource`](../sap_hana_resource/) — HANA SQL alternative
- [`odata_resource`](../odata_resource/) — OData alternative
