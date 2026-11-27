# Lambda Resource

Register a `LambdaResource` exposing `invoke(...)` -- the AWS Lambda invoke
action as a reusable, injectable resource method, callable from any op or
asset you write, rather than baked into a single-purpose asset component.

See `assets/infrastructure/lambda_invoke_asset` for a ready-made asset that
wraps this same logic if you just want a materializable asset out of the
box with no code.

## Why a resource, not an asset

An AWS Lambda invocation is a reusable action, not data with lineage --
packaging it as an injectable resource lets any op or asset in your own
code call it directly, rather than locking the logic inside one
pre-built asset shape.

## Usage

```python
import dagster as dg

@dg.asset
def my_asset(context: dg.AssetExecutionContext, lambda_resource) -> None:
    result = lambda_resource.invoke(
        function_name="my-function",
        payload={"key": "value"},
        log=context.log,
    )
    context.log.info(result["payload"])
```

`region_name` is optional -- omit it to fall back to boto3's own default
credential/region chain (env vars, profile config, instance metadata).

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `lambda_resource` | Key used to register this resource. Other components reference it via resource_key. |
| `region_name` | `Optional[str]` | — | AWS region, e.g. 'us-east-1'. Omit to use the default boto3 credential/region chain (env vars, profile config, instance role). |
| `profile_name` | `Optional[str]` | — | Named AWS CLI/SDK profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.). |
| `aws_access_key_id_env_var` | `Optional[str]` | — | Env var holding the AWS access key ID. |
| `aws_secret_access_key_env_var` | `Optional[str]` | — | Env var holding the AWS secret access key. |
| `aws_session_token_env_var` | `Optional[str]` | — | Env var holding a temporary AWS session token, paired with the access key/secret above. |

## `invoke(...)` arguments

| Argument | Type | Default | Description |
|---|---|---|---|
| `function_name` | `str` | required | Lambda function name, partial ARN, or full ARN. |
| `payload` | `Optional[Dict[str, Any]]` | `None` | JSON payload sent as the Lambda event. |
| `invocation_type` | `str` | `RequestResponse` | `RequestResponse` (synchronous, parses the response) or `Event` (async fire-and-forget). |
| `qualifier` | `Optional[str]` | `None` | Function version or alias to invoke, e.g. `PROD` or `3`. Omit for `$LATEST`. |
| `include_logs` | `bool` | `False` | Request the last 4KB of CloudWatch logs (`LogType=Tail`) and include them in the result. |
| `fail_on_function_error` | `bool` | `True` | Raise if the Lambda's own invocation errored (`FunctionError` in the response). `False` returns the error in the result dict instead. |
| `log` | `Optional[Any]` | `None` | A logger (e.g. `context.log`) for an info-level invocation summary. |
