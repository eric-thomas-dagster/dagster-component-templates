"""Snowflake Resource component."""
from typing import Optional
import dagster as dg
from pydantic import Field, model_validator


class SnowflakeResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-snowflake SnowflakeResource for use by other components."""

    resource_key: str = Field(default="snowflake_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    account: Optional[str] = Field(default=None, description="Snowflake account identifier, e.g. 'xy12345.us-east-1'. Set this or account_env_var.")
    account_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Snowflake account identifier (secret-hygiene alternative to the plain `account` field -- account identifiers differ per environment, so teams often keep this out of checked-in YAML too).")
    user: Optional[str] = Field(default=None, description="Snowflake username. Set this or user_env_var.")
    user_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Snowflake username.")
    password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the Snowflake password. Set exactly one of password_env_var / pat_env_var / private_key_path_env_var.")
    pat_env_var: Optional[str] = Field(default=None, description="Environment variable holding a Snowflake Programmatic Access Token (passed as the connector's password field -- same convention as snowflake_stored_procedure_call_asset).")
    private_key_path_env_var: Optional[str] = Field(default=None, description="Environment variable holding the path to an RSA private key file, for key-pair authentication.")
    private_key_passphrase_env_var: Optional[str] = Field(default=None, description="Environment variable holding the private key file's passphrase, if it's encrypted. Only used with private_key_path_env_var.")
    database: Optional[str] = Field(default=None, description="Default database")
    warehouse: Optional[str] = Field(default=None, description="Default warehouse")
    role: Optional[str] = Field(default=None, description="Default role")
    schema_name: Optional[str] = Field(default=None, description="Default schema")

    @model_validator(mode="after")
    def _validate_auth_fields(self):
        if not self.account and not self.account_env_var:
            raise ValueError("SnowflakeResourceComponent: set `account` or `account_env_var`.")
        if not self.user and not self.user_env_var:
            raise ValueError("SnowflakeResourceComponent: set `user` or `user_env_var`.")
        auth_methods = [m for m in (self.password_env_var, self.pat_env_var, self.private_key_path_env_var) if m]
        if len(auth_methods) != 1:
            raise ValueError(
                "SnowflakeResourceComponent: set exactly one of `password_env_var`, `pat_env_var`, "
                f"`private_key_path_env_var` (got {len(auth_methods)})."
            )
        return self

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Snowflake Resource', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_snowflake import SnowflakeResource

        kwargs: dict = {
            "account": self.account or dg.EnvVar(self.account_env_var),
            "user": self.user or dg.EnvVar(self.user_env_var),
            "database": self.database,
            "warehouse": self.warehouse,
            "role": self.role,
            "schema": self.schema_name,
        }
        if self.pat_env_var:
            kwargs["password"] = dg.EnvVar(self.pat_env_var)
        elif self.private_key_path_env_var:
            kwargs["private_key_path"] = dg.EnvVar(self.private_key_path_env_var)
            if self.private_key_passphrase_env_var:
                kwargs["private_key_password"] = dg.EnvVar(self.private_key_passphrase_env_var)
        else:
            kwargs["password"] = dg.EnvVar(self.password_env_var)

        resource = SnowflakeResource(**kwargs)
        return dg.Definitions(resources={self.resource_key: resource})
