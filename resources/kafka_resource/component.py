"""Kafka Resource component."""
from dataclasses import dataclass
from typing import Optional
import os
import dagster as dg
from pydantic import Field


class KafkaResource(dg.ConfigurableResource):
    bootstrap_servers: str
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str = ""
    sasl_username: str = ""
    sasl_password: str = ""

    def _common_config(self) -> dict:
        cfg = {
            "bootstrap_servers": self.bootstrap_servers.split(","),
            "security_protocol": self.security_protocol,
        }
        if self.sasl_mechanism:
            cfg["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            cfg["sasl_plain_username"] = self.sasl_username
            cfg["sasl_plain_password"] = self.sasl_password
        return cfg

    def get_producer(self, **kwargs):
        from kafka import KafkaProducer
        return KafkaProducer(**{**self._common_config(), **kwargs})

    def get_consumer(self, *topics, **kwargs):
        from kafka import KafkaConsumer
        return KafkaConsumer(*topics, **{**self._common_config(), **kwargs})


@dataclass
class KafkaResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a Kafka resource providing producer and consumer factories."""

    resource_key: str = Field(default="kafka_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    bootstrap_servers: str = Field(description="Comma-separated Kafka broker addresses, e.g. 'broker1:9092,broker2:9092'")
    security_protocol: Optional[str] = Field(default="PLAINTEXT", description="Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL")
    sasl_mechanism: Optional[str] = Field(default=None, description="SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512")
    sasl_username_env_var: Optional[str] = Field(default=None, description="Environment variable holding the SASL username")
    sasl_password_env_var: Optional[str] = Field(default=None, description="Environment variable holding the SASL password")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        resource = KafkaResource(
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol or "PLAINTEXT",
            sasl_mechanism=self.sasl_mechanism or "",
            sasl_username=os.environ.get(self.sasl_username_env_var, "") if self.sasl_username_env_var else "",
            sasl_password=os.environ.get(self.sasl_password_env_var, "") if self.sasl_password_env_var else "",
        )
        return dg.Definitions(resources={self.resource_key: resource})
