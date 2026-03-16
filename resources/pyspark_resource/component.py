"""PySpark Resource component."""
from dataclasses import dataclass
from typing import Optional, Dict
import dagster as dg
from pydantic import Field


@dataclass
class PySparkResourceComponent(dg.Component, dg.Model, dg.Resolvable):
    """Register a dagster-pyspark PySparkResource for use by other components."""

    resource_key: str = Field(default="pyspark_resource", description="Key used to register this resource. Other components reference it via resource_key.")
    spark_config: Optional[Dict[str, str]] = Field(default=None, description="Spark configuration key-value pairs, e.g. {'spark.executor.memory': '4g'}")
    master: Optional[str] = Field(default=None, description="Spark master URL, e.g. 'local[*]' or 'spark://host:7077'")
    app_name: str = Field(default="dagster", description="Spark application name")

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        from dagster_pyspark import PySparkResource
        spark_conf = dict(self.spark_config or {})
        if self.master:
            spark_conf["spark.master"] = self.master
        resource = PySparkResource(spark_config=spark_conf)
        return dg.Definitions(resources={self.resource_key: resource})
