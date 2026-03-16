# KafkaResourceComponent

Registers a [Kafka](https://kafka.apache.org) resource providing producer and consumer factories in your Dagster project.

## Install

```
pip install kafka-python>=2.0.2
```

## Configuration

```yaml
type: dagster_component_templates.KafkaResourceComponent
attributes:
  resource_key: kafka_resource
  bootstrap_servers: broker1:9092,broker2:9092
  security_protocol: SASL_SSL          # optional, default: PLAINTEXT
  sasl_mechanism: SCRAM-SHA-256        # optional
  sasl_username_env_var: KAFKA_USER    # optional
  sasl_password_env_var: KAFKA_PASS    # optional
```

## Auth

For SASL-authenticated clusters set the environment variables named in `sasl_username_env_var` and `sasl_password_env_var`.

```
export KAFKA_USER=<username>
export KAFKA_PASS=<password>
```
