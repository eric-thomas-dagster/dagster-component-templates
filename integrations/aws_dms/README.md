# AWS Database Migration Service (DMS) Component

Import AWS DMS entities as Dagster assets for orchestrating database migrations and Change Data Capture (CDC) replication.

## Features

- **Replication Tasks**: Start/stop migration and CDC tasks
- **Endpoints**: Observe source and target database connections
- **Replication Instances**: Monitor DMS instance health
- **CDC Support**: Track CDC latency and replication lag
- **Observation Sensor**: Monitor task progress and completion

## Configuration

### Basic Example
```yaml
type: dagster_component_templates.AWSDMSComponent
attributes:
  aws_region: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
  import_replication_tasks: true
```

### Advanced Example
```yaml
type: dagster_component_templates.AWSDMSComponent
attributes:
  aws_region: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"

  # Import all entity types
  import_replication_tasks: true
  import_endpoints: true
  import_replication_instances: true

  # Filtering
  filter_by_name_pattern: ^prod_.*
  exclude_name_pattern: test|dev
  filter_by_tags: env,team

  # Task configuration
  replication_task_type: start-replication

  # Sensor configuration
  generate_sensor: true
  poll_interval_seconds: 60

  group_name: dms_prod
  description: Production database migrations
```

## Entity Types

### Replication Tasks (Materializable)
- Start migration and CDC tasks
- Three start types:
  - **start-replication**: Initial full load (+ CDC if configured)
  - **resume-processing**: Continue from last stop position
  - **reload-target**: Reload all tables and restart CDC
- Wait for task to reach running state
- Track full load progress and CDC latency
- Support for full-load, full-load-and-cdc, and cdc migration types

### Endpoints (Observable)
- Monitor source and target database connections
- Track endpoint status and connection health
- Support for multiple database engines:
  - MySQL, PostgreSQL, Oracle, SQL Server
  - MongoDB, DynamoDB, S3, Redshift
  - Aurora, MariaDB, SAP ASE, and more

### Replication Instances (Observable)
- Monitor DMS instance health and status
- Track instance class and allocated storage
- Observe Multi-AZ configuration
- Monitor engine version

## Authentication

Three authentication options:

1. **IAM Role** (recommended): Omit credentials, use instance/task role
2. **Access Keys**: Provide aws_access_key_id and aws_secret_access_key
3. **Temporary Credentials**: Include aws_session_token

## Permissions

Required IAM permissions:

### Replication Task Operations
- `dms:DescribeReplicationTasks` - List and describe tasks
- `dms:StartReplicationTask` - Start migration/CDC tasks
- `dms:StopReplicationTask` - Stop running tasks

### Endpoint Operations
- `dms:DescribeEndpoints` - List and describe endpoints
- `dms:TestConnection` - Test endpoint connectivity

### Replication Instance Operations
- `dms:DescribeReplicationInstances` - List and describe instances

### Monitoring
- `cloudwatch:GetMetricStatistics` - Access CloudWatch metrics
- `logs:DescribeLogStreams` - Access DMS task logs
- `logs:GetLogEvents` - Read task log events

## Replication Task Types

### Full Load
- One-time migration of existing data
- Tables are loaded in parallel
- No CDC after initial load

### Full Load + CDC
- Initial full load followed by continuous CDC
- Captures changes during full load
- Transitions to CDC after full load completes

### CDC Only
- Continuous replication of changes
- Assumes target already has baseline data
- Captures INSERT, UPDATE, DELETE operations

## CDC Metrics

The component tracks key CDC metrics:

- **CDCLatencySource**: Gap between source event capture and current time
- **CDCLatencyTarget**: Gap between event ready to commit and current time
- **FullLoadProgressPercent**: Progress of initial full load (0-100%)
- **TablesLoaded**: Number of tables successfully loaded
- **TablesLoading**: Number of tables currently loading
- **TablesErrored**: Number of tables with errors

## Limitations

- Task start waits up to 10 minutes for running state
- Endpoint connection testing requires replication instance ARN
- Hard limit of 100 endpoints per replication instance
- Hard limit of 1000 endpoints per AWS account
- Task logs retained for 24 hours by default
- Single-threaded task execution (limited parallelism)

## Use Cases

### Database Migration
- One-time migration from on-premises to cloud
- Cloud-to-cloud migrations
- Database consolidation
- Schema/engine conversions

### Change Data Capture (CDC)
- Real-time data replication
- Data warehouse synchronization
- Analytics database feeds
- Disaster recovery replication

### Hybrid Architectures
- On-premises to cloud replication
- Multi-region data distribution
- Active-passive failover setups
- Data lake ingestion from operational databases

## Best Practices

1. Use `filter_by_name_pattern` to scope to production tasks
2. Apply tags for fine-grained resource filtering
3. Monitor CDC latency for replication lag
4. Use `resume-processing` after investigating failures
5. Use IAM roles instead of access keys when possible
6. Configure CloudWatch alarms for CDC latency
7. Test endpoint connections before creating tasks
8. Allocate sufficient replication instance resources
9. Use Multi-AZ for production workloads

## Troubleshooting

### Task Fails to Start
- Check replication instance is available
- Verify endpoints are reachable
- Review task configuration (table mappings, transformation rules)
- Check IAM permissions

### High CDC Latency
- Increase replication instance size
- Reduce number of tables per task
- Optimize source database for CDC (binary logging, archiving)
- Check network bandwidth between source/target

### Connection Errors
- Verify security groups and network ACLs
- Check database firewall rules
- Validate endpoint credentials
- Ensure SSL/TLS configuration matches database

### Task Stops Unexpectedly
- Review CloudWatch logs for error messages
- Check for source database schema changes
- Verify target database has sufficient storage
- Monitor replication instance disk space

## Integration with Dagster

### Upstream Dependencies
Create dependencies on data readiness:
```python
@asset(deps=["source_data_ready"])
def my_dms_task():
    # Triggers when source data is ready
    pass
```

### Downstream Dependencies
Use migrated data in downstream assets:
```python
@asset(deps=["dms_task_my_migration"])
def my_analytics():
    # Runs after migration completes
    pass
```

### CDC Event Processing
Combine with streaming assets for real-time processing:
```python
@asset(deps=["dms_task_cdc_replication"])
def process_cdc_stream():
    # Process CDC events from target database
    pass
```

## Resources

- [AWS DMS Documentation](https://docs.aws.amazon.com/dms/)
- [boto3 DMS Client Reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dms.html)
- [Creating Tasks for Ongoing Replication](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Task.CDC.html)
- [Monitoring with CloudWatch](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Monitoring.html)
