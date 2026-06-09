"""Database Schema Inventory.

List every database object in a source SQL database — tables, views,
procedures, functions, sequences, triggers, scheduled jobs — into a single
DataFrame asset.

The companion piece to `database_replication` during a warehouse migration.
`database_replication` moves the DATA (tables); this component surfaces the
NON-data objects (PL/SQL procs, Oracle scheduler jobs, views, etc.) that have
to be rewritten by hand for the target warehouse — so the migration team has
a concrete checklist instead of a wishful-thinking estimate.

Supported source dialects: postgres / mysql / mssql / oracle / db2 /
snowflake / redshift. Dialect-specific metadata queries are baked in.
"""
from typing import Dict, List, Optional

import dagster as dg
from pydantic import Field


_INVENTORY_QUERIES = {
    "postgres": {
        "table": """
            SELECT 'table' AS object_type, table_schema AS schema_name, table_name AS object_name,
                   NULL::text AS definition, NULL::bigint AS row_count
            FROM information_schema.tables WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
        """,
        "view": """
            SELECT 'view', table_schema, table_name,
                   view_definition, NULL::bigint
            FROM information_schema.views
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        """,
        "procedure": """
            SELECT 'procedure', routine_schema, routine_name,
                   routine_definition, NULL::bigint
            FROM information_schema.routines
            WHERE routine_type = 'PROCEDURE'
              AND routine_schema NOT IN ('pg_catalog', 'information_schema')
        """,
        "function": """
            SELECT 'function', routine_schema, routine_name,
                   routine_definition, NULL::bigint
            FROM information_schema.routines
            WHERE routine_type = 'FUNCTION'
              AND routine_schema NOT IN ('pg_catalog', 'information_schema')
        """,
        "sequence": """
            SELECT 'sequence', sequence_schema, sequence_name,
                   NULL::text, NULL::bigint
            FROM information_schema.sequences
            WHERE sequence_schema NOT IN ('pg_catalog', 'information_schema')
        """,
        "trigger": """
            SELECT 'trigger', trigger_schema, trigger_name,
                   action_statement, NULL::bigint
            FROM information_schema.triggers
            WHERE trigger_schema NOT IN ('pg_catalog', 'information_schema')
        """,
    },
    "oracle": {
        "table":     "SELECT 'table' AS object_type, owner AS schema_name, table_name AS object_name, NULL AS definition, num_rows AS row_count FROM all_tables WHERE owner NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS','GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS','DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')",
        "view":      "SELECT 'view', owner, view_name, text, NULL FROM all_views WHERE owner NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS','GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS','DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')",
        "procedure": "SELECT 'procedure', owner, object_name, NULL, NULL FROM all_procedures WHERE object_type='PROCEDURE' AND owner NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS','GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS','DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')",
        "function":  "SELECT 'function', owner, object_name, NULL, NULL FROM all_procedures WHERE object_type='FUNCTION' AND owner NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS','GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS','DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')",
        "package":   "SELECT 'package', owner, object_name, NULL, NULL FROM all_procedures WHERE object_type='PACKAGE' AND owner NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS','GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS','DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')",
        "sequence":  "SELECT 'sequence', sequence_owner, sequence_name, NULL, NULL FROM all_sequences WHERE sequence_owner NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS','GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS','DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')",
        "trigger":   "SELECT 'trigger', owner, trigger_name, trigger_body, NULL FROM all_triggers WHERE owner NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS','GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS','DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')",
        "job":       "SELECT 'job', owner, job_name, job_action, NULL FROM all_scheduler_jobs WHERE owner NOT IN ('SYS','SYSTEM','XDB','MDSYS','CTXSYS','DBSNMP','OUTLN','APPQOSSYS','GSMADMIN_INTERNAL','AUDSYS','OJVMSYS','ORDDATA','ORDSYS','LBACSYS','DVSYS','DVF','REMOTE_SCHEDULER_AGENT','GGSYS')",
    },
    "db2": {
        "table":     "SELECT 'table' AS object_type, tabschema AS schema_name, tabname AS object_name, NULL AS definition, card AS row_count FROM syscat.tables WHERE type='T' AND tabschema NOT LIKE 'SYS%'",
        "view":      "SELECT 'view', tabschema, tabname, NULL, NULL FROM syscat.tables WHERE type='V' AND tabschema NOT LIKE 'SYS%'",
        "procedure": "SELECT 'procedure', procschema, procname, NULL, NULL FROM syscat.procedures WHERE procschema NOT LIKE 'SYS%'",
        "function":  "SELECT 'function', funcschema, funcname, NULL, NULL FROM syscat.functions WHERE funcschema NOT LIKE 'SYS%'",
        "sequence":  "SELECT 'sequence', seqschema, seqname, NULL, NULL FROM syscat.sequences WHERE seqschema NOT LIKE 'SYS%'",
        "trigger":   "SELECT 'trigger', trigschema, trigname, NULL, NULL FROM syscat.triggers WHERE trigschema NOT LIKE 'SYS%'",
    },
    # IBM i / AS/400 / iSeries — Db2 for i uses the QSYS2 catalog, NOT
    # SYSCAT.* (which is Db2 LUW-only). On i:
    #   - schemas / libraries: QSYS2.SYSSCHEMAS
    #   - tables / files:      QSYS2.SYSTABLES  (table_type='T' for tables, 'V' for views, 'P' for physical files)
    #   - procedures:          QSYS2.SYSPROCS
    #   - functions:           QSYS2.SYSFUNCS
    #   - triggers:            QSYS2.SYSTRIGGERS
    #   - sequences:           QSYS2.SYSSEQUENCES
    # The "library list" concept replaces the schema search path —
    # connect via db2_resource(system_type=iseries, library_list=[...])
    # to drive CURRENT SCHEMA at session start.
    "db2_iseries": {
        "table":     "SELECT 'table' AS object_type, table_schema AS schema_name, table_name AS object_name, NULL AS definition, NULL AS row_count FROM qsys2.systables WHERE table_type IN ('T','P') AND table_schema NOT IN ('QSYS','QSYS2','SYSIBM','SYSPROC','SYSCAT','SYSIBMADM','QSYS2ROW')",
        "view":      "SELECT 'view', table_schema, table_name, NULL, NULL FROM qsys2.systables WHERE table_type='V' AND table_schema NOT IN ('QSYS','QSYS2','SYSIBM','SYSPROC','SYSCAT','SYSIBMADM','QSYS2ROW')",
        "procedure": "SELECT 'procedure', routine_schema, routine_name, NULL, NULL FROM qsys2.sysprocs WHERE routine_schema NOT IN ('QSYS','QSYS2','SYSIBM','SYSPROC','SYSCAT','SYSIBMADM','QSYS2ROW')",
        "function":  "SELECT 'function', routine_schema, routine_name, NULL, NULL FROM qsys2.sysfuncs WHERE routine_schema NOT IN ('QSYS','QSYS2','SYSIBM','SYSPROC','SYSCAT','SYSIBMADM','QSYS2ROW')",
        "sequence":  "SELECT 'sequence', sequence_schema, sequence_name, NULL, NULL FROM qsys2.syssequences WHERE sequence_schema NOT IN ('QSYS','QSYS2','SYSIBM','SYSPROC','SYSCAT','SYSIBMADM','QSYS2ROW')",
        "trigger":   "SELECT 'trigger', trigger_schema, trigger_name, NULL, NULL FROM qsys2.systriggers WHERE trigger_schema NOT IN ('QSYS','QSYS2','SYSIBM','SYSPROC','SYSCAT','SYSIBMADM','QSYS2ROW')",
    },
    "snowflake": {
        "table":     "SELECT 'table' AS OBJECT_TYPE, TABLE_SCHEMA AS SCHEMA_NAME, TABLE_NAME AS OBJECT_NAME, NULL AS DEFINITION, ROW_COUNT FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA <> 'INFORMATION_SCHEMA'",
        "view":      "SELECT 'view', TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION, NULL FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'",
        "procedure": "SELECT 'procedure', PROCEDURE_SCHEMA, PROCEDURE_NAME, NULL, NULL FROM INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_SCHEMA <> 'INFORMATION_SCHEMA'",
        "function":  "SELECT 'function', FUNCTION_SCHEMA, FUNCTION_NAME, NULL, NULL FROM INFORMATION_SCHEMA.FUNCTIONS WHERE FUNCTION_SCHEMA <> 'INFORMATION_SCHEMA'",
        "sequence":  "SELECT 'sequence', SEQUENCE_SCHEMA, SEQUENCE_NAME, NULL, NULL FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA <> 'INFORMATION_SCHEMA'",
    },
    "mysql": {
        "table":     "SELECT 'table' AS object_type, table_schema AS schema_name, table_name AS object_name, NULL AS definition, table_rows AS row_count FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('mysql','sys','information_schema','performance_schema')",
        "view":      "SELECT 'view', table_schema, table_name, view_definition, NULL FROM information_schema.views WHERE table_schema NOT IN ('mysql','sys','information_schema','performance_schema')",
        "procedure": "SELECT 'procedure', routine_schema, routine_name, routine_definition, NULL FROM information_schema.routines WHERE routine_type='PROCEDURE' AND routine_schema NOT IN ('mysql','sys','information_schema','performance_schema')",
        "function":  "SELECT 'function', routine_schema, routine_name, routine_definition, NULL FROM information_schema.routines WHERE routine_type='FUNCTION' AND routine_schema NOT IN ('mysql','sys','information_schema','performance_schema')",
        "trigger":   "SELECT 'trigger', trigger_schema, trigger_name, action_statement, NULL FROM information_schema.triggers WHERE trigger_schema NOT IN ('mysql','sys','information_schema','performance_schema')",
    },
    "mssql": {
        "table":     "SELECT 'table' AS object_type, s.name AS schema_name, t.name AS object_name, NULL AS definition, CAST(p.rows AS BIGINT) AS row_count FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id LEFT JOIN sys.partitions p ON p.object_id=t.object_id AND p.index_id IN (0,1)",
        "view":      "SELECT 'view', s.name, v.name, m.definition, NULL FROM sys.views v JOIN sys.schemas s ON s.schema_id=v.schema_id LEFT JOIN sys.sql_modules m ON m.object_id=v.object_id",
        "procedure": "SELECT 'procedure', s.name, p.name, m.definition, NULL FROM sys.procedures p JOIN sys.schemas s ON s.schema_id=p.schema_id LEFT JOIN sys.sql_modules m ON m.object_id=p.object_id",
        "function":  "SELECT 'function', s.name, o.name, m.definition, NULL FROM sys.objects o JOIN sys.schemas s ON s.schema_id=o.schema_id LEFT JOIN sys.sql_modules m ON m.object_id=o.object_id WHERE o.type IN ('FN','IF','TF')",
        "trigger":   "SELECT 'trigger', s.name, tr.name, m.definition, NULL FROM sys.triggers tr JOIN sys.objects o ON o.object_id=tr.parent_id JOIN sys.schemas s ON s.schema_id=o.schema_id LEFT JOIN sys.sql_modules m ON m.object_id=tr.object_id",
    },
    # Apache Doris — speaks MySQL wire protocol; INFORMATION_SCHEMA is
    # MySQL-compatible. Doris has no stored procedures / functions /
    # triggers / sequences (it's an OLAP DB), so only tables + views.
    # Doris does have a unique "materialized views" surface — exposed
    # via INFORMATION_SCHEMA.MV_INFOS or SHOW MATERIALIZED VIEWS.
    "doris": {
        "table":     "SELECT 'table' AS object_type, table_schema AS schema_name, table_name AS object_name, NULL AS definition, NULL AS row_count FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('information_schema','__internal_schema','mysql','sys')",
        "view":      "SELECT 'view', table_schema, table_name, view_definition, NULL FROM information_schema.views WHERE table_schema NOT IN ('information_schema','__internal_schema','mysql','sys')",
    },
    # StarRocks — same OLAP MPP family as Doris (literal fork); identical
    # INFORMATION_SCHEMA surface. Catalog queries identical to Doris.
    "starrocks": {
        "table":     "SELECT 'table' AS object_type, table_schema AS schema_name, table_name AS object_name, NULL AS definition, NULL AS row_count FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('information_schema','_statistics_','sys')",
        "view":      "SELECT 'view', table_schema, table_name, view_definition, NULL FROM information_schema.views WHERE table_schema NOT IN ('information_schema','_statistics_','sys')",
    },
    # ClickHouse — uses system.tables + system.views; SQL dialect is its
    # own (close to ANSI). Includes engine info on tables which is useful
    # for migration planning (MergeTree / ReplicatedMergeTree / etc.).
    "clickhouse": {
        "table":     "SELECT 'table' AS object_type, database AS schema_name, name AS object_name, engine AS definition, total_rows AS row_count FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') AND engine NOT LIKE 'View%' AND engine NOT LIKE 'MaterializedView%'",
        "view":      "SELECT 'view', database, name, create_table_query, NULL FROM system.tables WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema') AND (engine LIKE 'View%' OR engine LIKE 'MaterializedView%')",
    },
    "redshift": {
        "table":     "SELECT 'table' AS object_type, table_schema AS schema_name, table_name AS object_name, NULL AS definition, NULL AS row_count FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog','information_schema')",
        "view":      "SELECT 'view', table_schema, table_name, view_definition, NULL FROM information_schema.views WHERE table_schema NOT IN ('pg_catalog','information_schema')",
        "function":  "SELECT 'function', routine_schema, routine_name, routine_definition, NULL FROM information_schema.routines WHERE routine_schema NOT IN ('pg_catalog','information_schema')",
    },
}


class DatabaseSchemaInventoryComponent(dg.Component, dg.Model, dg.Resolvable):
    """List every object (table / view / proc / function / sequence / trigger / job)
    in a source SQL database as a DataFrame.

    Companion to `database_replication` for warehouse migration projects:
    `database_replication` moves the data; this component surfaces the
    non-data objects (PL/SQL, Oracle scheduler jobs, views, etc.) that the
    migration team has to rewrite by hand. The output is a checklist.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    connection: Optional[str] = Field(
        default=None,
        description="SQLAlchemy URL for the source DB (e.g. 'postgresql://user:pass@host:5432/db'). "
                    "Set this OR connection_env_var.",
    )
    connection_env_var: Optional[str] = Field(
        default=None,
        description="Env var with the source database SQLAlchemy URL. Set this OR connection.",
    )
    database_type: str = Field(
        description=(
            "Source database dialect: postgres / mysql / mssql / oracle / "
            "db2 / db2_iseries / snowflake / redshift. Use 'db2' for Db2 "
            "LUW / Cloud / Warehouse (catalog at SYSCAT.*); use "
            "'db2_iseries' for Db2 for i / AS/400 / IBM i (catalog at "
            "QSYS2.*). Connect via db2_resource(system_type='iseries', "
            "library_list=[...]) for the i path."
        ),
    )

    schemas: Optional[List[str]] = Field(
        default=None,
        description="Optional schema filter — only list objects in these schemas. Default: all non-system schemas.",
    )
    object_types: Optional[List[str]] = Field(
        default=None,
        description="Optional filter — only inventory these object types. Default: every type the dialect supports.",
    )

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
    description: Optional[str] = Field(default=None, description="Asset description")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners")
    asset_tags: Optional[Dict[str, str]] = Field(default=None, description="Additional asset tags")
    kinds: Optional[List[str]] = Field(default=None, description="Asset kinds (default: [database_type, 'inventory'])")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys")

    @classmethod
    def get_description(cls) -> str:
        return (
            "Inventory every database object (tables / views / procedures / functions / sequences / "
            "triggers / scheduled jobs) in a source SQL database — companion to database_replication "
            "for warehouse migration planning."
        )

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        db_type = self.database_type.lower()
        if db_type not in _INVENTORY_QUERIES:
            raise ValueError(
                f"database_type={db_type!r} not supported. Use one of {sorted(_INVENTORY_QUERIES.keys())}."
            )

        queries = _INVENTORY_QUERIES[db_type]
        if self.object_types:
            wanted = {t.lower() for t in self.object_types}
            unsupported = wanted - set(queries.keys())
            if unsupported:
                raise ValueError(
                    f"object_types {sorted(unsupported)} not supported for "
                    f"database_type={db_type!r}. Available: {sorted(queries.keys())}."
                )
            queries = {t: q for t, q in queries.items() if t in wanted}

        connection_literal = self.connection
        connection_env_var = self.connection_env_var
        asset_name = self.asset_name
        schemas_filter = [s.lower() for s in (self.schemas or [])]

        kinds_list = self.kinds or [db_type, "inventory"]
        all_tags = dict(self.asset_tags or {})
        for k in kinds_list:
            all_tags[f"dagster/kind/{k}"] = ""

        @dg.asset(
            key=dg.AssetKey.from_user_string(asset_name),
            group_name=self.group_name,
            description=self.description or self.get_description(),
            owners=self.owners or [],
            tags=all_tags,
            kinds=set(kinds_list),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def _inventory(context: dg.AssetExecutionContext):
            import os
            import pandas as pd
            from sqlalchemy import create_engine, text

            if connection_literal:
                url = connection_literal
            elif connection_env_var and connection_env_var in os.environ:
                url = os.environ[connection_env_var]
            else:
                raise EnvironmentError(
                    f"Set either 'connection' or env var '{connection_env_var}'"
                )
            # Normalize URL scheme so the same env var works with Sling-style
            # URLs (postgres://, mssql://) and SQLAlchemy-style ones (postgresql://, mssql+pyodbc://).
            _scheme_aliases = {
                "postgres://": "postgresql+psycopg2://",
                "postgresql://": "postgresql+psycopg2://",
                "mysql://": "mysql+pymysql://",
                "mssql://": "mssql+pyodbc://",
                "sqlserver://": "mssql+pyodbc://",
                "oracle://": "oracle+oracledb://",
                "db2://": "ibm_db_sa://",
                "redshift://": "redshift+redshift_connector://",
            }
            for src_prefix, sa_prefix in _scheme_aliases.items():
                if url.startswith(src_prefix) and not url.startswith(sa_prefix):
                    url = sa_prefix + url[len(src_prefix):]
                    break
            engine = create_engine(url)

            # Auto-detect Db2-LUW vs Db2-for-i (AS/400). Customers can leave
            # database_type='db2' and we route to the right catalog by
            # probing for QSYS2.SYSTABLES — exists on Db2 for i, doesn't
            # exist on Db2 LUW. Saves the customer from picking
            # database_type='db2_iseries' explicitly.
            _db_type_resolved = db_type
            if db_type == "db2":
                try:
                    with engine.connect() as _probe:
                        _probe.execute(text("SELECT 1 FROM QSYS2.SYSTABLES FETCH FIRST 1 ROW ONLY"))
                    _db_type_resolved = "db2_iseries"
                    context.log.info("Auto-detected Db2 for i (QSYS2 catalog) — routing inventory to db2_iseries dialect.")
                except Exception:
                    # QSYS2 not present → Db2 LUW; keep db_type='db2'.
                    pass
            queries_resolved = _INVENTORY_QUERIES[_db_type_resolved]
            if self.object_types:
                queries_resolved = {k: v for k, v in queries_resolved.items() if k in self.object_types}

            canonical_cols = ["object_type", "schema_name", "object_name", "definition", "row_count"]
            frames = []
            with engine.connect() as conn:
                for object_type, sql in queries_resolved.items():
                    try:
                        df = pd.read_sql(text(sql), conn)
                    except Exception as e:
                        context.log.warning(
                            f"Skipping {object_type}: {e.__class__.__name__}: {str(e)[:200]}"
                        )
                        continue
                    if df.shape[1] != 5:
                        context.log.warning(
                            f"Inventory query for {object_type} returned {df.shape[1]} cols, expected 5; skipping"
                        )
                        continue
                    df.columns = canonical_cols
                    if not df.empty:
                        frames.append(df)
                        context.log.info(f"Inventoried {len(df)} {object_type}(s)")

            if not frames:
                empty = pd.DataFrame(
                    columns=["object_type", "schema_name", "object_name", "definition", "row_count"]
                )
                return dg.MaterializeResult(
                    value=empty,
                    metadata={
                        "row_count": dg.MetadataValue.int(0),
                        "dagster/row_count": dg.MetadataValue.int(0),
                    },
                )

            inventory = pd.concat(frames, ignore_index=True)

            if schemas_filter:
                inventory = inventory[
                    inventory["schema_name"].astype(str).str.lower().isin(schemas_filter)
                ]

            summary = (
                inventory.groupby("object_type")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )

            return dg.MaterializeResult(
                value=inventory,
                metadata={
                    "row_count": dg.MetadataValue.int(len(inventory)),
                    "dagster/row_count": dg.MetadataValue.int(len(inventory)),
                    "database_type": dg.MetadataValue.text(db_type),
                    "by_object_type": dg.MetadataValue.md(
                        summary.to_markdown(index=False)
                    ),
                    "preview": dg.MetadataValue.md(
                        inventory.head(50).to_markdown(index=False)
                    ),
                },
            )

        return dg.Definitions(assets=[_inventory])
