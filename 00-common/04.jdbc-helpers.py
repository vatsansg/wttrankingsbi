# Databricks notebook source
# MAGIC %md
# MAGIC ## 04. Bronze helpers -- JDBC (Azure SQL Server) sources
# MAGIC Shared read/metadata/validation helpers for all 18 SQL-Server-sourced
# MAGIC bronze tables (4 ledger + 14 reference/player).
# MAGIC
# MAGIC **Partitioning:** none of these tables are read with
# MAGIC `numPartitions`/`partitionColumn` -- at current volumes (hundreds of rows
# MAGIC for reference/player tables, thousands to low hundreds-of-thousands for the
# MAGIC ledger) a single-connection read is fine and simpler to reason about.
# MAGIC Revisit only if a table grows past roughly 500K-1M rows or a read starts
# MAGIC breaching the job's SLA -- and only for a table with a good monotonic
# MAGIC column to partition on (a surrogate id column works for this).

# COMMAND ----------

# MAGIC %run ./03.jdbc-config

# COMMAND ----------

from pyspark.sql import functions as F

def read_from_sqlserver(source_table, columns):
    """
    Read one SQL Server table via JDBC, pinned to an explicit column list.

    Pinning columns (never `SELECT *`) means an upstream schema change -- a
    renamed or dropped column -- surfaces as a read error here instead of
    silently drifting into bronze and breaking Silver downstream. There is no
    JDBC equivalent of the CSV reader's `mode('FAILFAST')`; this explicit
    column pin is the guardrail that plays the same role.
    """
    select_list = ", ".join(columns)
    dbtable_query = f"(SELECT {select_list} FROM {sql_server_source_schema}.{source_table}) AS src"
    return (
        spark.read
             .format('jdbc')
             .option('url', jdbc_url)
             .option('dbtable', dbtable_query)
             .options(**connection_properties)
             .load()
    )


def add_jdbc_ingestion_metadata(df, source_table):
    """
    JDBC reads have no `_metadata.file_path` to stamp (that's a file-source-only
    Spark feature), so JDBC-sourced bronze tables get a `_source_system` /
    `_source_table` pair instead of `_source_file` -- same lineage intent,
    JDBC-appropriate shape.
    """
    return (
        df.withColumn('_ingestion_timestamp', F.current_timestamp())
          .withColumn('_source_system', F.lit(f'AzureSQL:{sql_server_database}'))
          .withColumn('_source_table', F.lit(f'{sql_server_source_schema}.{source_table}'))
    )


def validate_bronze_table(table_name, min_expected_rows=1):
    """
    Lightweight post-write sanity check: table exists and has a sane row count.
    Raises if the table is empty or missing, so a broken/empty pull fails the
    job loudly instead of silently reaching Silver as an empty table.
    """
    count = spark.table(table_name).count()
    if count < min_expected_rows:
        raise ValueError(
            f"Bronze validation FAILED for {table_name}: {count} rows "
            f"(expected >= {min_expected_rows})"
        )
    print(f"OK    {table_name}: {count:,} rows")
    return count