# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest one reference/player table (generic, parameterized)
# MAGIC Driven by `p_table_key`, one of the 14 keys in `REFERENCE_TABLES`
# MAGIC (`00-common/05.reference-table-registry`). Run once per table via a
# MAGIC Databricks Job **for-each task** (see `03-jobs/bronze_ingestion_job.json`)
# MAGIC rather than maintaining 14 near-identical hand-written notebooks -- the
# MAGIC 12 reference tables and 2 player/competitor tables all share this exact
# MAGIC shape (small, JDBC, full overwrite), so one generic notebook plus a
# MAGIC registry entry is the more maintainable pattern for this many
# MAGIC structurally-identical tables.
# MAGIC
# MAGIC For one fully-worked example with no widgets/indirection -- useful for
# MAGIC reading top-to-bottom without mentally substituting a parameter -- see
# MAGIC `02-bronze/08.Ingest Ref Countries (Worked Example)`. It does exactly what
# MAGIC this notebook does for `p_table_key = 'ref_countries'`, spelled out.
# MAGIC
# MAGIC 1. Look up the table's source/target/columns from the registry
# MAGIC 2. Read from Azure SQL Server via JDBC, pinned to the registered column list
# MAGIC 3. Add Metadata Columns
# MAGIC 4. Write to bronze delta table (full overwrite)
# MAGIC 5. Validate row count

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

# MAGIC %run ../00-common/05.reference-table-registry

# COMMAND ----------

dbutils.widgets.text("p_table_key", "ref_countries")
v_table_key = dbutils.widgets.get("p_table_key")

if v_table_key not in REFERENCE_TABLES:
    raise ValueError(
        f"Unknown p_table_key '{v_table_key}'. Valid keys: {sorted(REFERENCE_TABLES)}"
    )

table_config = REFERENCE_TABLES[v_table_key]
source_table = table_config["source_table"]
columns = table_config["columns"]
table_name = f"{catalog_name}.{bronze_schema}.{table_config['target_table']}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read from Azure SQL Server via JDBC

# COMMAND ----------

reference_df = read_from_sqlserver(source_table, columns)
display(reference_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns

# COMMAND ----------

reference_final_df = add_jdbc_ingestion_metadata(reference_df, source_table)
display(reference_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    reference_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Validate

# COMMAND ----------

validate_bronze_table(table_name)