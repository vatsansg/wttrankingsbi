# Databricks notebook source
# MAGIC %md
# MAGIC # Silver one reference table (generic, parameterized)
# MAGIC Driven by `p_table_key`, one of the 12 keys in `SILVER_REFERENCE_TABLES`
# MAGIC (`00-common/07.silver-reference-registry`). Run once per table via a
# MAGIC Databricks Job **for-each task**, same pattern as bronze notebook 07 --
# MAGIC these 12 reference tables all get the same light treatment (snake_case,
# MAGIC drop soft-deleted rows where that column exists), so one generic
# MAGIC notebook is more maintainable than 12 near-identical ones.
# MAGIC
# MAGIC Does NOT cover `competitors` / `players_doubles` (bronze's other 2
# MAGIC generic-notebook tables) -- those feed `03-silver/01.Build Player
# MAGIC Identity Dimension` instead of a 1:1 silver copy.
# MAGIC 1. Look up the table's config from the registry
# MAGIC 2. Read from bronze, snake_case
# MAGIC 3. Drop soft-deleted rows (`is_deleted = true`) where that column exists
# MAGIC 4. Write to silver delta table (full overwrite)

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

# MAGIC %run ../00-common/07.silver-reference-registry

# COMMAND ----------

dbutils.widgets.text("p_table_key", "")
v_table_key = dbutils.widgets.get("p_table_key")

if v_table_key not in SILVER_REFERENCE_TABLES:
    raise ValueError(
        f"Unknown p_table_key '{v_table_key}'. Valid keys: {sorted(SILVER_REFERENCE_TABLES)}"
    )

table_config = SILVER_REFERENCE_TABLES[v_table_key]
source_table = f"{catalog_name}.{bronze_schema}.{v_table_key}"
target_table = f"{catalog_name}.{silver_schema}.{v_table_key}"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read from bronze, snake_case

# COMMAND ----------

bronze_df = spark.table(source_table)
renamed_df = rename_to_snake_case(bronze_df)

# 4 of these 12 tables (ref_subevents_codes, ref_subevents_codes_description,
# ref_subevent_dependent_categories, ref_subevent_types) carry a SubEventCode
# column. The generic snake_case pass splits it to sub_event_code (two words),
# not subevent_code (one word) -- same mismatch already fixed in the bespoke
# 02/03/04 notebooks, missed here because it only shows up on the subset of
# reference tables that happen to have this column. Applied conditionally
# since most of the 12 tables don't have it.
if 'sub_event_code' in renamed_df.columns:
    renamed_df = renamed_df.withColumnRenamed('sub_event_code', 'subevent_code')

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop soft-deleted rows, if this table has that column

# COMMAND ----------

if table_config["has_is_deleted"]:
    filtered_df = renamed_df.filter(
        (F.col('is_deleted').isNull()) | (F.col('is_deleted') == False)  # noqa: E712
    )
else:
    filtered_df = renamed_df

silver_reference_df = add_silver_metadata(filtered_df)
display(silver_reference_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to silver delta table

# COMMAND ----------

(
    silver_reference_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())
