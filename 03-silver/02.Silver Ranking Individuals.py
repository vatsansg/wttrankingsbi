# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Ranking Individuals
# MAGIC Source: `bronze.ranking_individuals` (already SEN+YOU unioned in bronze)
# MAGIC 1. Read bronze, rename to snake_case
# MAGIC 2. Normalize the identity column to `ittfid` (matches the dimension
# MAGIC    built in notebook 01) and resolve it against `silver.player_identity`
# MAGIC 3. Dedupe on the natural key (a safety net -- see `06.silver-helpers`)
# MAGIC 4. Write to `silver.ranking_individuals`
# MAGIC
# MAGIC Depends on notebook 01 (`Build Player Identity Dimension`) having run
# MAGIC first in this same job -- see `03-jobs/`.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

source_table = f"{catalog_name}.{bronze_schema}.ranking_individuals"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.ranking_individuals"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze, snake_case, normalize the identity column
# MAGIC `rename_to_snake_case` splits every capitalized word boundary, so
# MAGIC `SubEventCode` -> `sub_event_code` (not `subevent_code`) -- renamed
# MAGIC back to `subevent_code` here to match the term used everywhere else in
# MAGIC this project (the bronze reference tables are already named
# MAGIC `ref_subevents_codes` etc., treating "subevent" as one word).

# COMMAND ----------

bronze_df = spark.table(source_table)

renamed_df = (
    rename_to_snake_case(bronze_df)
        .withColumnRenamed('ittf_id', 'ittfid')
        .withColumnRenamed('sub_event_code', 'subevent_code')
        .withColumn('entity_type', F.lit('INDIVIDUAL'))
        .withColumn('is_active', F.col('is_active').cast('boolean'))
)

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Resolve `ittfid` against the player identity dimension

# COMMAND ----------

player_identity_df = spark.table(identity_table)

resolved_df = resolve_identity(renamed_df, 'ittfid', 'entity_type', player_identity_df)

display(resolved_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Dedupe on natural key (player x subevent x week x run),
# MAGIC keep the most recently updated row if a true duplicate ever shows up

# COMMAND ----------

deduped_df = dedup_on_business_key(
    resolved_df,
    key_cols=['ittfid', 'subevent_code', 'ranking_year', 'ranking_week', 'ranking_run_code'],
    order_by_desc_cols=['updated_date', 'publish_date'],
)

silver_ranking_individuals_df = add_silver_metadata(deduped_df)

display(silver_ranking_individuals_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to silver delta table
# MAGIC Full overwrite, same as bronze, for this one-off load track -- bronze
# MAGIC itself only ever holds the latest loaded week (also `overwrite`), so
# MAGIC there is no history to preserve here yet either. The incremental
# MAGIC production track (Step 7+) switches both layers to append/merge.

# COMMAND ----------

(
    silver_ranking_individuals_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())

# COMMAND ----------

display(
    spark.table(target_table)
         .filter(F.col('identity_resolved') == False)  # noqa: E712
         .count()
)
