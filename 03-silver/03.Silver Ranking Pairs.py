# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Ranking Pairs
# MAGIC Source: `bronze.ranking_pairs` (already SEN+YOU unioned in bronze)
# MAGIC 1. Read bronze, rename to snake_case
# MAGIC 2. Add a uniform `ittfid` column (== `pair_id` -- pairs are their own
# MAGIC    entity in the identity model) so Gold can treat
# MAGIC    `silver.ranking_individuals` and `silver.ranking_pairs` the same way;
# MAGIC    also normalize the two partner-id columns to `partner1_ittfid` /
# MAGIC    `partner2_ittfid` to match `silver.player_identity`'s naming
# MAGIC 3. Resolve `ittfid` against `silver.player_identity`
# MAGIC 4. Dedupe on the natural key, write to `silver.ranking_pairs`
# MAGIC
# MAGIC Depends on notebook 01 (`Build Player Identity Dimension`) having run
# MAGIC first in this same job -- see `03-jobs/`.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

source_table = f"{catalog_name}.{bronze_schema}.ranking_pairs"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.ranking_pairs"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze, snake_case, normalize the identity columns

# COMMAND ----------

bronze_df = spark.table(source_table)

renamed_df = (
    rename_to_snake_case(bronze_df)
        .withColumnRenamed('ittf_id1', 'partner1_ittfid')
        .withColumnRenamed('ittf_id1d', 'partner2_ittfid')
        # SubEventCode -> sub_event_code via the generic snake_case pass;
        # renamed back to subevent_code to match the rest of the project.
        .withColumnRenamed('sub_event_code', 'subevent_code')
        .withColumn('ittfid', F.col('pair_id'))
        .withColumn('entity_type', F.lit('PAIR'))
        .withColumn('is_active', F.col('is_active').cast('boolean'))
)

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Resolve `ittfid` (== pair_id) against the player identity
# MAGIC dimension

# COMMAND ----------

player_identity_df = spark.table(identity_table)

resolved_df = resolve_identity(renamed_df, 'ittfid', 'entity_type', player_identity_df)

display(resolved_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Dedupe on natural key (pair x subevent x week x run)

# COMMAND ----------

deduped_df = dedup_on_business_key(
    resolved_df,
    key_cols=['pair_id', 'subevent_code', 'ranking_year', 'ranking_week', 'ranking_run_code'],
    order_by_desc_cols=['updated_date', 'publish_date'],
)

silver_ranking_pairs_df = add_silver_metadata(deduped_df)

display(silver_ranking_pairs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to silver delta table

# COMMAND ----------

(
    silver_ranking_pairs_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())
