# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Individuals Event Penalties
# MAGIC Source: `bronze.individuals_event_penalties` -- Zero Point Penalty
# MAGIC records (today the only penalty type; see the discovery doc's
# MAGIC `ref_event_penalty_types` trim note).
# MAGIC
# MAGIC Unlike the points ledger, this table's identity is unambiguous per row
# MAGIC rather than derived from a discipline code: `ittfid` is populated when
# MAGIC the penalty applies to an individual result, `PairId` is populated when
# MAGIC it applies to a pair result -- exactly one of the two, per the
# MAGIC discovery doc's column notes.
# MAGIC 1. Read bronze, snake_case
# MAGIC 2. Derive a uniform `ittfid` (coalesce the individual and pair ids) +
# MAGIC    `entity_type`, resolve against `silver.player_identity`
# MAGIC 3. Write to `silver.individuals_event_penalties`
# MAGIC
# MAGIC Depends on notebook 01 (`Build Player Identity Dimension`) having run
# MAGIC first in this same job -- see `03-jobs/`.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

source_table = f"{catalog_name}.{bronze_schema}.individuals_event_penalties"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.individuals_event_penalties"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze, snake_case
# MAGIC The source `ittfid` column is already lowercase (matches the source
# MAGIC system's own naming, unlike every other bronze column) -- snake_case is
# MAGIC a no-op on it, but it's renamed out of the way first so the derived,
# MAGIC uniform `ittfid` column (individual OR pair id) can take that name.

# COMMAND ----------

bronze_df = spark.table(source_table)

renamed_df = (
    rename_to_snake_case(bronze_df)
        .withColumnRenamed('ittfid', 'individual_ittfid')
        .withColumnRenamed('pair_id', 'pair_ittfid')
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Derive uniform `ittfid` + `entity_type`, resolve identity

# COMMAND ----------

with_identity_cols_df = (
    renamed_df
        .withColumn('ittfid', F.coalesce(F.col('individual_ittfid').cast('string'), F.col('pair_ittfid').cast('string')))
        .withColumn(
            'entity_type',
            F.when(F.col('individual_ittfid').isNotNull(), F.lit('INDIVIDUAL'))
             .when(F.col('pair_ittfid').isNotNull(), F.lit('PAIR'))
             .otherwise(F.lit(None).cast('string')),
        )
)

player_identity_df = spark.table(identity_table)

resolved_df = resolve_identity(with_identity_cols_df, 'ittfid', 'entity_type', player_identity_df)

silver_penalties_df = add_silver_metadata(resolved_df)

display(silver_penalties_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to silver delta table

# COMMAND ----------

(
    silver_penalties_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())
