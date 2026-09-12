# Databricks notebook source
# MAGIC %md
# MAGIC # Fact Ranking Pair
# MAGIC Source: `silver.ranking_pairs`
# MAGIC
# MAGIC Same grain as its silver source (pair x subevent x week x run). Mirrors
# MAGIC `07.Fact Ranking Individual` -- see that notebook's header for why no
# MAGIC identity lookup happens here. `is_cross_country` is NOT re-derived here
# MAGIC (it already lives on `gold.dim_pair`, a pair-level attribute, not a
# MAGIC fact-level one).
# MAGIC
# MAGIC Derived flag:
# MAGIC - `is_new_entrant` -- `previous_rank IS NULL` (dashboard 07, "Fresh
# MAGIC   Faces")
# MAGIC 1. Read `silver.ranking_pairs`
# MAGIC 2. Derive `is_new_entrant`
# MAGIC 3. Write to `gold.fact_ranking_pair`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

source_table = f"{catalog_name}.{silver_schema}.ranking_pairs"
target_table = f"{catalog_name}.{gold_schema}.fact_ranking_pair"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read silver, derive flag, narrow columns

# COMMAND ----------

silver_df = spark.table(source_table)

fact_ranking_pair_df = silver_df.select(
    F.col('ittfid'),
    F.col('subevent_code'),
    F.col('age_category_code'),
    F.col('category_code'),
    F.col('ranking_run_code'),
    F.col('ranking_year').cast('int'),
    F.col('ranking_week').cast('int'),
    F.col('current_rank'),
    F.col('previous_rank'),
    F.col('points'),
    F.col('ranking_difference'),
    F.col('ranking_position'),
    F.col('is_active'),
    F.col('identity_resolved'),
    F.col('previous_rank').isNull().alias('is_new_entrant'),
)

display(fact_ranking_pair_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Write to gold delta table

# COMMAND ----------

(
    add_gold_metadata(fact_ranking_pair_df)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())