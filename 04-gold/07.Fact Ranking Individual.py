# Databricks notebook source
# MAGIC %md
# MAGIC # Fact Ranking Individual
# MAGIC Source: `silver.ranking_individuals`
# MAGIC
# MAGIC Same grain as its silver source (player x subevent x week x run) --
# MAGIC gold doesn't change the grain, it adds dashboard-ready derived flags and
# MAGIC narrows to the columns dashboards actually need. Unlike Silver's
# MAGIC `resolve_identity`, no lookup happens here: `country_code`,
# MAGIC `subevent_code`, and `age_category_code` are already the correct foreign
# MAGIC keys as plain columns on the silver row -- the join to `dim_country` /
# MAGIC `dim_subevent` / `dim_age_category` happens later, at query time (see
# MAGIC `00-common/08.gold-helpers`'s note on why gold has no resolve step).
# MAGIC
# MAGIC Derived flags:
# MAGIC - `is_new_entrant` -- `previous_rank IS NULL` (dashboard 07, "Fresh
# MAGIC   Faces")
# MAGIC - `is_junior_in_senior_file` -- appears in the SEN file
# MAGIC   (`ranking_run_code = 'SEN'`) but isn't itself a senior
# MAGIC   (`age_category_code != 'SEN'`) (dashboard 04, "Youth-to-Senior
# MAGIC   Pipeline" -- the 41%-are-juniors finding)
# MAGIC 1. Read `silver.ranking_individuals`
# MAGIC 2. Derive the two flags above
# MAGIC 3. Write to `gold.fact_ranking_individual`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

source_table = f"{catalog_name}.{silver_schema}.ranking_individuals"
target_table = f"{catalog_name}.{gold_schema}.fact_ranking_individual"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read silver, derive flags, narrow columns

# COMMAND ----------

silver_df = spark.table(source_table)

fact_ranking_individual_df = silver_df.select(
    F.col('ittfid'),
    F.col('country_code'),
    F.col('subevent_code'),
    F.col('age_category_code'),
    F.col('category_code'),
    F.col('ranking_run_code'),
    F.col('ranking_year').cast('int'),
    F.col('ranking_week').cast('int'),
    F.col('current_rank'),
    F.col('previous_rank'),
    F.col('ranking_points_ytd'),
    F.col('ranking_points__previous').alias('ranking_points_previous'),
    F.col('ranking_difference'),
    F.col('ranking_position'),
    F.col('is_active'),
    F.col('identity_resolved'),
    F.col('previous_rank').isNull().alias('is_new_entrant'),
    (
        (F.col('ranking_run_code') == 'SEN') & (F.col('age_category_code') != 'SEN')
    ).alias('is_junior_in_senior_file'),
)

display(fact_ranking_individual_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Write to gold delta table
# MAGIC Full overwrite, same one-off track as bronze/silver -- see the Step 3
# MAGIC README's "Full overwrite, not merge/upsert" note. This means, today,
# MAGIC this fact only ever holds ONE ranking week's worth of rows at a time
# MAGIC (whatever `silver.ranking_individuals` currently has) -- see
# MAGIC `04.Build Dim Ranking Week`'s note on why that table's row count won't
# MAGIC match this one until the incremental production track (Step 7+).

# COMMAND ----------

(
    add_gold_metadata(fact_ranking_individual_df)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())
