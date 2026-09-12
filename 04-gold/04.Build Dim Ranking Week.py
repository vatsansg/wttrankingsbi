# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dim Ranking Week
# MAGIC Source: `silver.ranking_individuals` + `silver.ranking_pairs` +
# MAGIC `silver.points_ledger`
# MAGIC
# MAGIC Unlike every other gold dimension, there is no bronze/silver reference
# MAGIC table for "ranking week" -- `ranking_year`/`ranking_week` are just data
# MAGIC columns carried on every fact-shaped silver table. This dimension is a
# MAGIC `SELECT DISTINCT` across all three, not a 1:1 copy of a source table.
# MAGIC
# MAGIC **Why this table has many more rows than `ranking_individuals` /
# MAGIC `ranking_pairs` do at any moment.** Those two are one-off, full-overwrite
# MAGIC tables (this build's Steps 1-6 track) -- bronze only ever holds the
# MAGIC latest loaded week, so silver does too, so at any given time they only
# MAGIC ever contribute ONE (year, week) pair each. `points_ledger` is
# MAGIC different: it's built from 3 bronze tables that are pulled as full
# MAGIC history from the operational DB (not from a weekly landing file), so it
# MAGIC already spans every ranking week back to 2021 (confirmed in the Step 3
# MAGIC README's pair-identity-gap investigation: `_log_archives` alone
# MAGIC contributes rows across many years). So this dimension is mostly
# MAGIC populated by `points_ledger`'s history today; the individuals/pairs
# MAGIC facts will only start contributing their own weekly trail once the
# MAGIC incremental production track (Step 7+) starts appending instead of
# MAGIC overwriting.
# MAGIC 1. `SELECT DISTINCT (ranking_year, ranking_week)` from all 3 sources,
# MAGIC    union
# MAGIC 2. Derive a sortable `ranking_week_key` (`ranking_year * 100 +
# MAGIC    ranking_week`) and a display label
# MAGIC 3. Write to `gold.dim_ranking_week`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

individuals_table = f"{catalog_name}.{silver_schema}.ranking_individuals"
pairs_table = f"{catalog_name}.{silver_schema}.ranking_pairs"
ledger_table = f"{catalog_name}.{silver_schema}.points_ledger"
target_table = f"{catalog_name}.{gold_schema}.dim_ranking_week"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Distinct weeks from all 3 sources, union

# COMMAND ----------

weeks_df = (
    spark.table(individuals_table).select('ranking_year', 'ranking_week')
        .unionByName(spark.table(pairs_table).select('ranking_year', 'ranking_week'))
        .unionByName(spark.table(ledger_table).select('ranking_year', 'ranking_week'))
        .where(F.col('ranking_year').isNotNull() & F.col('ranking_week').isNotNull())
        .distinct()
)

display(weeks_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Derive a sortable key + display label

# COMMAND ----------

dim_ranking_week_df = weeks_df.select(
    (F.col('ranking_year').cast('int') * F.lit(100) + F.col('ranking_week').cast('int')).alias('ranking_week_key'),
    F.col('ranking_year').cast('int').alias('ranking_year'),
    F.col('ranking_week').cast('int').alias('ranking_week'),
    F.concat(F.col('ranking_year').cast('string'), F.lit('-W'), F.lpad(F.col('ranking_week').cast('string'), 2, '0')).alias('ranking_week_label'),
)

display(dim_ranking_week_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to gold delta table

# COMMAND ----------

(
    add_gold_metadata(dim_ranking_week_df)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())
