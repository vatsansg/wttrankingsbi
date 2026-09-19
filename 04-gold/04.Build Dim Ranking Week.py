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
# MAGIC **Comment corrected 2026-09-19 (Step 8) -- this section was stale.** It
# MAGIC used to say `ranking_individuals`/`ranking_pairs` only ever contribute
# MAGIC ONE `(year, week)` pair each, and would only start contributing their own
# MAGIC weekly trail "once the incremental production track (Step 7+) starts
# MAGIC appending." That was true before the MainRanking historical backfill and
# MAGIC the Step 2b accumulate-write fix; it has not been true since -- both
# MAGIC silver tables already hold the full multi-year backfilled history (see
# MAGIC `09.Gold Dashboard Views.py`'s own corrected note, and the
# MAGIC `Incremental_Load_Steps_7-11_Plan.docx` review that caught this comment
# MAGIC still being stale here and in `07.Fact Ranking Individual.py`). This
# MAGIC dimension is populated by all three sources' full history today, not
# MAGIC mostly by `points_ledger` alone -- **do not use "row count > 0 here"
# MAGIC as a freshness signal for whether this week's data landed**, since the
# MAGIC table is always non-empty regardless of whether the latest week made it
# MAGIC in. The Step 8 target-week-landed assertions in `10.Validate All Gold
# MAGIC Tables.py` (and bronze/silver's equivalents) are what actually check
# MAGIC that.
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