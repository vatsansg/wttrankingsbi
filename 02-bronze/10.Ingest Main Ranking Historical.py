# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest Main Ranking Historical (one-off backfill + progressive test)
# MAGIC Source: `dbo_MainRanking.csv` -- a full export of the source system's
# MAGIC `dbo.MainRanking` table (3,637,665 rows, RankingYear 2021 week 1 through
# MAGIC 2026 week 37, confirmed by direct inspection). This table was originally
# MAGIC scoped **out** of bronze entirely (decision 10 in the discovery doc,
# MAGIC "calculation-run-internal, not ranking-domain BI data") -- it's back in
# MAGIC scope now specifically to bootstrap 2021-2026 wk30 history that the real
# MAGIC weekly ADF landing drops never captured (those only start existing from
# MAGIC whatever week you first pointed the pipeline at).
# MAGIC
# MAGIC **Two independent uses of the same source file, controlled by
# MAGIC `p_load_mode`:**
# MAGIC 1. `bulk_upto` -- the one-off historical backfill. Loads every row with
# MAGIC    `(RankingYear, RankingWeek) <= (p_cutoff_year, p_cutoff_week)`
# MAGIC    (chronological, not a naive AND of two independent comparisons -- see
# MAGIC    Step 2 below for why that distinction matters). Intended cutoff:
# MAGIC    2026 week 30, i.e. everything before the real landing-CSV pipeline's
# MAGIC    first tested week (31).
# MAGIC 2. `single_week` -- loads exactly one `(p_ranking_year, p_ranking_week)`
# MAGIC    from the *same* file. This is how weeks 2026:32-37 get tested: since
# MAGIC    this file already contains the "known right answer" for those weeks
# MAGIC    too, running this mode against them (alongside the real weekly
# MAGIC    landing-CSV drops for the same weeks) exercises the accumulate-write
# MAGIC    mechanism end-to-end against a source you can independently verify,
# MAGIC    before trusting it against a real, only-happens-once weekly drop.
# MAGIC
# MAGIC Both modes write into the same `bronze.main_ranking_historical` table
# MAGIC via a `replaceWhere` scoped to exactly the week-range each run touches,
# MAGIC so the two modes accumulate together rather than one erasing the other.
# MAGIC
# MAGIC **This table is never read by Gold and never joins into
# MAGIC `bronze.ranking_individuals`/`bronze.ranking_pairs`.** It's a separate,
# MAGIC differently-shaped raw source (no player/pair geography, no audit
# MAGIC columns -- see `README.md`'s column-mapping table for exactly what's
# MAGIC missing and why). The merge into the *silver* `ranking_individuals`/
# MAGIC `ranking_pairs` tables -- where this data actually becomes useful to
# MAGIC Gold -- happens in the modified `03-silver/02.Silver Ranking
# MAGIC Individuals.py` / `03.Silver Ranking Pairs.py`.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

dbutils.widgets.dropdown("p_load_mode", "bulk_upto", ["bulk_upto", "single_week"])
dbutils.widgets.text("p_cutoff_year", "2026")
dbutils.widgets.text("p_cutoff_week", "30")
dbutils.widgets.text("p_ranking_year", "")
dbutils.widgets.text("p_ranking_week", "")

v_load_mode = dbutils.widgets.get("p_load_mode")
v_cutoff_year = dbutils.widgets.get("p_cutoff_year")
v_cutoff_week = dbutils.widgets.get("p_cutoff_week")
v_ranking_year = dbutils.widgets.get("p_ranking_year")
v_ranking_week = dbutils.widgets.get("p_ranking_week")

# COMMAND ----------

source_path = f"{landing_folder_path}/historical/dbo_MainRanking.csv"
print (source_path)
table_name = f"{catalog_name}.{bronze_schema}.main_ranking_historical"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read, schema-enforced, no inferSchema
# MAGIC `RankingPosAgeCategory` is blank for every SEN-category row (only
# MAGIC Youth-category rows carry a rank-within-age-category) -- declared
# MAGIC nullable IntegerType, not a required column.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType,
)

main_ranking_schema = StructType([
    StructField('MainRankingId', StringType()),
    StructField('CompetitorId', StringType()),
    StructField('RankingPos', IntegerType()),
    StructField('RankingPoints', DecimalType(10, 2)),
    StructField('RankingCategory', StringType()),
    StructField('RankingYear', IntegerType()),
    StructField('RankingMonth', IntegerType()),
    StructField('RankingWeek', IntegerType()),
    StructField('OrganizationCode', StringType()),
    StructField('CategoryCode', StringType()),
    StructField('AgeCategoryCode', StringType()),
    StructField('RankingPosAgeCategory', IntegerType()),
])

raw_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
         .option('mode', 'FAILFAST')
         .schema(main_ranking_schema)
         .load(source_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Filter to this run's scope
# MAGIC **This is the one part of this notebook where a wrong-but-plausible
# MAGIC implementation silently loses data rather than erroring.** A naive
# MAGIC `(RankingYear <= cutoff_year) & (RankingWeek <= cutoff_week)` is *not*
# MAGIC the same as chronological `<=` -- e.g. `(2022, 35)` has
# MAGIC `RankingYear(2022) <= 2026` true but `RankingWeek(35) <= 30` false, so a
# MAGIC naive AND would wrongly **exclude** it even though 2022 week 35 is
# MAGIC chronologically before 2026 week 30. Since most weeks in 2021-2025 have
# MAGIC week numbers above 30, that bug would silently drop the large majority
# MAGIC of the intended historical load with no error thrown. Built explicitly as
# MAGIC branched OR logic below instead (equivalent to comparing the
# MAGIC `(year, week)` tuple lexicographically).

# COMMAND ----------

from pyspark.sql import functions as F

if v_load_mode == "bulk_upto":
    cutoff_year_i, cutoff_week_i = int(v_cutoff_year), int(v_cutoff_week)
    scope_predicate = (
        (F.col('RankingYear') < cutoff_year_i)
        | ((F.col('RankingYear') == cutoff_year_i) & (F.col('RankingWeek') <= cutoff_week_i))
    )
    replace_predicate = f"(RankingYear < {cutoff_year_i}) OR (RankingYear = {cutoff_year_i} AND RankingWeek <= {cutoff_week_i})"
elif v_load_mode == "single_week":
    ranking_year_i, ranking_week_i = int(v_ranking_year), int(v_ranking_week)
    scope_predicate = (F.col('RankingYear') == ranking_year_i) & (F.col('RankingWeek') == ranking_week_i)
    replace_predicate = f"RankingYear = {ranking_year_i} AND RankingWeek = {ranking_week_i}"
else:
    raise ValueError(f"Unknown p_load_mode: {v_load_mode!r}")

scoped_df = raw_df.filter(scope_predicate)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2b - Independently verify the row count before writing
# MAGIC Counts the source file's rows for this scope a second, independent way
# MAGIC (a plain Python/pandas-free pass isn't available in this environment, so
# MAGIC this recomputes via a *separately expressed* predicate on the raw file
# MAGIC read, rather than trusting the same `scoped_df` object it's meant to
# MAGIC check) and fails loudly on any mismatch, rather than treating "the write
# MAGIC ran without a Spark error" as proof the filter was correct -- this
# MAGIC project has already shipped more than one bug that ran clean but produced
# MAGIC wrong data.

# COMMAND ----------

expected_count = (
    spark.read
         .format('csv')
         .option('header', 'true')
         .option('mode', 'FAILFAST')
         .schema(main_ranking_schema)
         .load(source_path)
         .filter(scope_predicate)
         .count()
)
actual_count = scoped_df.count()
if expected_count != actual_count:
    raise ValueError(
        f"Row-count mismatch for mode={v_load_mode}: expected {expected_count}, "
        f"got {actual_count} -- refusing to write."
    )
display(f"Row-count check passed: {actual_count} rows in scope for mode={v_load_mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add metadata columns and write
# MAGIC `replaceWhere` scoped to exactly this run's `(RankingYear, RankingWeek)`
# MAGIC range, so `bulk_upto` and every later `single_week` run accumulate into
# MAGIC the same table without disturbing each other's rows. Same
# MAGIC `replaceWhere.dataColumns.enabled` note as the modified `01`/`02`
# MAGIC notebooks applies here.

# COMMAND ----------

final_df = (
    scoped_df
        .withColumn('_ingestion_timestamp', F.current_timestamp())
        .withColumn('_source_file', F.lit(source_path))
        .withColumn('_load_mode', F.lit(v_load_mode))
)

spark.conf.set('spark.databricks.delta.replaceWhere.dataColumns.enabled', 'true')

(
    final_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('replaceWhere', replace_predicate)
        .option('mergeSchema', 'false')
        .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name).count())

# COMMAND ----------

display(
    spark.table(table_name)
         .groupBy('RankingYear')
         .agg(F.min('RankingWeek').alias('min_week'), F.max('RankingWeek').alias('max_week'), F.count('*').alias('rows'))
         .orderBy('RankingYear')
)