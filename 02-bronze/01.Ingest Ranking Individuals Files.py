# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest weekly Ranking Individuals files
# MAGIC Source: `RankingIndividuals_SEN_<yyyy>_<ww>.csv` +
# MAGIC `RankingIndividuals_YOU_<yyyy>_<ww>.csv` (landing Volume, weekly folder)
# MAGIC 1. Read both files for the week using the dataframe reader API
# MAGIC 2. Tag each file's rows with `ranking_run_code` (SEN / YOU), parsed from the
# MAGIC    filename -- a player can appear in both, so they are unioned into one
# MAGIC    bronze table rather than split into two
# MAGIC 3. Add Metadata Columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 4. Write to bronze delta table
# MAGIC
# MAGIC **Change (2026-09-12, MainRanking historical backfill exercise):** this
# MAGIC notebook used to do a full `mode('overwrite')` of the *entire*
# MAGIC `bronze.ranking_individuals` table every run -- fine for a single-week
# MAGIC test, but it meant loading week 32 silently erased week 31. It now writes
# MAGIC with `option('replaceWhere', ...)` scoped to just the
# MAGIC `(RankingYear, RankingWeek)` pair this run is loading, so every week's
# MAGIC data accumulates in the table instead of replacing the last one. This is
# MAGIC a permanent change to production write semantics, not part of the
# MAGIC one-off MainRanking exercise -- see `README.md`'s "Accumulate-write fix"
# MAGIC section for the rationale, the explicit conf flag it depends on, and how
# MAGIC it was verified.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

dbutils.widgets.text("p_ranking_year", "")
dbutils.widgets.text("p_ranking_week", "")
v_ranking_year = dbutils.widgets.get("p_ranking_year")
v_ranking_week = dbutils.widgets.get("p_ranking_week")

# COMMAND ----------

source_folder = f"{landing_folder_path}/{v_ranking_year} - {v_ranking_week}"
table_name = f"{catalog_name}.{bronze_schema}.ranking_individuals"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the SEN and YOU CSV files using the dataframe reader API
# MAGIC Schema is enforced explicitly (never `inferSchema`) and columns that are
# MAGIC blank in the current export (e.g. `RankingPointsCareer`, reserved by the
# MAGIC source system) are still declared, so a future populated export does not
# MAGIC require a schema change here.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType,
    DateType, TimestampType, BooleanType,
)

ranking_individuals_schema = StructType([
    StructField('IttfId', StringType()),
    StructField('PlayerName', StringType()),
    StructField('CountryCode', StringType()),
    StructField('CountryName', StringType()),
    StructField('AssociationCountryCode', StringType()),
    StructField('AssociationCountryName', StringType()),
    StructField('NationalityCode', StringType()),
    StructField('NationalityName', StringType()),
    StructField('ContinentId', StringType()),
    StructField('CategoryCode', StringType()),
    StructField('AgeCategoryCode', StringType()),
    StructField('SubEventCode', StringType()),
    StructField('RankingYear', IntegerType()),
    StructField('RankingMonth', IntegerType()),
    StructField('RankingWeek', IntegerType()),
    StructField('RankingPointsCareer', DecimalType(10, 2)),
    StructField('RankingPointsYTD', DecimalType(10, 2)),
    StructField('RankingPosition', IntegerType()),
    StructField('CurrentRank', IntegerType()),
    StructField('PreviousRank', IntegerType()),
    StructField('RankingPoints_Previous', DecimalType(10, 2)),
    StructField('RankingDifference', IntegerType()),
    StructField('PublishDate', TimestampType()),
    StructField('UpdatedDate', TimestampType()),
    StructField('UpdatedBy', StringType()),
    StructField('IsActive', IntegerType()),
])

# COMMAND ----------

individuals_sen_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
         .option('mode', 'FAILFAST')
         .schema(ranking_individuals_schema)
         .load(f"{source_folder}/RankingIndividuals_SEN_{v_ranking_year}_{v_ranking_week}.csv")
         .withColumn('ranking_run_code', F.lit('SEN'))
         .withColumn('_source_file', F.col('_metadata.file_path'))
)

individuals_you_df = (
    spark.read
         .format('csv')
         .option('header', 'true')
         .option('mode', 'FAILFAST')
         .schema(ranking_individuals_schema)
         .load(f"{source_folder}/RankingIndividuals_YOU_{v_ranking_year}_{v_ranking_week}.csv")
         .withColumn('ranking_run_code', F.lit('YOU'))
         .withColumn('_source_file', F.col('_metadata.file_path'))
)

ranking_individuals_df = individuals_sen_df.unionByName(individuals_you_df)

display(ranking_individuals_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns
# MAGIC - Source File
# MAGIC - Ingestion Timestamp

# COMMAND ----------

ranking_individuals_final_df = (
    ranking_individuals_df
        .withColumn('_ingestion_timestamp', F.current_timestamp())
)
display(ranking_individuals_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2b - Guard against a stray/duplicate week in the incoming file
# MAGIC The whole point of the partitioned `replaceWhere` below is "this run only
# MAGIC touches the one week it read" -- that guarantee only holds if the file we
# MAGIC just read actually contains exactly the one `(RankingYear, RankingWeek)`
# MAGIC pair the widgets asked for. Fail loudly rather than silently replacing
# MAGIC the wrong partition (or a wider one than intended) if it doesn't.

# COMMAND ----------

distinct_weeks = (
    ranking_individuals_final_df
        .select('RankingYear', 'RankingWeek')
        .distinct()
        .collect()
)
if len(distinct_weeks) != 1:
    raise ValueError(
        f"Expected exactly one (RankingYear, RankingWeek) in the loaded file(s), "
        f"found {len(distinct_weeks)}: {distinct_weeks}. Refusing to write -- "
        f"the replaceWhere predicate below assumes a single week."
    )
loaded_year, loaded_week = distinct_weeks[0]['RankingYear'], distinct_weeks[0]['RankingWeek']
if str(loaded_year) != str(v_ranking_year) or str(loaded_week) != str(v_ranking_week):
    raise ValueError(
        f"File contents ({loaded_year}, {loaded_week}) don't match the widget "
        f"parameters ({v_ranking_year}, {v_ranking_week}) -- check the folder/"
        f"filenames before re-running."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table
# MAGIC **Accumulate, don't overwrite.** Every week's write is scoped to just
# MAGIC that week's `(RankingYear, RankingWeek)` partition via `replaceWhere` --
# MAGIC re-running the same week is still safe/idempotent (it just replaces that
# MAGIC one week again), but a *different* week no longer erases the ones already
# MAGIC there. `bronze.ranking_individuals` is not physically partitioned by
# MAGIC these columns (row volume here -- a few thousand rows/week -- doesn't
# MAGIC justify it); `replaceWhere` on an arbitrary (non-partition) column
# MAGIC requires `spark.databricks.delta.replaceWhere.dataColumns.enabled`, set
# MAGIC explicitly below rather than assumed, since this project has already hit
# MAGIC more than one "looked right, silently wasn't" config-default bug (the
# MAGIC JDBC `databaseName` property being the first). On the very first run
# MAGIC ever (table doesn't exist yet), `replaceWhere` has nothing to replace and
# MAGIC behaves like a plain create.

# COMMAND ----------

spark.conf.set('spark.databricks.delta.replaceWhere.dataColumns.enabled', 'true')

replace_predicate = f"RankingYear = {int(v_ranking_year)} AND RankingWeek = {int(v_ranking_week)}"

(
    ranking_individuals_final_df
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

# MAGIC %md
# MAGIC #### Step 4 - Confirm this week landed and no other week was disturbed

# COMMAND ----------

display(
    spark.table(table_name)
         .groupBy('RankingYear', 'RankingWeek')
         .count()
         .orderBy('RankingYear', 'RankingWeek')
)
