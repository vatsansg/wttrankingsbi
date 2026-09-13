# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest weekly Ranking Pairs files
# MAGIC Source: `RankingPairs_SEN_<yyyy>_<ww>.csv` +
# MAGIC `RankingPairs_YOU_<yyyy>_<ww>.csv` (landing Volume, weekly folder)
# MAGIC 1. Read both files for the week using the dataframe reader API
# MAGIC 2. Tag each file's rows with `ranking_run_code` (SEN / YOU), parsed from the
# MAGIC    filename
# MAGIC 3. Add Metadata Columns
# MAGIC     - Source File
# MAGIC     - Ingestion Timestamp
# MAGIC 4. Write to bronze delta table
# MAGIC
# MAGIC **Change (2026-09-12, MainRanking historical backfill exercise):** same
# MAGIC accumulate-write fix as `01.Ingest Ranking Individuals Files` -- see that
# MAGIC notebook's header and `README.md`'s "Accumulate-write fix" section for
# MAGIC the full rationale. Was a full `mode('overwrite')` of the whole table
# MAGIC every run; now a `replaceWhere`-scoped overwrite of just the
# MAGIC `(RankingYear, RankingWeek)` this run loads.

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
table_name = f"{catalog_name}.{bronze_schema}.ranking_pairs"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the SEN and YOU CSV files using the dataframe reader API
# MAGIC Note `Points` here vs. `RankingPointsYTD` on the individuals file -- the
# MAGIC source system names the equivalent column differently between the two
# MAGIC exports; both are carried through bronze as-is and reconciled in Silver.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType,
    DateType, TimestampType, BooleanType,
)

ranking_pairs_schema = StructType([
    StructField('PairId', StringType()),
    StructField('IttfId1', StringType()),
    StructField('PlayerName1', StringType()),
    StructField('CountryCode1', StringType()),
    StructField('CountryName1', StringType()),
    StructField('AssociationCountryCode1', StringType()),
    StructField('AssociationCountryName1', StringType()),
    StructField('NationalityCode1', StringType()),
    StructField('NationalityName1', StringType()),
    StructField('ContinentId1', StringType()),
    StructField('IttfId1d', StringType()),
    StructField('PlayerName1d', StringType()),
    StructField('CountryCode1d', StringType()),
    StructField('CountryName1d', StringType()),
    StructField('AssociationCountryCode1d', StringType()),
    StructField('AssociationCountryName1d', StringType()),
    StructField('NationalityCode1d', StringType()),
    StructField('NationalityName1d', StringType()),
    StructField('ContinentId1d', StringType()),
    StructField('CategoryCode', StringType()),
    StructField('AgeCategoryCode', StringType()),
    StructField('SubEventCode', StringType()),
    StructField('RankingYear', IntegerType()),
    StructField('RankingMonth', IntegerType()),
    StructField('RankingWeek', IntegerType()),
    StructField('Points', DecimalType(10, 2)),
    StructField('RankingPosition', IntegerType()),
    StructField('CurrentRank', IntegerType()),
    StructField('PreviousRank', IntegerType()),
    StructField('RankingDifference', IntegerType()),
    StructField('PublishDate', TimestampType()),
    StructField('UpdatedDate', TimestampType()),
    StructField('UpdatedBy', StringType()),
    StructField('IsActive', IntegerType()),
])

# COMMAND ----------

pairs_sen_df = add_ingestion_metadata(
    spark.read
         .format('csv')
         .option('header', 'true')
         .option('mode', 'FAILFAST')
         .schema(ranking_pairs_schema)
         .load(f"{source_folder}/RankingPairs_SEN_{v_ranking_year}_{v_ranking_week}.csv")
         .withColumn('ranking_run_code', F.lit('SEN'))
)

pairs_you_df = add_ingestion_metadata(
    spark.read
         .format('csv')
         .option('header', 'true')
         .option('mode', 'FAILFAST')
         .schema(ranking_pairs_schema)
         .load(f"{source_folder}/RankingPairs_YOU_{v_ranking_year}_{v_ranking_week}.csv")
         .withColumn('ranking_run_code', F.lit('YOU'))
)

ranking_pairs_df = pairs_sen_df.unionByName(pairs_you_df)

display(ranking_pairs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns
# MAGIC - Source File
# MAGIC - Ingestion Timestamp

# COMMAND ----------

ranking_pairs_final_df = ranking_pairs_df
display(ranking_pairs_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2b - Guard against a stray/duplicate week in the incoming file
# MAGIC Same guard as `01.Ingest Ranking Individuals Files` -- the `replaceWhere`
# MAGIC below assumes the file(s) just read contain exactly one
# MAGIC `(RankingYear, RankingWeek)` pair, and that it matches the widgets.

# COMMAND ----------

distinct_weeks = (
    ranking_pairs_final_df
        .select('RankingYear', 'RankingWeek')
        .distinct()
        .collect()
)
if len(distinct_weeks) != 1:
    raise ValueError(
        f"Expected exactly one (RankingYear, RankingWeek) in the loaded file(s), "
        f"found {len(distinct_weeks)}: {distinct_weeks}. Refusing to write."
    )
loaded_year, loaded_week = distinct_weeks[0]['RankingYear'], distinct_weeks[0]['RankingWeek']
if str(loaded_year) != str(v_ranking_year) or str(loaded_week) != str(v_ranking_week):
    raise ValueError(
        f"File contents ({loaded_year}, {loaded_week}) don't match the widget "
        f"parameters ({v_ranking_year}, {v_ranking_week})."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table
# MAGIC Accumulate via `replaceWhere`, same rationale as `01.Ingest Ranking
# MAGIC Individuals Files` -- see that notebook / `README.md` for detail.

# COMMAND ----------

spark.conf.set('spark.databricks.delta.replaceWhere.dataColumns.enabled', 'true')

replace_predicate = f"RankingYear = {int(v_ranking_year)} AND RankingWeek = {int(v_ranking_week)}"

(
    ranking_pairs_final_df
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
         .groupBy('RankingYear', 'RankingWeek')
         .count()
         .orderBy('RankingYear', 'RankingWeek')
)
