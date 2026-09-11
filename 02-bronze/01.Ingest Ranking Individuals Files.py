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

# DBTITLE 1,Cell 8
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

# DBTITLE 1,Cell 10
ranking_individuals_final_df = (
    ranking_individuals_df
        .withColumn('_ingestion_timestamp', F.current_timestamp())
)
display(ranking_individuals_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table
# MAGIC Full overwrite for this initial/one-off load track. The incremental
# MAGIC production track (Step 7+) switches this to a `batch_id`-partitioned
# MAGIC `replaceWhere` overwrite, same as the Formula 1 incremental variant --
# MAGIC that's why `batch_id` is already reserved as a metadata column in the
# MAGIC Step 1 design even though it is unused here.

# COMMAND ----------

(
    ranking_individuals_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

display(spark.table(table_name).count())