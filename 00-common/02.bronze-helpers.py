# Databricks notebook source
# MAGIC %md
# MAGIC ## 02. Bronze helpers -- file-based sources
# MAGIC Used by the 2 landing-CSV bronze notebooks only (weekly Ranking
# MAGIC Individuals / Ranking Pairs files). The JDBC-sourced tables use the
# MAGIC separate helpers in `04.jdbc-helpers` instead, since a SQL Server read has
# MAGIC no `_metadata.file_path` to stamp.

# COMMAND ----------

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    """Stamp every row with where it came from and when it landed in bronze."""
    return (
        df.withColumn('_ingestion_timestamp', F.current_timestamp())
          .withColumn('_source_file', F.col('_metadata.file_path'))
    )