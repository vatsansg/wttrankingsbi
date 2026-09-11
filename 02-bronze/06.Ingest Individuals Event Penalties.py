# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Individuals Event Penalties
# MAGIC Source: `dbo.Individuals_EventPenalties` (Azure SQL Server) via JDBC
# MAGIC Grain: One row per Zero Point Penalty (or future penalty type) applied to a player/pair result
# MAGIC
# MAGIC `PenaltyTypeId` is carried as a plain value, not a resolved FK -- its
# MAGIC source lookup (`dbo.EventPenaltyTypes`) is a single row (Zero Point
# MAGIC Penalty only) and was dropped as its own bronze table on review.
# MAGIC `PairId` is set only when the penalty applies to a pair result.
# MAGIC 1. Read the table via JDBC, pinned to an explicit column list
# MAGIC 2. Add Metadata Columns (source system/table, ingestion timestamp)
# MAGIC 3. Write to bronze delta table (full overwrite)
# MAGIC 4. Validate row count

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

source_table = 'Individuals_EventPenalties'
table_name = f"{catalog_name}.{bronze_schema}.individuals_event_penalties"

columns = [
    "Individuals_EventPenaltyId",
    "ittfid",
    "EventId",
    "CategoryCode",
    "AgeCategoryCode",
    "SubEventId",
    "RankingCategoryCode",
    "PenaltyWeekExpiry",
    "PenaltyMonthExpiry",
    "PenaltyYearExpiry",
    "PenaltyExpiryDate",
    "CreatedDateTime",
    "LastUpdatedDateTime",
    "PenaltyTypeId",
    "IsDirectPenalty",
    "Active",
    "Comments",
    "PairId",
    "PairCombination",
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read from Azure SQL Server via JDBC

# COMMAND ----------

individuals_event_penalties_df = read_from_sqlserver(source_table, columns)
display(individuals_event_penalties_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns

# COMMAND ----------

individuals_event_penalties_final_df = add_jdbc_ingestion_metadata(individuals_event_penalties_df, source_table)
display(individuals_event_penalties_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    individuals_event_penalties_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Validate

# COMMAND ----------

validate_bronze_table(table_name)