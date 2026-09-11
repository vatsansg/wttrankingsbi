# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Players Events Results Master Log
# MAGIC Source: `dbo.PlayersEventsResultsMaster_Log` (Azure SQL Server) via JDBC
# MAGIC Grain: Recent prior weeks, mutually exclusive with _log_archives — same 31 columns as current
# MAGIC
# MAGIC Same 31-column shape as the current-week master table (confirmed by
# MAGIC WTT to be mutually exclusive by ranking week) -- only the surrogate
# MAGIC key column name differs (`PlayersEventsResultsMaster_LogId`).
# MAGIC 1. Read the table via JDBC, pinned to an explicit column list
# MAGIC 2. Add Metadata Columns (source system/table, ingestion timestamp)
# MAGIC 3. Write to bronze delta table (full overwrite)
# MAGIC 4. Validate row count

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

source_table = 'PlayersEventsResultsMaster_Log'
table_name = f"{catalog_name}.{bronze_schema}.players_events_results_master_log"

columns = [
    "PlayersEventsResultsMaster_LogId",
    "CompetitorId",
    "EventId",
    "SubEventCode",
    "RankingCategoryCode",
    "ResultPosition",
    "RankingPoints",
    "RankingYear",
    "RankingMonth",
    "RankingWeek",
    "ExpiryYear",
    "ExpiryMonth",
    "ExpiryWeek",
    "PlayerBestRankingResultNumber",
    "Active",
    "MatchesPlayed",
    "MatchesWon",
    "MatchesLost",
    "Qualifier",
    "ResultType",
    "ZeroPointPenalty",
    "LastPhaseWin",
    "LastPhaseWinWithoutBye",
    "MandatoryInclusionforBestResults",
    "ExcludedDuetoZeroPointPenalty",
    "AgeCategoryCode",
    "CategoryCode",
    "OrganizationCode",
    "BestResultNoSENYOU",
    "IsImported",
    "ResultCategory",
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read from Azure SQL Server via JDBC

# COMMAND ----------

players_events_results_master_log_df = read_from_sqlserver(source_table, columns)
display(players_events_results_master_log_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns

# COMMAND ----------

players_events_results_master_log_final_df = add_jdbc_ingestion_metadata(players_events_results_master_log_df, source_table)
display(players_events_results_master_log_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    players_events_results_master_log_final_df
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