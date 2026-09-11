# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Players Events Results Master Log Archives
# MAGIC Source: `dbo.PlayersEventsResultsMaster_Log_Archives` (Azure SQL Server) via JDBC
# MAGIC Grain: Older prior weeks, rolled off from _log — same 31 columns as current
# MAGIC
# MAGIC Same 31-column shape again -- older prior weeks rolled off from
# MAGIC `_log`. Full overwrite for this initial/one-off load track, same as
# MAGIC every other bronze table here. **Note for the incremental production
# MAGIC track (Step 7+):** this table is naturally append-only (once a week
# MAGIC rolls off into it, it should not change again) -- re-pulling it whole
# MAGIC on every run is wasted transfer once it has real history. Step 7+
# MAGIC should replace this notebook's write step with a watermark-based
# MAGIC append (track the max RankingYear/RankingWeek already pulled, filter
# MAGIC the JDBC read to rows beyond it) instead of full overwrite. Not done
# MAGIC here because Step 2 is the initial historical load, where a full
# MAGIC pull is correct exactly once.
# MAGIC 1. Read the table via JDBC, pinned to an explicit column list
# MAGIC 2. Add Metadata Columns (source system/table, ingestion timestamp)
# MAGIC 3. Write to bronze delta table (full overwrite)
# MAGIC 4. Validate row count

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

source_table = 'PlayersEventsResultsMaster_Log_Archives'
table_name = f"{catalog_name}.{bronze_schema}.players_events_results_master_log_archives"

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

players_events_results_master_log_archives_df = read_from_sqlserver(source_table, columns)
display(players_events_results_master_log_archives_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns

# COMMAND ----------

players_events_results_master_log_archives_final_df = add_jdbc_ingestion_metadata(players_events_results_master_log_archives_df, source_table)
display(players_events_results_master_log_archives_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    players_events_results_master_log_archives_final_df
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