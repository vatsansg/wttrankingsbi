# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest Players Events Results Master
# MAGIC Source: `dbo.PlayersEventsResultsMaster` (Azure SQL Server) via JDBC
# MAGIC Grain: Current / live published ranking week only — one row per competitor × event × subevent
# MAGIC
# MAGIC `PlayerEventResultID` is kept for lineage/dedup only -- it is a
# MAGIC separate sequence on each of the 3 ledger tables, not a stable id
# MAGIC across the union, so Silver does not carry it forward as a join key.
# MAGIC `CompetitorId` is polymorphic: individual disciplines (MS/WS/MDI/WDI/XDI)
# MAGIC resolve against `competitors.PlayerID`; pair disciplines (MD/WD/XD)
# MAGIC resolve against `players_doubles.DoublesId` -- `SubEventCode` tells you
# MAGIC which. Silver merges CompetitorId/PlayerID/PairId into one `ittfid`.
# MAGIC 1. Read the table via JDBC, pinned to an explicit column list
# MAGIC 2. Add Metadata Columns (source system/table, ingestion timestamp)
# MAGIC 3. Write to bronze delta table (full overwrite)
# MAGIC 4. Validate row count

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

source_table = 'PlayersEventsResultsMaster'
table_name = f"{catalog_name}.{bronze_schema}.players_events_results_master"

columns = [
    "PlayerEventResultID",
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

players_events_results_master_df = read_from_sqlserver(source_table, columns)
display(players_events_results_master_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns

# COMMAND ----------

players_events_results_master_final_df = add_jdbc_ingestion_metadata(players_events_results_master_df, source_table)
display(players_events_results_master_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    players_events_results_master_final_df
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