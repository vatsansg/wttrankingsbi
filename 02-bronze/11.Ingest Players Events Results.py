# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Ingest Players Events Results (consolidated) — Phase 12
# MAGIC Target: **`bronze.players_events_results`**. This is the **one** Databricks table for every player/pair event result.
# MAGIC It replaces the three per-source bronze tables (`players_events_results_master`, `_log`, `_log_archives`),
# MAGIC which are retired in Phase 12; see `01-setup/Phase12 Cutover Checks and Retire Legacy Ledger.sql`.
# MAGIC
# MAGIC | Mode (`p_load_mode`) | When | Source (Azure SQL `dbtabletennisuniverse_ranking`) | Write |
# MAGIC |---|---|---|---|
# MAGIC | `initial` | **Once**, by hand, at cut-over | `dbo.PlayersEventsResultsMaster_Log` ∪ `dbo.PlayersEventsResultsMaster_Log_Archives` | full overwrite |
# MAGIC | `weekly` (default; the job uses this) | Every new ranking week | **Target week** (the job's detected week) from `dbo.PlayersEventsResultsMaster`. If Master has already rolled past it, the target week is read from `dbo.PlayersEventsResultsMaster_Log`, which also holds the current week. If neither has it, the task **fails**, so a week can never be skipped silently. Best-N (`PlayerBestRankingResultNumber`) is final when the week lands (rankings are calculated and published first), so each week is loaded once; validator 09 **fails** a week with 0% counted | `replaceWhere` on that week (idempotent) |
# MAGIC
# MAGIC In SQL, `_Log` holds every week, including the current one, except weeks already moved to `_Log_Archives`.
# MAGIC The archive move is manual and on demand. The two tables are mutually exclusive by week. If a week ever
# MAGIC appears in both, the `_Log` copy wins; the guard below drops the archive copy for that week.
# MAGIC Later archive moves in SQL don't affect this table, because both sources were loaded once and every new week arrives
# MAGIC from `PlayersEventsResultsMaster`. `_Log` and `_Log_Archives` are never read again after the initial load.
# MAGIC
# MAGIC Bronze stays source-shaped: the original column names are kept, and each source's surrogate key
# MAGIC (`PlayerEventResultID` or `PlayersEventsResultsMaster_LogId`) is aligned to `source_result_id` for lineage only.
# MAGIC Silver (`03-silver/08`) does all conforming.
# MAGIC
# MAGIC Sets taskValue **`loaded_week_keys`** (for example `"202641"`) so the silver task refreshes only those weeks.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

dbutils.widgets.dropdown("p_load_mode", "weekly", ["weekly", "initial"])

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("p_ranking_year", "")
dbutils.widgets.text("p_ranking_week", "")
v_load_mode = dbutils.widgets.get("p_load_mode")
v_ranking_year = dbutils.widgets.get("p_ranking_year")
v_ranking_week = dbutils.widgets.get("p_ranking_week")

table_name = f"{catalog_name}.{bronze_schema}.players_events_results"

# The 30 business columns common to all three SQL tables (same order as the source DDL).
BUSINESS_COLUMNS = [
    "CompetitorId", "EventId", "SubEventCode", "RankingCategoryCode", "ResultPosition", "RankingPoints",
    "RankingYear", "RankingMonth", "RankingWeek", "ExpiryYear", "ExpiryMonth", "ExpiryWeek",
    "PlayerBestRankingResultNumber", "Active", "MatchesPlayed", "MatchesWon", "MatchesLost", "Qualifier",
    "ResultType", "ZeroPointPenalty", "LastPhaseWin", "LastPhaseWinWithoutBye",
    "MandatoryInclusionforBestResults", "ExcludedDuetoZeroPointPenalty", "AgeCategoryCode", "CategoryCode",
    "OrganizationCode", "BestResultNoSENYOU", "IsImported", "ResultCategory",
]

SOURCES = {
    # source table -> its surrogate-key column
    "PlayersEventsResultsMaster": "PlayerEventResultID",
    "PlayersEventsResultsMaster_Log": "PlayersEventsResultsMaster_LogId",
    "PlayersEventsResultsMaster_Log_Archives": "PlayersEventsResultsMaster_LogId",
}

print(f"INFO  mode={v_load_mode}  target={table_name}  orchestrator week=({v_ranking_year}, {v_ranking_week})")

# COMMAND ----------

def read_results_source(source_table):
    """Read one SQL results table (explicit column pin), align its surrogate key, and stamp lineage."""
    key_col = SOURCES[source_table]
    df = read_from_sqlserver(source_table, [key_col] + BUSINESS_COLUMNS)
    df = df.withColumn(key_col, F.col(key_col).cast("bigint")).withColumnRenamed(key_col, "source_result_id")
    return add_jdbc_ingestion_metadata(df, source_table).select(
        "source_result_id", *BUSINESS_COLUMNS, "_ingestion_timestamp", "_source_system", "_source_table"
    )


def week_predicate(weeks):
    return " OR ".join(f"(RankingYear = {int(y)} AND RankingWeek = {int(w)})" for y, w in weeks)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initial load (one-off): `_Log` ∪ `_Log_Archives`, full overwrite

# COMMAND ----------

if v_load_mode == "initial":
    from pyspark import StorageLevel
    # Persist each JDBC read once: the overlap check, the anti-join and the write would otherwise each re-read SQL.
    log_df = read_results_source("PlayersEventsResultsMaster_Log").persist(StorageLevel.DISK_ONLY)
    arch_df = read_results_source("PlayersEventsResultsMaster_Log_Archives").persist(StorageLevel.DISK_ONLY)

    # Guard: the two sources should be mutually exclusive by week. If a week is in both, keep _Log.
    log_weeks = log_df.select("RankingYear", "RankingWeek").distinct()
    overlap = arch_df.select("RankingYear", "RankingWeek").distinct().join(log_weeks, ["RankingYear", "RankingWeek"]).count()
    if overlap:
        print(f"WARN  {overlap} week(s) are present in both _Log and _Log_Archives -- keeping the _Log copy for those weeks.")
    arch_df = arch_df.join(log_weeks, ["RankingYear", "RankingWeek"], "left_anti")

    all_df = log_df.unionByName(arch_df)
    (
        all_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("RankingYear")
            .saveAsTable(table_name)
    )
    loaded_weeks = [(r["RankingYear"], r["RankingWeek"]) for r in
                    spark.table(table_name).select("RankingYear", "RankingWeek").distinct().collect()]
    print(f"OK    initial load written: {len(loaded_weeks)} week(s), "
          f"{spark.table(table_name).count():,} rows")
    log_df.unpersist(); arch_df.unpersist()
    # An empty key list tells silver to do a FULL rebuild.
    dbutils.jobs.taskValues.set(key="loaded_week_keys", value="")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Weekly load: current week from `PlayersEventsResultsMaster` → `replaceWhere` that week

# COMMAND ----------

if v_load_mode == "weekly":
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"{table_name} does not exist yet -- run this notebook once with p_load_mode=initial first.")

    master_df = read_results_source("PlayersEventsResultsMaster").cache()
    master_weeks = sorted((r["RankingYear"], r["RankingWeek"]) for r in
                          master_df.select("RankingYear", "RankingWeek").distinct().collect())
    print(f"INFO  dbo.PlayersEventsResultsMaster holds week(s) {master_weeks}")

    parts = []          # (week, dataframe, source label)
    if v_ranking_year and v_ranking_week:
        target = (int(v_ranking_year), int(v_ranking_week))
        if target in master_weeks:
            parts.append((target, master_df.where((F.col("RankingYear") == target[0]) & (F.col("RankingWeek") == target[1])),
                          "PlayersEventsResultsMaster"))
        else:
            # Master has rolled on (backlog / re-run / SQL lag): _Log holds every non-archived week incl. the current one.
            log_week_df = (read_results_source("PlayersEventsResultsMaster_Log")
                             .where((F.col("RankingYear") == target[0]) & (F.col("RankingWeek") == target[1])).cache())
            if log_week_df.limit(1).count() == 0:
                raise ValueError(f"Target week {target} is in neither dbo.PlayersEventsResultsMaster ({master_weeks}) nor "
                                 f"dbo.PlayersEventsResultsMaster_Log -- refusing to continue (the week would be lost). "
                                 f"If it was archived, load it from _Log_Archives by hand.")
            print(f"WARN  target week {target} is not the SQL current week {master_weeks} -- loading it from _Log instead.")
            parts.append((target, log_week_df, "PlayersEventsResultsMaster_Log"))
    else:
        # Manual run without an orchestrator week: load whatever Master holds.
        if not master_weeks:
            raise ValueError("dbo.PlayersEventsResultsMaster returned 0 rows -- refusing to replace any week with nothing.")
        for wk in master_weeks:
            parts.append((wk, master_df.where((F.col("RankingYear") == wk[0]) & (F.col("RankingWeek") == wk[1])),
                          "PlayersEventsResultsMaster"))

    loaded = []
    for wk, df, label in sorted(parts, key=lambda p: p[0]):
        n = df.count()
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", week_predicate([wk])).saveAsTable(table_name))
        counted_pct = (df.where(F.col("PlayerBestRankingResultNumber") != 0).count() * 100.0 / n) if n else 0.0
        print(f"OK    replaced week {wk} from dbo.{label}: {n:,} rows ({counted_pct:.1f}% flagged best-N counted)")
        loaded.append(wk[0] * 100 + wk[1])
    dbutils.jobs.taskValues.set(key="loaded_week_keys", value=",".join(str(k) for k in sorted(loaded)))
    master_df.unpersist()

# COMMAND ----------

validate_bronze_table(table_name)
display(
    spark.table(table_name).groupBy("_source_table").agg(
        F.count("*").alias("rows"),
        F.min(F.col("RankingYear") * 100 + F.col("RankingWeek")).alias("first_week_key"),
        F.max(F.col("RankingYear") * 100 + F.col("RankingWeek")).alias("last_week_key"),
    )
)