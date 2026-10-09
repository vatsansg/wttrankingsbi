# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Ingest Events Metadata — Phase 12
# MAGIC Source: `dbo.EventsMetadata` in Azure SQL (`dbtabletennisuniverse_ranking`, read over the existing JDBC connection).
# MAGIC Target: **`bronze.events_metadata`**. Events come from two Azure SQL tables, and nothing else (no `EventTypes`,
# MAGIC `EventTypeGeneral`, subevent tables or mappings):
# MAGIC - `dbo.Events` → `bronze.events` (`02-bronze/13`): one row per event; name, dates, ranking year/month/week, rounds,
# MAGIC   sanctioned. The main event source.
# MAGIC - `dbo.EventsMetadata` → **this table**: several rows per event, one per `EventId` + `RankingCategoryCode` +
# MAGIC   `ForRankingCategoryCode`; tier (`EventTypeGeneralCode`/`Desc`), organisation (`OrganizationCode`), draw-size type,
# MAGIC   ranking category description and SEN/YOU category (`EventCategoryCodeforRankingCalculations`). All 16 DDL columns
# MAGIC   are copied as-is; silver uses only the ones above (event name and dates come from `bronze.events`).
# MAGIC
# MAGIC The Ranking team is still back-filling history. When the complete table is ready, re-run this notebook once with
# MAGIC `p_load_mode=initial` (a full reload), then let the weekly refresh carry on.
# MAGIC
# MAGIC | Mode (`p_load_mode`) | When | What it does |
# MAGIC |---|---|---|
# MAGIC | `initial` | **Once**, by hand | Full copy of `dbo.EventsMetadata`, full overwrite |
# MAGIC | `weekly` (default; the job uses this) | Every new ranking week | Reads `Events_<RankingYear>_<RankingWeek>.csv` from the week's landing folder (`<year> - <week>/`). For the EventIds it lists, **deletes** those EventIds' rows and **inserts** them fresh from `dbo.EventsMetadata` in one atomic `replaceWhere` write |
# MAGIC
# MAGIC **Why this refresh happens at Bronze, not Silver.** "Make our copy of these EventIds match the source" is a
# MAGIC source-synchronisation rule: it is about *which source rows we hold*, not about cleaning them. Bronze is the layer
# MAGIC that mirrors source state, so the delete-and-insert by EventId lives here. Silver
# MAGIC (`03-silver/09.Silver Events Metadata`) then rebuilds its conformed copy from bronze: trimming, typing, de-duplicating
# MAGIC and deriving event-level attributes. A Silver-side refresh would mean either re-reading SQL from silver (breaking the
# MAGIC medallion contract) or keeping two copies of the same sync logic.
# MAGIC
# MAGIC **Weekly rule (exactly as specified by WTT):** for every EventId listed in the file, delete its rows if they exist,
# MAGIC then insert that EventId's rows from `dbo.EventsMetadata`. Both happen in one atomic `replaceWhere` commit, so a
# MAGIC failure leaves the old rows untouched. If SQL returns no rows for a listed EventId, its rows end up deleted, and a
# MAGIC WARN names those EventIds.
# MAGIC
# MAGIC **Other rules in the weekly mode:**
# MAGIC - The file is read by the shared `read_weekly_events_file()` (`00-common/10.events-helpers`), the same one
# MAGIC   `02-bronze/13` uses: a missing file is a WARN before 2026 wk41 and a **failure** from wk41
# MAGIC   (`p_require_events_file=auto`; `true`/`false` force either); a missing `dbo.Events` column or a non-numeric `EventId`
# MAGIC   fails; a UTF-8 BOM is stripped.
# MAGIC - **Only the file's `EventId` column is used here**: the metadata rows always come from `dbo.EventsMetadata`.
# MAGIC   (`02-bronze/13` upserts the file's event rows into `bronze.events`.)

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

# MAGIC %run ../00-common/10.events-helpers

# COMMAND ----------

dbutils.widgets.dropdown("p_load_mode", "weekly", ["weekly", "initial"])

# COMMAND ----------

from pyspark.sql import functions as F


dbutils.widgets.text("p_ranking_year", "")
dbutils.widgets.text("p_ranking_week", "")
dbutils.widgets.dropdown("p_require_events_file", "auto", ["auto", "true", "false"])
v_load_mode = dbutils.widgets.get("p_load_mode")
v_ranking_year = dbutils.widgets.get("p_ranking_year")
v_ranking_week = dbutils.widgets.get("p_ranking_week")
v_require_mode = dbutils.widgets.get("p_require_events_file")

source_table = "EventsMetadata"
table_name = f"{catalog_name}.{bronze_schema}.events_metadata"

COLUMNS = [
    "EventId", "EventName", "StartDate", "EndDate", "RankingYear", "RankingMonth", "RankingWeek",
    "EventTypeGeneralCode", "EventTypeGeneralDesc", "EventTypeCodewithDrawsize", "EventTypewithDrawsizeDesc",
    "ForRankingCategoryCode", "RankingCategoryCode", "RankingCategoryDesc",
    "EventCategoryCodeforRankingCalculations", "OrganizationCode",
]


def read_events_metadata(event_ids=None):
    """Read dbo.EventsMetadata over JDBC (shared helper, explicit column pin). With event_ids, filter to those
    events. Spark's JDBC source pushes an `isin` filter down to SQL Server as `WHERE EventId IN (...)`, so only
    the listed events cross the wire."""
    df = read_from_sqlserver(source_table, COLUMNS)
    if event_ids is None:
        return df
    return df.where(F.col("EventId").isin([int(e) for e in event_ids]))


def stamp(df, events_file):
    return (add_jdbc_ingestion_metadata(df, source_table)
              .withColumn("_events_file", F.lit(events_file))
              .withColumn("_refresh_ranking_year", F.lit(int(v_ranking_year) if v_ranking_year else None).cast("int"))
              .withColumn("_refresh_ranking_week", F.lit(int(v_ranking_week) if v_ranking_week else None).cast("int")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initial load (one-off): full copy

# COMMAND ----------

if v_load_mode == "initial":
    full_df = stamp(read_events_metadata(), "initial-full-load")
    (full_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name))
    print(f"OK    initial load: {spark.table(table_name).count():,} rows, "
          f"{spark.table(table_name).select('EventId').distinct().count():,} events")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Weekly refresh: delete + insert the EventIds listed in `Events_<year>_<week>.csv`

# COMMAND ----------

if v_load_mode == "weekly":
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"{table_name} does not exist yet -- run this notebook once with p_load_mode=initial first.")
    if not (v_ranking_year and v_ranking_week):
        raise ValueError("weekly mode needs p_ranking_year and p_ranking_week (the job passes them from detect_new_week).")

    # File reading, the missing-file rule and the EventId checks are shared with 02-bronze/13 (00-common/10).
    events_file_name, file_rows = read_weekly_events_file(v_ranking_year, v_ranking_week, v_require_mode)
    listed_ids = None if file_rows is None else sorted(
        {int(r[0]) for r in file_rows.select("EventId").distinct().collect()})

    if listed_ids is not None:
        print(f"INFO  {events_file_name}: {len(listed_ids)} EventId(s) listed")
        if listed_ids:
            src_df = read_events_metadata(listed_ids).cache()
            found_ids = sorted(r["EventId"] for r in src_df.select("EventId").distinct().collect())
            not_in_sql = sorted(set(listed_ids) - set(found_ids))
            # Delete every listed EventId's rows and insert what SQL returns, in one atomic commit.
            predicate = f"EventId IN ({', '.join(str(i) for i in listed_ids)})"
            before = spark.table(table_name).where(predicate).count()
            (stamp(src_df, events_file_name).write.format("delta").mode("overwrite")
                .option("replaceWhere", predicate).saveAsTable(table_name))
            print(f"OK    {len(listed_ids)} listed event(s): {before:,} row(s) deleted, {src_df.count():,} row(s) inserted "
                  f"from dbo.EventsMetadata ({len(found_ids)} event(s))")
            if not_in_sql:
                print(f"WARN  {len(not_in_sql)} listed EventId(s) returned no rows from dbo.EventsMetadata, so they now have "
                      f"no metadata rows: {not_in_sql}")
            src_df.unpersist()

# COMMAND ----------

validate_bronze_table(table_name)
display(spark.table(table_name).groupBy("_events_file").agg(F.countDistinct("EventId").alias("events"),
                                                             F.count("*").alias("rows")).orderBy("_events_file"))