# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Events — Phase 12
# MAGIC Target: **`bronze.events`**, one row per event: the main event source (name, dates, ranking year/month/week, rounds,
# MAGIC sanctioned). Tier, organisation and the per-category rows come from `bronze.events_metadata` (`02-bronze/12`).
# MAGIC
# MAGIC | Mode (`p_load_mode`) | When | Source | Write |
# MAGIC |---|---|---|---|
# MAGIC | `initial` | **Once**, by hand, at cut-over | Azure SQL `dbo.Events` (all events), over the existing JDBC connection | full overwrite |
# MAGIC | `weekly` (default; the job uses this) | Every new ranking week | the rows of `Events_<RankingYear>_<RankingWeek>.csv` in the week's landing folder (it carries the `dbo.Events` layout) | **upsert** (`MERGE` on `EventId`): listed events are updated, new ones inserted, nothing deleted |
# MAGIC
# MAGIC Only these `dbo.Events` columns are kept: `EventId`, `EventName`, `StartDate`, `EndDate`, `RankingYear`,
# MAGIC `RankingMonth`, `RankingWeek`, `Rounds`, `Sanctioned`. The file's other columns (`EventTypeGeneralCode`,
# MAGIC `InsertUpdateDeleteFlag`, audit columns, `IsForbidden`) are ignored.
# MAGIC
# MAGIC **Rules in the weekly mode** (file handling is shared with `02-bronze/12` via `00-common/10.events-helpers`):
# MAGIC - A missing file is a WARN before 2026 wk41 and a **failure** from wk41 (`p_require_events_file=auto`).
# MAGIC - A missing `dbo.Events` column or a non-numeric `EventId` fails the task.
# MAGIC - If an EventId appears more than once in the file, one row is kept deterministically (the latest `StartDate`, then
# MAGIC   the highest name) and a WARN names it.
# MAGIC - A value that is present but unreadable (a date that isn't ISO or day/month/year, a non-integer in `RankingYear`,
# MAGIC   `RankingMonth`, `RankingWeek` or `Sanctioned`) is loaded as NULL, with a WARN naming the column and EventIds.
# MAGIC - Upserted rows are stamped with `_refresh_ranking_year` / `_refresh_ranking_week`, so validator 09 can check exactly
# MAGIC   the events refreshed this week.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

# MAGIC %run ../00-common/10.events-helpers

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

#dbutils.widgets.dropdown("p_load_mode", "weekly", ["weekly", "initial"])
dbutils.widgets.text("p_ranking_year", "")
dbutils.widgets.text("p_ranking_week", "")
dbutils.widgets.dropdown("p_require_events_file", "auto", ["auto", "true", "false"])
v_load_mode = dbutils.widgets.get("p_load_mode")
v_ranking_year = dbutils.widgets.get("p_ranking_year")
v_ranking_week = dbutils.widgets.get("p_ranking_week")
v_require_mode = dbutils.widgets.get("p_require_events_file")

source_table = "Events"
table_name = f"{catalog_name}.{bronze_schema}.events"


def stamp(df, source_label):
    return (df.withColumn("_source", F.lit(source_label))
              .withColumn("_refresh_ranking_year", F.lit(int(v_ranking_year) if v_ranking_year else None).cast("int"))
              .withColumn("_refresh_ranking_week", F.lit(int(v_ranking_week) if v_ranking_week else None).cast("int"))
              .withColumn("_ingestion_timestamp", F.current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initial load (one-off): full copy of `dbo.Events`

# COMMAND ----------

if v_load_mode == "initial":
    full_df = stamp(conform_events(read_from_sqlserver(source_table, EV_COLS)), "dbo.Events (initial)")
    (full_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name))
    t = spark.table(table_name)
    dup = t.groupBy("EventId").count().where("count > 1").count()
    print(f"OK    initial load: {t.count():,} events" + (f" -- WARN {dup} duplicated EventId(s) in dbo.Events" if dup else ""))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Weekly upsert from `Events_<year>_<week>.csv`

# COMMAND ----------

if v_load_mode == "weekly":
    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"{table_name} does not exist yet -- run this notebook once with p_load_mode=initial first.")
    if not (v_ranking_year and v_ranking_week):
        raise ValueError("weekly mode needs p_ranking_year and p_ranking_week (the job passes them from detect_new_week).")

    file_name, file_rows = read_weekly_events_file(v_ranking_year, v_ranking_week, v_require_mode)
    if file_rows is not None:
        conformed = conform_events(file_rows)
        # Values present in the file but unreadable (dates, integers) -> loaded as NULL, reported per column.
        checked = file_rows.select(F.col("EventId").cast("int").alias("EventId"),
                                   *[F.col(c).alias(f"raw_{c}") for c in EV_COLS[2:] if c != "Rounds"]) \
                           .join(conformed, "EventId")
        for c in [c for c in EV_COLS[2:] if c != "Rounds"]:
            bad = [r[0] for r in checked.where((F.trim(F.col(f"raw_{c}")) != "") & F.col(c).isNull())
                                        .select("EventId").distinct().limit(50).collect()]
            if bad:
                print(f"WARN  {file_name}: unreadable {c} for EventId(s) {sorted(bad)} -- loaded as NULL")
        # One row per EventId (deterministic if the file repeats an event).
        w = Window.partitionBy("EventId").orderBy(F.col("StartDate").desc_nulls_last(), F.col("EventName").desc_nulls_last(),
                                                  *[F.col(c).desc_nulls_last() for c in EV_COLS[3:]])
        dups = conformed.groupBy("EventId").count().where("count > 1").select("EventId").collect()
        if dups:
            print(f"WARN  {file_name}: EventId(s) listed more than once, one row kept: {sorted(r[0] for r in dups)}")
        src = stamp(conformed.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn"), file_name)
        src.createOrReplaceTempView("_events_week")
        n_listed = src.count()
        existing = spark.table(table_name).join(src.select("EventId"), "EventId", "left_semi").count()
        if n_listed:
            spark.sql(f"""
                MERGE INTO {table_name} t
                USING _events_week s
                ON t.EventId = s.EventId
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        print(f"OK    {file_name}: {n_listed} event(s) upserted into {table_name} "
              f"({existing} updated, {n_listed - existing} inserted)")

# COMMAND ----------

t = spark.table(table_name)
nulls = t.where(F.col("EventId").isNull()).count()
if t.count() == 0 or nulls:
    raise ValueError(f"{table_name}: {t.count()} rows, {nulls} NULL EventId(s) -- the load is broken")
print(f"OK    {table_name}: {t.count():,} events")
display(t.groupBy("_source").count().orderBy("_source"))