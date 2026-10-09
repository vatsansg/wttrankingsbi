# Databricks notebook source
# MAGIC %md
# MAGIC ## 10. Events helpers (Phase 12)
# MAGIC Shared by every events notebook, so the weekly file and the contract rules are handled in exactly one place:
# MAGIC - `02-bronze/13.Ingest Events` and `02-bronze/12.Ingest Events Metadata` read the weekly file with
# MAGIC   `read_weekly_events_file()`;
# MAGIC - `02-bronze/09.Validate All Bronze Tables` (weekly job: **only the events in that week's file**) and
# MAGIC   `06-validation/02.Validate Events Metadata` (on demand: **the whole tables**) use `events_issues()` and `em_issues()`.
# MAGIC Nothing here writes a table. The check functions never raise; every problem is returned as a row (callers WARN).
# MAGIC
# MAGIC **Sources (Azure SQL `dbtabletennisuniverse_ranking`):**
# MAGIC - `dbo.Events` → `bronze.events`, one row per event: the **event-level** columns `EventId`, `EventName`, `StartDate`,
# MAGIC   `EndDate`, `RankingYear`, `RankingMonth`, `RankingWeek`, `Rounds`, `Sanctioned`. One-off full dump, then upserted
# MAGIC   every week from the rows of `Events_<RankingYear>_<RankingWeek>.csv`.
# MAGIC - `dbo.EventsMetadata` → `bronze.events_metadata`, several rows per event: the **row-level** columns
# MAGIC   `EventTypeCodewithDrawsize`, `EventTypewithDrawsizeDesc`, `ForRankingCategoryCode`, `RankingCategoryCode`,
# MAGIC   `RankingCategoryDesc`, `EventCategoryCodeforRankingCalculations`, plus the per-event tier and organisation
# MAGIC   (`EventTypeGeneralCode`, `EventTypeGeneralDesc`, `OrganizationCode`, constant across an event's rows). A row is
# MAGIC   identified by `EventId` + `RankingCategoryCode` + `ForRankingCategoryCode` (trimmed). Listed EventIds are deleted
# MAGIC   and re-inserted from SQL every week.
# MAGIC
# MAGIC **Contract checks (WARN only):**
# MAGIC - `events_issues(df)`: duplicate `EventId`; missing `EventName` or `StartDate`. (A date in the weekly file that
# MAGIC   can't be parsed is reported by `02-bronze/13` when it reads the file.)
# MAGIC - `em_issues(df)`: tier / organisation (`EventTypeGeneralCode`, `EventTypeGeneralDesc`, `OrganizationCode`) differ
# MAGIC   between an event's rows; duplicate `EventId` + `RankingCategoryCode` + `ForRankingCategoryCode`.

# COMMAND ----------

import re
from pyspark.sql import functions as F

EVENTS_FILE_START = (2026, 41)   # first week the Ranking team produces Events_<year>_<week>.csv

EV_COLS = ["EventId", "EventName", "StartDate", "EndDate", "RankingYear", "RankingMonth", "RankingWeek", "Rounds", "Sanctioned"]
EM_EVENT_COLS = ["EventTypeGeneralCode", "EventTypeGeneralDesc", "OrganizationCode"]          # constant per event
EM_ROW_COLS = ["RankingCategoryCode", "ForRankingCategoryCode", "RankingCategoryDesc", "EventTypeCodewithDrawsize",
               "EventTypewithDrawsizeDesc", "EventCategoryCodeforRankingCalculations"]
EM_ROW_KEY = ["EventId", "RankingCategoryCode", "ForRankingCategoryCode"]


def _blank_to_null(c):
    return F.nullif(F.trim(F.col(c).cast("string")), F.lit(""))


def parse_ts(c):
    """Timestamp from SQL (already a timestamp) or from the weekly CSV: ISO ('yyyy-MM-ddTHH:mm:ss', with or without
    fractional seconds) or day-first ('15/03/2021 08:00:00', '4/10/2026 8:01'). Single-letter patterns (d/M/H) also accept
    two digits, and avoid Spark's INCONSISTENT_BEHAVIOR_CROSS_VERSION error that 'dd/MM' raises on single-digit values.
    Anything else becomes NULL (never an error)."""
    s = _blank_to_null(c)
    return F.coalesce(F.try_to_timestamp(s), F.try_to_timestamp(s, F.lit("d/M/yyyy H:mm[:ss]")))


def try_int(c):
    """Integer or NULL, never an error (ANSI-safe): '' / 'abc' / '2026.0' -> NULL."""
    return F.expr(f"try_cast(nullif(trim(cast(`{c}` AS STRING)), '') AS INT)")


def conform_events(df):
    """Any source with the dbo.Events columns (JDBC or the weekly CSV) -> the bronze.events column types."""
    return df.select(
        F.col("EventId").cast("int").alias("EventId"),
        _blank_to_null("EventName").alias("EventName"),
        parse_ts("StartDate").alias("StartDate"),
        parse_ts("EndDate").alias("EndDate"),
        try_int("RankingYear").alias("RankingYear"),
        try_int("RankingMonth").alias("RankingMonth"),
        try_int("RankingWeek").alias("RankingWeek"),
        _blank_to_null("Rounds").alias("Rounds"),
        try_int("Sanctioned").alias("Sanctioned"),
    )


def read_weekly_events_file(ranking_year, ranking_week, require_mode="auto"):
    """Read Events_<year>_<week>.csv from the week's landing folder.
    Returns (file_name, DataFrame of the file's rows with every column as a string, BOM stripped), or (file_name, None)
    when the file is missing and not required. Raises when the file is required and missing (from EVENTS_FILE_START
    with require_mode='auto', or always with 'true'), when a needed column is missing, or when an EventId is non-numeric.
    Only 'path not found' counts as missing; any other read problem always fails."""
    y, w = int(ranking_year), int(ranking_week)
    file_name = f"Events_{y}_{w}.csv"
    path = f"{landing_folder_path}/{y} - {w}/{file_name}"
    required = (require_mode == "true") or (require_mode == "auto" and (y, w) >= EVENTS_FILE_START)
    try:
        raw = spark.read.option("header", "true").option("multiLine", "true").option("escape", '"').csv(path)
        raw = raw.toDF(*[c.replace("﻿", "").strip() for c in raw.columns])     # strip BOM / spaces
        raw.columns                                                                    # force the read
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e) or "FileNotFoundException" in str(e) or "does not exist" in str(e):
            msg = f"weekly events file not found at {path}"
            if required:
                raise ValueError(msg + f" -- required from week {EVENTS_FILE_START} (p_require_events_file={require_mode}).")
            print(f"WARN  {msg} -- no events refresh this week (before {EVENTS_FILE_START} this is expected).")
            return file_name, None
        raise
    missing = [c for c in EV_COLS if c not in raw.columns]
    if missing:
        raise ValueError(f"{file_name}: missing column(s) {missing} -- expected the dbo.Events layout.")
    rows = raw.withColumn("EventId", F.trim(F.col("EventId"))).where(F.col("EventId").isNotNull() & (F.col("EventId") != ""))
    bad = sorted(r[0] for r in rows.where(~F.col("EventId").rlike(r"^[0-9]+$")).select("EventId").distinct().limit(20).collect())
    if bad:
        raise ValueError(f"{file_name}: non-numeric EventId value(s) {bad} -- fix the file and re-run.")
    return file_name, rows


def events_issues(df):
    """bronze.events-shaped rows -> one row per problem: EventId, EventName, issue, detail, rows_affected."""
    a = (df.groupBy("EventId").agg(F.count("*").alias("rows_affected"), F.min("EventName").alias("EventName"))
            .where("rows_affected > 1")
            .select("EventId", "EventName", F.lit("duplicate EventId in Events").alias("issue"), F.lit("").alias("detail"),
                    "rows_affected"))
    b = (df.where(F.col("EventName").isNull() | F.col("StartDate").isNull())
            .groupBy("EventId").agg(F.count("*").alias("rows_affected"), F.min("EventName").alias("EventName"))
            .select("EventId", "EventName", F.lit("missing EventName or StartDate").alias("issue"), F.lit("").alias("detail"),
                    "rows_affected"))
    return a.unionByName(b)


def em_normalised(df):
    return df.select(F.col("EventId").cast("int").alias("EventId"),
                     *[_blank_to_null(c).alias(c) for c in EM_EVENT_COLS + EM_ROW_COLS])


def em_issues(df):
    """bronze.events_metadata-shaped rows -> one row per problem: EventId, EventName (NULL here), issue, detail, rows_affected."""
    n = em_normalised(df)
    counts = n.groupBy("EventId").agg(
        F.count("*").alias("rows_affected"),
        *[F.countDistinct(F.coalesce(F.col(c), F.lit("<NULL>"))).alias(c) for c in EM_EVENT_COLS])
    differing = F.concat_ws(", ", *[F.when(F.col(c) > 1, F.lit(c)) for c in EM_EVENT_COLS])
    a = (counts.withColumn("detail", differing).where(F.length("detail") > 0)
            .select("EventId", F.lit("tier/organisation differ between EventsMetadata rows").alias("issue"), "detail",
                    "rows_affected"))
    b = (n.groupBy(*EM_ROW_KEY).agg(F.count("*").alias("rows_affected")).where("rows_affected > 1")
            .select("EventId", F.lit("duplicate EventsMetadata row key").alias("issue"),
                    F.concat_ws(" / ", F.coalesce("RankingCategoryCode", F.lit("<NULL>")),
                                F.coalesce("ForRankingCategoryCode", F.lit("<NULL>"))).alias("detail"),
                    "rows_affected"))
    return (a.unionByName(b).withColumn("EventName", F.lit(None).cast("string"))
             .select("EventId", "EventName", "issue", "detail", "rows_affected"))