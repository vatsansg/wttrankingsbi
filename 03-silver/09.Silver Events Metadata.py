# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Events Metadata — Phase 12
# MAGIC Builds the two silver events tables from the two bronze events tables. Both are fully rebuilt every run (the
# MAGIC sources are small, so a rebuild is cheaper and safer than a merge).
# MAGIC
# MAGIC | Target | Grain | Built from |
# MAGIC |---|---|---|
# MAGIC | **`silver.events`** | one row per `EventId` | **`bronze.events`** (`dbo.Events`, the main event source): `event_name`, `start_date`, `end_date`, `event_ranking_year/month/week`, `rounds`, `sanctioned`, plus date helpers `event_year`, `event_quarter_*`; **and** the per-event tier and organisation from `bronze.events_metadata` (`EventTypeGeneralCode` → `event_type_general_code`, `EventTypeGeneralDesc` → `tier`, `OrganizationCode` → `organization_code`) |
# MAGIC | **`silver.events_metadata`** | one row per `EventId` + `RankingCategoryCode` + `ForRankingCategoryCode` | **`bronze.events_metadata`** (`dbo.EventsMetadata`): `ranking_category_desc`, `event_type_code` (`EventTypeCodewithDrawsize`), `event_type_desc` (`EventTypewithDrawsizeDesc`), `event_category_code` (`EventCategoryCodeforRankingCalculations`, **SEN/YOU**) |
# MAGIC
# MAGIC **Joining a result.** Event-level attributes join on `event_id` alone (`silver.events`). Row-level attributes join on
# MAGIC `event_id` + `ranking_category_code` + `age_category_code`, where the result's `AgeCategoryCode` matches the trimmed
# MAGIC `ForRankingCategoryCode`. `EventId` + `RankingCategoryCode` alone is not unique: a youth event has one row per age band
# MAGIC (U11 … U19, SEN) for the same ranking category, each with its own draw size and SEN/YOU category.
# MAGIC
# MAGIC **Coverage.** `silver.events` holds every event in `bronze.events` **or** `bronze.events_metadata`: an event only in
# MAGIC `Events` has no tier/organisation yet (NULL); an event only in `EventsMetadata` has no name/dates. Validator 09 and
# MAGIC `06-validation/02` report both cases.
# MAGIC
# MAGIC **Deterministic picks** (contract breaks are WARNs in validator 09 / `06-validation/02`, never failures here):
# MAGIC - an event whose `EventsMetadata` rows disagree on tier / organisation → values from its most recently ingested row
# MAGIC   (ties: lowest ranking category, age category, then every other column);
# MAGIC - two `EventsMetadata` rows with the same 3-part key → newest ingestion, then lowest `EventTypeCodewithDrawsize`, then
# MAGIC   every other column;
# MAGIC - `bronze.events` is one row per `EventId` (initial load from the SQL primary key, weekly `MERGE` on `EventId`); a
# MAGIC   duplicate there would be reported by validator 09 and resolved here by the newest ingestion.
# MAGIC
# MAGIC Conforming: explicit snake_case names; trimmed codes (the source pads `ForRankingCategoryCode`, e.g. `'U13 '`);
# MAGIC dates as DATE. No mappings and no other tables.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

ev_source = f"{catalog_name}.{bronze_schema}.events"
em_source = f"{catalog_name}.{bronze_schema}.events_metadata"
events_table = f"{catalog_name}.{silver_schema}.events"              # event level: one row per EventId
rows_table = f"{catalog_name}.{silver_schema}.events_metadata"       # row level: EventId + RankingCategoryCode + ForRankingCategoryCode

def trim_str(c):
    return F.nullif(F.trim(F.col(c).cast("string")), F.lit(""))

# ---- bronze.events -> event-level core (one row per event_id)
ev_df = spark.table(ev_source).select(
    F.col("EventId").cast("int").alias("event_id"),
    trim_str("EventName").alias("event_name"),
    F.to_date("StartDate").alias("start_date"),
    F.to_date("EndDate").alias("end_date"),
    F.col("RankingYear").cast("int").alias("event_ranking_year"),
    F.col("RankingMonth").cast("int").alias("event_ranking_month"),
    F.col("RankingWeek").cast("int").alias("event_ranking_week"),
    trim_str("Rounds").alias("rounds"),
    F.col("Sanctioned").cast("int").alias("sanctioned"),
    F.col("_ingestion_timestamp").alias("_events_ingestion_timestamp"),
)
EV_CORE = ["event_name", "start_date", "end_date", "event_ranking_year", "event_ranking_month", "event_ranking_week",
           "rounds", "sanctioned"]
ev_w = Window.partitionBy("event_id").orderBy(F.col("_events_ingestion_timestamp").desc(),
                                              *[F.col(c).asc_nulls_last() for c in EV_CORE])
ev_core = ev_df.withColumn("_rn", F.row_number().over(ev_w)).where("_rn = 1").drop("_rn")

# ---- bronze.events_metadata, conformed
em_df = spark.table(em_source).select(
    F.col("EventId").cast("int").alias("event_id"),
    trim_str("EventTypeGeneralCode").alias("event_type_general_code"),
    trim_str("EventTypeGeneralDesc").alias("tier"),
    trim_str("OrganizationCode").alias("organization_code"),
    trim_str("RankingCategoryCode").alias("ranking_category_code"),
    trim_str("ForRankingCategoryCode").alias("age_category_code"),
    trim_str("RankingCategoryDesc").alias("ranking_category_desc"),
    trim_str("EventTypeCodewithDrawsize").alias("event_type_code"),
    trim_str("EventTypewithDrawsizeDesc").alias("event_type_desc"),
    trim_str("EventCategoryCodeforRankingCalculations").alias("event_category_code"),
    F.col("_events_file").alias("source_events_file"),
    F.col("_refresh_ranking_year").alias("refresh_ranking_year"),
    F.col("_refresh_ranking_week").alias("refresh_ranking_week"),
    F.col("_ingestion_timestamp").alias("_bronze_ingestion_timestamp"),
).cache()

EM_EVENT = ["event_type_general_code", "tier", "organization_code"]
ROW_KEY = ["event_id", "ranking_category_code", "age_category_code"]
ROW_COLS = ["ranking_category_desc", "event_type_code", "event_type_desc", "event_category_code"]

# ---- Row level: one row per 3-part key (deterministic if the key is duplicated)
row_w = Window.partitionBy(*ROW_KEY).orderBy(F.col("_bronze_ingestion_timestamp").desc(),
                                             *[F.col(c).asc_nulls_last() for c in ROW_COLS + EM_EVENT])
rows_df = add_silver_metadata(
    em_df.withColumn("_rn", F.row_number().over(row_w)).where("_rn = 1")
         .select(*ROW_KEY, *ROW_COLS, "source_events_file", "refresh_ranking_year", "refresh_ranking_week",
                 "_bronze_ingestion_timestamp")
)

# ---- Per-event tier / organisation from EventsMetadata (deterministic if an event's rows disagree)
em_ev_w = Window.partitionBy("event_id").orderBy(F.col("_bronze_ingestion_timestamp").desc(),
                                                 F.col("ranking_category_code").asc_nulls_last(),
                                                 F.col("age_category_code").asc_nulls_last(),
                                                 *[F.col(c).asc_nulls_last() for c in EM_EVENT + ROW_COLS])
em_event = (em_df.withColumn("_rn", F.row_number().over(em_ev_w)).where("_rn = 1")
                 .select("event_id", *EM_EVENT).withColumn("_in_em", F.lit(True)))

# ---- Event level = Events (main source) + tier/organisation from EventsMetadata, every event in either table
events_df = add_silver_metadata(
    ev_core.join(em_event, "event_id", "full_outer")
        .withColumn("in_events", F.col("_events_ingestion_timestamp").isNotNull())
        .withColumn("in_events_metadata", F.coalesce(F.col("_in_em"), F.lit(False)))
        .withColumn("event_year", F.year("start_date"))
        .withColumn("event_quarter_start", F.to_date(F.date_trunc("quarter", "start_date")))
        .withColumn("event_quarter_label",
                    F.concat(F.year("start_date").cast("string"), F.lit(" Q"), F.quarter("start_date").cast("string")))
        .select("event_id", *EV_CORE, *EM_EVENT, "event_year", "event_quarter_start", "event_quarter_label",
                "in_events", "in_events_metadata")
)

(events_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(events_table))
(rows_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(rows_table))
em_df.unpersist()

# COMMAND ----------

e = spark.table(events_table)
n_src = spark.table(em_source).count()
n_rows = spark.table(rows_table).count()
print(f"OK    {events_table}: {e.count():,} events ({e.where('NOT in_events').count():,} only in EventsMetadata, "
      f"{e.where('NOT in_events_metadata').count():,} only in Events)")
print(f"OK    {rows_table}: {n_rows:,} rows ({n_src - n_rows} duplicate-key row(s) dropped)")
display(e.groupBy("organization_code").count().orderBy("organization_code"))