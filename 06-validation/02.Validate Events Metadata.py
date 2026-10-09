# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Events and Events Metadata (full tables) — Phase 12
# MAGIC **On demand, not part of the weekly job.** Checks the whole of `bronze.events` (copy of Azure SQL `dbo.Events`) and
# MAGIC `bronze.events_metadata` (copy of `dbo.EventsMetadata`) against the Ranking team's contract, and lists every event
# MAGIC with an error so the list can be sent to the Ranking team. Run it after the initial loads, after the one-off full
# MAGIC EventsMetadata reload, and whenever you want a complete picture. The weekly job applies the same rules (shared helper
# MAGIC `00-common/10.events-helpers`) but only to the events in that week's file, in `02-bronze/09`.
# MAGIC
# MAGIC Never fails. Output:
# MAGIC 1. Summary: number of events per issue.
# MAGIC 2. **Events with errors**: one row per event and issue (also written to `control.events_metadata_issues`, replaced on
# MAGIC    every run, so it can be queried or exported). Issues:
# MAGIC    - `duplicate EventId in Events`, `missing EventName or StartDate` (`dbo.Events`);
# MAGIC    - `tier/organisation differ between EventsMetadata rows`, `duplicate EventsMetadata row key` (`dbo.EventsMetadata`);
# MAGIC    - `in Events, no EventsMetadata rows` and `in EventsMetadata, not in Events` (severity `coverage`: one table has the
# MAGIC      event, the other doesn't; expected to shrink to zero after the full EventsMetadata dump). Everything else is `error`.
# MAGIC 3. Coverage of the results (information): result events in neither table, and result category/age combinations with
# MAGIC    no `EventsMetadata` row for an event that has rows.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/10.events-helpers

# COMMAND ----------

from pyspark.sql import functions as F

ev_bronze = f"{catalog_name}.{bronze_schema}.events"
em_bronze = f"{catalog_name}.{bronze_schema}.events_metadata"
per_table = f"{catalog_name}.{silver_schema}.player_event_results"
em_rows = f"{catalog_name}.{silver_schema}.events_metadata"
issues_table = f"{catalog_name}.control.events_metadata_issues"

ev = spark.table(ev_bronze)
em = spark.table(em_bronze)
print(f"INFO  {ev_bronze}: {ev.count():,} rows, {ev.select('EventId').distinct().count():,} events")
print(f"INFO  {em_bronze}: {em.count():,} rows, {em.select('EventId').distinct().count():,} events")

ev_ids = ev.select("EventId").distinct()
em_ids = em.select(F.col("EventId").cast("int").alias("EventId")).distinct()
names = ev.groupBy("EventId").agg(F.min("EventName").alias("_name"))
only_ev = (ev_ids.join(em_ids, "EventId", "left_anti")
             .select("EventId", F.lit(None).cast("string").alias("EventName"),
                     F.lit("in Events, no EventsMetadata rows").alias("issue"), F.lit("").alias("detail"),
                     F.lit(0).cast("long").alias("rows_affected")))
only_em = (em.groupBy(F.col("EventId").cast("int").alias("EventId")).agg(F.count("*").alias("rows_affected"))
             .join(ev_ids, "EventId", "left_anti")
             .select("EventId", F.lit(None).cast("string").alias("EventName"),
                     F.lit("in EventsMetadata, not in Events").alias("issue"), F.lit("").alias("detail"), "rows_affected"))

issues = (events_issues(ev).unionByName(em_issues(em)).unionByName(only_ev).unionByName(only_em)
            .join(names, "EventId", "left")
            .withColumn("EventName", F.coalesce("EventName", "_name")).drop("_name")
            .withColumn("severity", F.when(F.col("issue").isin("in Events, no EventsMetadata rows",
                                                                "in EventsMetadata, not in Events"), "coverage")
                                     .otherwise("error"))
            .withColumn("checked_at", F.current_timestamp())).cache()
n_issues = issues.count()
(issues.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(issues_table))

n_err = issues.where("severity = 'error'").select("EventId").distinct().count()
n_cov = issues.where("severity = 'coverage'").select("EventId").distinct().count()
if n_issues:
    print(f"WARN  {n_err:,} event(s) with errors, {n_cov:,} event(s) in only one of Events / EventsMetadata -- "
          f"full list in {issues_table}; send it to the Ranking team")
else:
    print("OK    every event meets the Events / EventsMetadata contract")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 - Summary per issue

# COMMAND ----------

display(issues.groupBy("severity", "issue").agg(F.countDistinct("EventId").alias("events"), F.sum("rows_affected").alias("rows"))
              .orderBy("severity", "issue"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - Events with errors (send to the Ranking team)

# COMMAND ----------

display(issues.orderBy(F.col("severity").desc(), "EventId", "issue", "detail"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 - Coverage of the results (information)

# COMMAND ----------

if spark.catalog.tableExists(per_table) and spark.catalog.tableExists(em_rows):
    res_keys = spark.table(per_table).select("event_id", "ranking_category_code", "age_category_code").distinct()
    known = ev_ids.select(F.col("EventId").alias("event_id")).union(em_ids.select(F.col("EventId").alias("event_id"))).distinct()
    res_events = res_keys.select("event_id").distinct()
    n_ev = res_events.count()
    n_missing = res_events.join(known, "event_id", "left_anti").count()
    em_events = spark.table(em_rows).select("event_id").distinct()
    missing_rows = (res_keys.join(em_events, "event_id", "left_semi")
                            .join(spark.table(em_rows).select("event_id", "ranking_category_code", "age_category_code"),
                                  ["event_id", "ranking_category_code", "age_category_code"], "left_anti"))
    print(f"INFO  events in the results: {n_ev:,}; in neither Events nor EventsMetadata: {n_missing:,} "
          f"({(100.0 * n_missing / n_ev) if n_ev else 0:.1f}%)")
    print(f"INFO  result category/age combinations with no EventsMetadata row (event has rows): {missing_rows.count():,}")
    display(missing_rows.groupBy("event_id").agg(F.collect_set(F.concat_ws(" / ", "ranking_category_code", "age_category_code"))
                                                 .alias("missing_category_age")).orderBy("event_id"))
else:
    print("INFO  silver tables not built yet -- coverage skipped")
issues.unpersist()