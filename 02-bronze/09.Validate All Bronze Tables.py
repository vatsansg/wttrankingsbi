# Databricks notebook source
# MAGIC %md
# MAGIC # Validate all bronze tables
# MAGIC Final task in the bronze ingestion job. Runs after every ingestion
# MAGIC notebook (landing, ledger, and the reference/player for-each) and fails
# MAGIC the job loudly if any of the 20 bronze tables is missing, empty, or
# MAGIC has an obviously-broken business key -- catching a broken pull before it
# MAGIC ever reaches Silver, rather than discovering it from a bad dashboard.
# MAGIC
# MAGIC This is intentionally lightweight (no pytest/dbx test scaffolding) --
# MAGIC proportionate to a 20-table bronze layer, not a large
# MAGIC production pipeline.
# MAGIC
# MAGIC **Step 8 additions (2026-09-19):**
# MAGIC - Check 3's `duplicate_key_checks` list now also covers
# MAGIC   `ranking_individuals`/`ranking_pairs`, not just `ref_countries` --
# MAGIC   closes the gap flagged in the `Incremental_Load_Steps_7-11_Plan.docx`
# MAGIC   review: bronze accumulates via `replaceWhere` now, so a bug in the
# MAGIC   replace predicate (or a landing file re-published with different
# MAGIC   content for an already-loaded week) could in principle produce true
# MAGIC   duplicates that only silver's dedup would quietly absorb.
# MAGIC - New Check 4: **target-week-landed assertion.** Takes
# MAGIC   `p_ranking_year`/`p_ranking_week` as widgets (populated via
# MAGIC   `dbutils.jobs.taskValues` from `01-control/02.Detect New Week` once
# MAGIC   Step 9 wires this into the scheduled job) and confirms that specific
# MAGIC   week actually has rows in `ranking_individuals`/`ranking_pairs` --
# MAGIC   closes the freshness-check gap from the same review: every existing
# MAGIC   check here passes on total accumulated history, so a run that
# MAGIC   detected/loaded the wrong week, or silently no-op'd, would otherwise
# MAGIC   look identical to a healthy run. Skipped (not failed) if the widgets
# MAGIC   are blank, so this notebook still runs standalone by hand during Step
# MAGIC   8/9 testing, same as before.
# MAGIC - On success, stamps `bronze_done` on the matching `batch_control` row
# MAGIC   (only when the target-week widgets were supplied); on failure, stamps
# MAGIC   `failed` with `failure_stage='bronze'` instead, so Step 10's freshness
# MAGIC   check and alerting can tell "never ran" apart from "ran and broke".

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

from pyspark.sql import functions as F

ALL_BRONZE_TABLES = [
    f"{catalog_name}.{bronze_schema}.ranking_individuals",
    f"{catalog_name}.{bronze_schema}.ranking_pairs",
    f"{catalog_name}.{bronze_schema}.players_events_results_master",
    f"{catalog_name}.{bronze_schema}.players_events_results_master_log",
    f"{catalog_name}.{bronze_schema}.players_events_results_master_log_archives",
    f"{catalog_name}.{bronze_schema}.individuals_event_penalties",
    f"{catalog_name}.{bronze_schema}.ref_countries",
    f"{catalog_name}.{bronze_schema}.ref_continents",
    f"{catalog_name}.{bronze_schema}.ref_age_categories",
    f"{catalog_name}.{bronze_schema}.ref_categories",
    f"{catalog_name}.{bronze_schema}.ref_ranking_categories",
    f"{catalog_name}.{bronze_schema}.ref_subevent_types",
    f"{catalog_name}.{bronze_schema}.ref_subevents_codes",
    f"{catalog_name}.{bronze_schema}.ref_subevents_codes_description",
    f"{catalog_name}.{bronze_schema}.ref_subevent_dependent_categories",
    f"{catalog_name}.{bronze_schema}.ref_organization",
    f"{catalog_name}.{bronze_schema}.ref_event_type_general",
    f"{catalog_name}.{bronze_schema}.ref_result_position",
    f"{catalog_name}.{bronze_schema}.competitors",
    f"{catalog_name}.{bronze_schema}.players_doubles"
]

dbutils.widgets.text("p_ranking_year", "")
dbutils.widgets.text("p_ranking_week", "")
v_ranking_year = dbutils.widgets.get("p_ranking_year")
v_ranking_week = dbutils.widgets.get("p_ranking_week")
HAVE_TARGET_WEEK = bool(v_ranking_year) and bool(v_ranking_week)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 1 - every table exists and has rows

# COMMAND ----------

failures = []
for full_table_name in ALL_BRONZE_TABLES:
    try:
        validate_bronze_table(full_table_name)
    except Exception as e:
        failures.append(str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 2 - no null business keys on a few load-bearing tables
# MAGIC Not exhaustive -- these are the columns everything else joins against, so
# MAGIC a null here would silently break joins all the way through Silver/Gold.

# COMMAND ----------

null_key_checks = [
    (f"{catalog_name}.{bronze_schema}.ref_countries", "CountryCode"),
    (f"{catalog_name}.{bronze_schema}.competitors", "PlayerID"),
    (f"{catalog_name}.{bronze_schema}.players_doubles", "DoublesId"),
    (f"{catalog_name}.{bronze_schema}.players_events_results_master", "CompetitorId"),
    (f"{catalog_name}.{bronze_schema}.ranking_individuals", "IttfId"),
    (f"{catalog_name}.{bronze_schema}.ranking_pairs", "PairId"),
]

for full_table_name, key_col in null_key_checks:
    null_count = spark.table(full_table_name).where(F.col(key_col).isNull()).count()
    if null_count > 0:
        failures.append(f"{full_table_name}.{key_col} has {null_count} null value(s)")
    else:
        print(f"OK    {full_table_name}.{key_col}: no nulls")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 3 - no duplicate business keys on tables a gold dimension
# MAGIC joins on by that key alone
# MAGIC **2026-09-13:** `ref_countries.CountryCode` added (expert pre-run review,
# MAGIC before the `ref_countries` source-table fix was executed) -- a duplicate
# MAGIC there would silently fan out `gold.dim_country` and every fact joined to
# MAGIC it, with nothing in Check 1/2 catching it (row counts and null-key checks
# MAGIC both stay clean on a duplicate non-null key).
# MAGIC
# MAGIC **2026-09-19 (Step 8):** `ranking_individuals`/`ranking_pairs` added,
# MAGIC same reasoning, scoped to their real natural key including
# MAGIC `(ranking_year, ranking_week)` -- a duplicate here wouldn't fan out a
# MAGIC gold dimension, but would silently double-count a player/pair for one
# MAGIC week's numbers, and only silver's `dedup_on_business_key` safety net
# MAGIC would ever quietly absorb it. Generalizes past `ref_countries` -- any
# MAGIC table a gold dimension or a per-week rollup depends on a single-row-per-
# MAGIC key guarantee for belongs in this list.

# COMMAND ----------

duplicate_key_checks = [
    (f"{catalog_name}.{bronze_schema}.ref_countries", ["CountryCode"]),
    (f"{catalog_name}.{bronze_schema}.ranking_individuals",
     ["IttfId", "SubEventCode", "RankingYear", "RankingWeek", "ranking_run_code"]),
    (f"{catalog_name}.{bronze_schema}.ranking_pairs",
     ["PairId", "SubEventCode", "RankingYear", "RankingWeek", "ranking_run_code"]),
]

for full_table_name, key_cols in duplicate_key_checks:
    dup_df = (
        spark.table(full_table_name)
            .groupBy(*key_cols)
            .count()
            .filter(F.col('count') > 1)
    )
    dup_count = dup_df.count()
    key_label = ", ".join(key_cols)
    if dup_count > 0:
        dup_values = [tuple(row[c] for c in key_cols) for row in dup_df.limit(20).collect()]
        failures.append(
            f"{full_table_name}.({key_label}) has {dup_count} duplicated key(s), "
            f"e.g. {dup_values} -- would silently double-count in any downstream rollup"
        )
    else:
        print(f"OK    {full_table_name}.({key_label}): no duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 4 - target-week-landed assertion (Step 8, freshness check)
# MAGIC Only runs when `p_ranking_year`/`p_ranking_week` were supplied (blank in
# MAGIC a standalone manual run -- this check is skipped, not failed, so nothing
# MAGIC about running this notebook by hand today changes). Once Step 9 wires
# MAGIC these widgets from `01-control/02.Detect New Week`'s taskValues, this is
# MAGIC what actually proves the week the orchestrator asked for landed --
# MAGIC Checks 1-3 above pass identically whether or not this run touched the
# MAGIC right week at all, since they only ever look at accumulated totals.

# COMMAND ----------

if HAVE_TARGET_WEEK:
    target_year, target_week = int(v_ranking_year), int(v_ranking_week)
    for full_table_name in [
        f"{catalog_name}.{bronze_schema}.ranking_individuals",
        f"{catalog_name}.{bronze_schema}.ranking_pairs",
    ]:
        landed_count = (
            spark.table(full_table_name)
                .where((F.col('RankingYear') == target_year) & (F.col('RankingWeek') == target_week))
                .count()
        )
        if landed_count == 0:
            failures.append(
                f"{full_table_name}: target week ({target_year}, {target_week}) has 0 rows -- "
                f"this run did not actually land the week the orchestrator asked for"
            )
        else:
            print(f"OK    {full_table_name}: target week ({target_year}, {target_week}) has {landed_count} row(s)")
else:
    print("INFO  p_ranking_year/p_ranking_week not supplied -- skipping Check 4 (standalone run).")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Result + `control.batch_control` status update
# MAGIC Status is only touched when the target-week widgets were supplied --
# MAGIC same standalone-run accommodation as Check 4.

# COMMAND ----------

if failures:
    if HAVE_TARGET_WEEK:
        spark.sql(f"""
            UPDATE wttrankingsbi.control.batch_control
            SET status = 'failed', failed_at = current_timestamp(),
                failure_stage = 'bronze', failure_message = {chr(39)}{'; '.join(failures)[:4000].replace(chr(39), chr(39)+chr(39))}{chr(39)},
                updated_at = current_timestamp()
            WHERE ranking_year = {int(v_ranking_year)} AND ranking_week = {int(v_ranking_week)}
        """)
    raise ValueError("Bronze validation FAILED:\n" + "\n".join(f"  - {f}" for f in failures))

if HAVE_TARGET_WEEK:
    spark.sql(f"""
        UPDATE wttrankingsbi.control.batch_control
        SET status = 'bronze_done', bronze_done_at = current_timestamp(), updated_at = current_timestamp()
        WHERE ranking_year = {int(v_ranking_year)} AND ranking_week = {int(v_ranking_week)}
    """)

print(f"Bronze validation passed: {len(ALL_BRONZE_TABLES)} tables checked.")