# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Validate all bronze tables
# MAGIC
# MAGIC > **Phase 12 (2026-10-08).** The three per-source ledger tables (`players_events_results_master`, `_log`, `_log_archives`)
# MAGIC > are retired and replaced by the consolidated **`players_events_results`**. The new **`events`** (`dbo.Events`) and
# MAGIC > **`events_metadata`** (`dbo.EventsMetadata`) tables are added. Bronze now has 20 tables. Phase 12 checks (Check 5):
# MAGIC > - no null keys on the three new tables;
# MAGIC > - source copies (a result written more than once under the result key) are a **WARN**, resolved in silver 08;
# MAGIC >   only two **counted** rows with the same `ResultType` and best-N slot **FAIL** (they can't be resolved without losing one);
# MAGIC > - the target week landed in `players_events_results` (**FAIL** if 0 rows). The bronze notebook loads the target week
# MAGIC >   from Master or `_Log`, so 0 rows means it was genuinely missed.
# MAGIC > - best-N is present for the target week (**FAIL** if no row has `PlayerBestRankingResultNumber <> 0`). Rankings are
# MAGIC >   calculated and published before the files land, so 0% counted means something went wrong upstream.
# MAGIC > - Events contract (WARN only, never a failure), **only for the events in this week's file** (the EventIds that
# MAGIC >   `02-bronze/13` upserted and stamped with this week; their `events_metadata` rows are checked too): in `events`, a duplicate `EventId` or a missing `EventName`/`StartDate`;
# MAGIC >   in `events_metadata`, tier/organisation that differ between an event's rows, or a duplicate `EventId` +
# MAGIC >   `RankingCategoryCode` + `ForRankingCategoryCode`; a listed event with no `EventsMetadata` rows. The rules live in
# MAGIC >   `00-common/10.events-helpers`; `06-validation/02.Validate Events Metadata` runs them on the whole tables.
# MAGIC
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

# MAGIC %run ../00-common/10.events-helpers

# COMMAND ----------

from pyspark.sql import functions as F

ALL_BRONZE_TABLES = [
    f"{catalog_name}.{bronze_schema}.ranking_individuals",
    f"{catalog_name}.{bronze_schema}.ranking_pairs",
    f"{catalog_name}.{bronze_schema}.players_events_results",   # Phase 12: replaces the 3 ledger tables
    f"{catalog_name}.{bronze_schema}.events",                   # Phase 12: new (dbo.Events, one row per event)
    f"{catalog_name}.{bronze_schema}.events_metadata",          # Phase 12: new (dbo.EventsMetadata)
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
    (f"{catalog_name}.{bronze_schema}.players_events_results", "CompetitorId"),
    (f"{catalog_name}.{bronze_schema}.players_events_results", "EventId"),
    (f"{catalog_name}.{bronze_schema}.events", "EventId"),
    (f"{catalog_name}.{bronze_schema}.events_metadata", "EventId"),
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
# MAGIC #### Check 5 - Phase 12: results grain + target week landed (FAIL) and events_metadata key (WARN)

# COMMAND ----------

# Phase 12. Grain of bronze.players_events_results = one row per
# (competitor, event, subevent, ranking category, age category, ranking category code SEN/YOU, result position, week).
# A youth result is listed under both the YOU and the SEN ranking (different points), and a lucky loser can have a
# qualifying AND a main-draw row in the same event -- hence CategoryCode and ResultPosition in the key.
# Checked on the target week only, to keep the weekly run cheap. A target week with 0 rows FAILS: the bronze
# notebook loads the target week from Master or _Log, so 0 rows means the week was genuinely not loaded.
warnings = []
results_table = f"{catalog_name}.{bronze_schema}.players_events_results"
RESULT_KEY = ["CompetitorId", "EventId", "SubEventCode", "RankingCategoryCode", "AgeCategoryCode", "CategoryCode",
              "ResultPosition", "RankingYear", "RankingWeek"]
results_scope = spark.table(results_table)
if HAVE_TARGET_WEEK:
    results_scope = results_scope.where((F.col("RankingYear") == int(v_ranking_year)) & (F.col("RankingWeek") == int(v_ranking_week)))
    landed = results_scope.count()
    if landed == 0:
        failures.append(f"{results_table}: target week ({v_ranking_year}, {v_ranking_week}) has 0 rows -- "
                        f"the week was not loaded (see 02-bronze/11 output)")
    else:
        print(f"OK    {results_table}: target week ({v_ranking_year}, {v_ranking_week}) has {landed:,} row(s)")
        counted = results_scope.where(F.coalesce(F.col("PlayerBestRankingResultNumber"), F.lit(0)) != 0).count()
        if counted == 0:
            failures.append(f"{results_table}: target week ({v_ranking_year}, {v_ranking_week}) has no row with "
                            f"PlayerBestRankingResultNumber <> 0 -- best-N must be final when the week lands; check the "
                            f"Ranking DB before re-running")
        else:
            print(f"OK    {results_table}: target week has {counted:,} best-N counted row(s) ({100.0 * counted / landed:.1f}%)")
# Source copies: the source sometimes writes a result more than once under RESULT_KEY (identical copies, a counted and
# an uncounted copy, older/newer versions, or several counted ZPP slots). Silver 08 resolves every one of these
# deterministically (grain RESULT_KEY + ResultType + best-N slot), so they are a WARN for the Ranking team. Only COUNTED
# rows with the same ResultType and best-N slot that DIFFER (points or best-N number) cannot be resolved without losing a
# counted result -> FAIL. Identical counted copies are just copies (WARN).
_counted = F.coalesce(F.col("PlayerBestRankingResultNumber"), F.lit(0)) != 0
_slot = F.when(_counted, F.coalesce(F.when(F.col("BestResultNoSENYOU") != 0, F.col("BestResultNoSENYOU")), F.col("PlayerBestRankingResultNumber"))).otherwise(F.lit(0))
copy_groups = (results_scope.groupBy(*RESULT_KEY)
                 .agg(F.count("*").alias("n"), F.countDistinct("ResultType").alias("types"))
                 .where("n > 1"))
n_copy_groups = copy_groups.count()
clashes = (results_scope.where(_counted)
             .groupBy(*RESULT_KEY, F.coalesce(F.trim("ResultType"), F.lit("")).alias("rt"), _slot.alias("slot"))
             .agg(F.countDistinct(F.concat_ws("|", F.coalesce(F.col("RankingPoints").cast("string"), F.lit("~")),
                                              F.col("PlayerBestRankingResultNumber").cast("string"))).alias("versions"))
             .where("versions > 1").count())
scope_label = " in the target week" if HAVE_TARGET_WEEK else ""
if clashes:
    failures.append(f"{results_table}: {clashes} result(s){scope_label} with two different COUNTED rows for the same ResultType "
                    f"and best-N slot (BestResultNoSENYOU) -- silver cannot keep both; check with the Ranking team")
if n_copy_groups:
    warnings.append(f"{results_table}: {n_copy_groups:,} result key(s){scope_label} written more than once by the source "
                    f"(copies / ZPP slots); silver 08 resolves them (see its Step 3) -- report to the Ranking team")
else:
    print(f"OK    {results_table}: no source copies{scope_label}")

ev_table = f"{catalog_name}.{bronze_schema}.events"
em_table = f"{catalog_name}.{bronze_schema}.events_metadata"
if HAVE_TARGET_WEEK:
    # Weekly job: only the events in this week's file (02-bronze/13 and 02-bronze/12 stamp their rows with the week).
    def _this_week(t):
        return spark.table(t).where((F.col("_refresh_ranking_year") == int(v_ranking_year)) &
                                    (F.col("_refresh_ranking_week") == int(v_ranking_week)))
    week_ids = [r[0] for r in _this_week(ev_table).select("EventId").distinct().collect()]
    if not week_ids:
        print(f"INFO  {ev_table}: no events upserted for week ({v_ranking_year}, {v_ranking_week}) -- events checks skipped")
    else:
        week_ev = spark.table(ev_table).where(F.col("EventId").isin(week_ids))        # every row of those events
        week_em = spark.table(em_table).where(F.col("EventId").isin(week_ids))
        issues = events_issues(week_ev).unionByName(em_issues(week_em)).orderBy("EventId", "issue").collect()
        no_em = sorted(set(week_ids) - {r[0] for r in week_em.select("EventId").distinct().collect()})
        for r in issues:
            warnings.append(f"EventId {r['EventId']}" + (f" ({r['EventName']})" if r['EventName'] else "") + f": {r['issue']}"
                            + (f" [{r['detail']}]" if r['detail'] else "") + f" -- {r['rows_affected']} row(s); tell the Ranking team")
        if no_em:
            warnings.append(f"{len(no_em)} event(s) in this week's file have no EventsMetadata rows (tier/organisation/"
                            f"categories unknown until they are added): {no_em[:50]}")
        if not issues and not no_em:
            print(f"OK    the {len(week_ids)} event(s) in this week's file meet the Events / EventsMetadata contract")
else:
    print(f"INFO  standalone run -- weekly Events / EventsMetadata checks skipped (use 06-validation/02 for the whole tables)")

for w in warnings:
    print(f"WARN  {w}")

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

print(f"Bronze validation passed: {len(ALL_BRONZE_TABLES)} tables checked ({len(warnings)} Phase 12 warning(s)).")