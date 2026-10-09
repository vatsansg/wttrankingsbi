# Databricks notebook source
# MAGIC %md
# MAGIC # Validate all silver tables
# MAGIC
# MAGIC > **Phase 12 (2026-10-08).** `silver.points_ledger` is retired and replaced by **`silver.player_event_results`**
# MAGIC > (from the consolidated `bronze.players_events_results`). **`silver.events`** (event level) and
# MAGIC > **`silver.events_metadata`** (row level) are added. Check 3 reports unmapped competitors in `player_event_results`
# MAGIC > per ranking category as **WARN only**: unmapped IDs are shown on the dashboard as `ITTFID_<category>` buckets with a
# MAGIC > drill-down (`gold.v_evt_unmapped_competitors`), so there is no threshold to maintain. Check 5 asserts the
# MAGIC > `player_event_results` grain and the two events tables' keys are unique. EventsMetadata contract checks (constant
# MAGIC > columns, duplicate keys) run in validator 09 for that week's events, and in full in `06-validation/02`.
# MAGIC Final task in the silver transformation job. Runs after every silver
# MAGIC notebook (identity dimension, the 4 bespoke tables, and the reference
# MAGIC for-each) and fails the job loudly if any of the 17 silver tables is
# MAGIC missing, empty, or has a broken business key -- same intent as bronze's
# MAGIC `09.Validate All Bronze Tables`, plus a check specific to Silver: how
# MAGIC much of each fact table's `ittfid` failed to resolve against
# MAGIC `silver.player_identity`.
# MAGIC
# MAGIC Intentionally lightweight, proportionate to a 17-table silver layer --
# MAGIC not a large production test suite.
# MAGIC
# MAGIC **Step 8 addition (2026-09-19): Check 4, target-week-landed assertion**
# MAGIC -- same freshness-check pattern as bronze's Check 4 (see that notebook),
# MAGIC scoped to `silver.ranking_individuals`/`ranking_pairs`. Skipped when
# MAGIC `p_ranking_year`/`p_ranking_week` aren't supplied; on success/failure,
# MAGIC updates the matching `control.batch_control` row.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

# MAGIC %run ../00-common/07.silver-reference-registry

# COMMAND ----------

ALL_SILVER_TABLES = [
    f"{catalog_name}.{silver_schema}.player_identity",
    f"{catalog_name}.{silver_schema}.ranking_individuals",
    f"{catalog_name}.{silver_schema}.ranking_pairs",
    f"{catalog_name}.{silver_schema}.player_event_results",   # Phase 12: replaces points_ledger
    f"{catalog_name}.{silver_schema}.events",                 # Phase 12: event level (one row per event)
    f"{catalog_name}.{silver_schema}.events_metadata",        # Phase 12: row level
    f"{catalog_name}.{silver_schema}.individuals_event_penalties",
] + [
    f"{catalog_name}.{silver_schema}.{table_key}" for table_key in sorted(SILVER_REFERENCE_TABLES)
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
for full_table_name in ALL_SILVER_TABLES:
    try:
        row_count = spark.table(full_table_name).count()
        if row_count == 0:
            failures.append(f"{full_table_name}: 0 rows")
        else:
            print(f"OK    {full_table_name}: {row_count} rows")
    except Exception as e:
        failures.append(f"{full_table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 2 - no null business keys on the load-bearing tables

# COMMAND ----------

null_key_checks = [
    (f"{catalog_name}.{silver_schema}.player_identity", "ittfid"),
    (f"{catalog_name}.{silver_schema}.ranking_individuals", "ittfid"),
    (f"{catalog_name}.{silver_schema}.ranking_pairs", "pair_id"),
    (f"{catalog_name}.{silver_schema}.player_event_results", "ittfid"),
    (f"{catalog_name}.{silver_schema}.player_event_results", "event_id"),
    (f"{catalog_name}.{silver_schema}.events", "event_id"),
    (f"{catalog_name}.{silver_schema}.events_metadata", "event_id"),
]

for full_table_name, key_col in null_key_checks:
    null_count = spark.table(full_table_name).where(F.col(key_col).isNull()).count()
    if null_count > 0:
        failures.append(f"{full_table_name}.{key_col} has {null_count} null value(s)")
    else:
        print(f"OK    {full_table_name}.{key_col}: no nulls")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 3 - identity resolution rate
# MAGIC Not a hard failure by itself (an unresolved `ittfid` is a data quality
# MAGIC signal for Gold to decide how to handle, per `06.silver-helpers`'
# MAGIC `resolve_identity` docstring) -- but a HIGH unresolved rate would mean
# MAGIC something upstream broke (e.g. the identity dimension ran against a
# MAGIC stale/empty `bronze.competitors`), so it's still worth surfacing loudly.
# MAGIC Threshold: fail if more than 5% of any table's rows are unresolved.
# MAGIC
# MAGIC **`player_event_results` (Phase 12): WARN only, per ranking category.** WTT decided (2026-10-08) that unmapped
# MAGIC competitors are shown, not blocked: on the dashboard each unmapped ID lands in a bucket `ITTFID_<category>`
# MAGIC (individuals not in `Competitors`: MS/WS/MDI/WDI/XDI; pairs not in `Players_Doubles`: MD/WD/XD), with a drill-down
# MAGIC listing the IDs (`gold.v_evt_unmapped_competitors`). This check prints the unmapped IDs, rows and % per ranking
# MAGIC category every run and never fails, so there is no threshold or allowance to maintain. The old 65% PAIR allowance
# MAGIC (measured on the retired `points_ledger`) is removed.

# COMMAND ----------

identity_resolution_checks = [
    f"{catalog_name}.{silver_schema}.ranking_individuals",
    f"{catalog_name}.{silver_schema}.ranking_pairs",
    f"{catalog_name}.{silver_schema}.individuals_event_penalties",
]

UNRESOLVED_THRESHOLD_PCT = 5.0

for full_table_name in identity_resolution_checks:
    df = spark.table(full_table_name)
    total = df.count()
    unresolved = df.filter(F.col('identity_resolved') == False).count()  # noqa: E712
    pct = (unresolved / total * 100) if total else 0.0
    if pct > UNRESOLVED_THRESHOLD_PCT:
        failures.append(
            f"{full_table_name}: {unresolved}/{total} rows ({pct:.1f}%) failed identity "
            f"resolution -- exceeds the {UNRESOLVED_THRESHOLD_PCT}% threshold"
        )
    else:
        print(f"OK    {full_table_name}: {unresolved}/{total} rows ({pct:.1f}%) unresolved")

# Phase 12: unmapped competitors in player_event_results -> WARN per ranking category, never a failure.
results_table = f"{catalog_name}.{silver_schema}.player_event_results"
results_df = spark.table(results_table)
# Checked against silver.player_identity as it is NOW (same lookup the gold views do at query time), so IDs added to
# Competitors / Players_Doubles since their weeks were loaded are no longer reported.
_known = (spark.table(f"{catalog_name}.{silver_schema}.player_identity").select("ittfid", "entity_type").distinct()
               .withColumn("_known", F.lit(True)))
_res = results_df.join(_known, ["ittfid", "entity_type"], "left").withColumn("_unmapped", F.col("_known").isNull())
unmapped_stats = (_res.groupBy("ranking_category_code", "entity_type")
                    .agg(F.count("*").alias("rows_"),
                         F.sum(F.when(F.col("_unmapped"), 1).otherwise(0)).alias("unmapped_rows"),
                         F.countDistinct(F.when(F.col("_unmapped"), F.col("ittfid"))).alias("unmapped_ids"))
                    .orderBy("entity_type", "ranking_category_code").collect())
for r in unmapped_stats:
    pct = (r["unmapped_rows"] / r["rows_"] * 100) if r["rows_"] else 0.0
    tag = "WARN" if r["unmapped_rows"] else "OK  "
    print(f"{tag}  {results_table} [ITTFID_{r['ranking_category_code']}, {r['entity_type']}]: {r['unmapped_ids']:,} unmapped ID(s), "
          f"{r['unmapped_rows']:,}/{r['rows_']:,} rows ({pct:.1f}%) -- drill-down: gold.v_evt_unmapped_competitors")

unmapped_category_count = results_df.filter(F.col('entity_type').isNull()).count()
if unmapped_category_count > 0:
    failures.append(
        f"{results_table}: {unmapped_category_count} row(s) have an unmapped "
        f"ranking_category_code (entity_type is null) -- PAIR_/INDIVIDUAL_RANKING_CATEGORIES "
        f"in 08.Silver Player Event Results may need updating"
    )
else:
    print(f"OK    {results_table}: no rows with an unmapped ranking_category_code")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 4 - target-week-landed assertion (Step 8, freshness check)
# MAGIC Same pattern as bronze's Check 4 -- see that notebook's comment.

# COMMAND ----------

if HAVE_TARGET_WEEK:
    target_year, target_week = int(v_ranking_year), int(v_ranking_week)
    for full_table_name in [
        f"{catalog_name}.{silver_schema}.ranking_individuals",
        f"{catalog_name}.{silver_schema}.ranking_pairs",
    ]:
        landed_count = (
            spark.table(full_table_name)
                .where((F.col('ranking_year') == target_year) & (F.col('ranking_week') == target_week))
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
# MAGIC #### Check 5 - Phase 12: grain of `player_event_results` (target week) and `events_metadata`

# COMMAND ----------

per_table = f"{catalog_name}.{silver_schema}.player_event_results"
per_scope = spark.table(per_table)
if HAVE_TARGET_WEEK:
    per_scope = per_scope.where(F.col("week_key") == int(v_ranking_year) * 100 + int(v_ranking_week))
per_dups = (per_scope.groupBy("ittfid", "event_id", "subevent_code", "ranking_category_code", "age_category_code",
                              "category_code", "result_position", "ranking_year", "ranking_week",
                              "result_type", "result_slot")
                     .count().filter(F.col("count") > 1).count())
if per_dups:
    failures.append(f"{per_table}: {per_dups} duplicated result key(s)")
else:
    print(f"OK    {per_table}: result grain unique" + (" in the target week" if HAVE_TARGET_WEEK else ""))

# WARN: the counting flag must give one row per player x event x subevent x ranking category (Gold "how many" views).
# By construction this is 0; a non-zero count means incremental runs drifted -> run 03-silver/08 with blank p_week_keys.
COUNT_KEY = ["ittfid", "event_id", "subevent_code", "ranking_category_code"]
_rr = spark.table(per_table).where(F.col("is_result_row"))
rr_multi = _rr.groupBy(*COUNT_KEY).count().filter(F.col("count") > 1).count()
if rr_multi:
    print(f"WARN  {per_table}: {rr_multi} result(s) with more than one is_result_row -- run 03-silver/08 with blank p_week_keys (full rebuild)")
else:
    print(f"OK    {per_table}: one is_result_row per player x event x subevent x ranking category")
# INFO: how many results changed age/category label over their life (these would double count without is_result_row).
_relabel = (spark.table(per_table).where(F.col("is_first_appearance") & F.col("is_primary_copy"))
                 .groupBy(*COUNT_KEY).count().filter(F.col("count") > 1).count())
print(f"INFO  {per_table}: {_relabel:,} result(s) whose copies first appear in different weeks (counted once via is_result_row)")

em_table = f"{catalog_name}.{silver_schema}.events_metadata"
em_dups = (spark.table(em_table).groupBy("event_id", "ranking_category_code", "age_category_code")
             .count().filter(F.col("count") > 1).count())
if em_dups:
    failures.append(f"{em_table}: {em_dups} duplicated (event_id, ranking_category_code, age_category_code) key(s) -- "
                    f"03-silver/09 should have kept one row per key")
else:
    print(f"OK    {em_table}: (event_id, ranking_category_code, age_category_code) unique")
ev_table = f"{catalog_name}.{silver_schema}.events"
ev_dups = spark.table(ev_table).groupBy("event_id").count().filter(F.col("count") > 1).count()
if ev_dups:
    failures.append(f"{ev_table}: {ev_dups} duplicated event_id(s) -- 03-silver/09 should have kept one row per event")
else:
    print(f"OK    {ev_table}: one row per event_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Result + `control.batch_control` status update

# COMMAND ----------

if failures:
    if HAVE_TARGET_WEEK:
        spark.sql(f"""
            UPDATE wttrankingsbi.control.batch_control
            SET status = 'failed', failed_at = current_timestamp(),
                failure_stage = 'silver', failure_message = {chr(39)}{'; '.join(failures)[:4000].replace(chr(39), chr(39)+chr(39))}{chr(39)},
                updated_at = current_timestamp()
            WHERE ranking_year = {int(v_ranking_year)} AND ranking_week = {int(v_ranking_week)}
        """)
    raise ValueError("Silver validation FAILED:\n" + "\n".join(f"  - {f}" for f in failures))

if HAVE_TARGET_WEEK:
    spark.sql(f"""
        UPDATE wttrankingsbi.control.batch_control
        SET status = 'silver_done', silver_done_at = current_timestamp(), updated_at = current_timestamp()
        WHERE ranking_year = {int(v_ranking_year)} AND ranking_week = {int(v_ranking_week)}
    """)

print(f"Silver validation passed: {len(ALL_SILVER_TABLES)} tables checked.")