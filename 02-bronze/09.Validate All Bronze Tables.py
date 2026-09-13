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
# MAGIC **Change (2026-09-12, MainRanking historical backfill exercise):**
# MAGIC `bronze.ranking_individuals`/`ranking_pairs` now accumulate across
# MAGIC multiple weeks instead of being replaced each run (see the modified
# MAGIC `01`/`02` notebooks) -- Check 3 below is new, and shows the distinct
# MAGIC weeks currently sitting in each, so a run's output makes it obvious
# MAGIC whether accumulation is behaving as expected. `bronze.
# MAGIC main_ranking_historical` is checked too, but only informationally, never
# MAGIC as a hard requirement -- it's populated by a separate, manually-run
# MAGIC notebook (`10.Ingest Main Ranking Historical`), not by this job, so a
# MAGIC normal weekly bronze run must not fail just because that table hasn't
# MAGIC been loaded (or loaded yet) in this workspace.

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
# MAGIC #### Check 3 (new) - accumulation visibility + optional MainRanking status
# MAGIC Informational only -- prints the distinct `(RankingYear, RankingWeek)`
# MAGIC pairs currently in `ranking_individuals`/`ranking_pairs` so a run makes
# MAGIC it obvious whether the accumulate-write fix is behaving (more than one
# MAGIC week present after more than one week has been loaded). Also checks
# MAGIC `main_ranking_historical` if it exists, but never fails this job if it
# MAGIC doesn't -- see the header note above.

# COMMAND ----------

for full_table_name in [
    f"{catalog_name}.{bronze_schema}.ranking_individuals",
    f"{catalog_name}.{bronze_schema}.ranking_pairs",
]:
    weeks_df = (
        spark.table(full_table_name)
             .select('RankingYear', 'RankingWeek')
             .distinct()
             .orderBy('RankingYear', 'RankingWeek')
    )
    week_count = weeks_df.count()
    print(f"INFO  {full_table_name}: {week_count} distinct (RankingYear, RankingWeek) pair(s) present")
    display(weeks_df)

main_ranking_table = f"{catalog_name}.{bronze_schema}.main_ranking_historical"
try:
    mr_count = spark.table(main_ranking_table).count()
    mr_null_key = spark.table(main_ranking_table).where(F.col('CompetitorId').isNull()).count()
    print(f"INFO  {main_ranking_table}: {mr_count} rows, {mr_null_key} null CompetitorId")
    if mr_count > 0 and mr_null_key > 0:
        failures.append(f"{main_ranking_table}.CompetitorId has {mr_null_key} null value(s)")
except Exception:
    print(f"INFO  {main_ranking_table}: not yet loaded in this workspace (not required by this job)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Result

# COMMAND ----------

if failures:
    raise ValueError("Bronze validation FAILED:\n" + "\n".join(f"  - {f}" for f in failures))

print(f"Bronze validation passed: {len(ALL_BRONZE_TABLES)} tables checked.")
