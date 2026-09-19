# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Ranking Individuals (Step 8 optimized)
# MAGIC Source: `bronze.ranking_individuals` (already SEN+YOU unioned in bronze)
# MAGIC **+ the frozen `silver.main_ranking_historical_individuals_frozen`**
# MAGIC (built once by `00.Materialize MainRanking Silver Contribution.py`),
# MAGIC instead of re-deriving the MainRanking branch from
# MAGIC `bronze.main_ranking_historical` on every run.
# MAGIC
# MAGIC **Verify with the parity check in this folder's `README.md` before this
# MAGIC notebook replaces the live one.** See `00.Materialize...`'s header for
# MAGIC why this is expected to be safe, not just faster.
# MAGIC
# MAGIC 1. Read bronze, rename to snake_case (unchanged)
# MAGIC 2. Read the frozen MainRanking-individuals table (no recompute)
# MAGIC 2b. **Defensive overlap assertion** -- fail loudly, don't silently
# MAGIC     mis-dedupe, if a landing week ever falls inside the frozen range
# MAGIC 3. Resolve `ittfid` against `silver.player_identity` -- landing branch
# MAGIC    only (the frozen branch already carries its own resolution)
# MAGIC 4. Backfill geography for the landing branch (no-op in practice -- see
# MAGIC    note below)
# MAGIC 5. Backfill `previous_rank`/`ranking_points__previous`/
# MAGIC    `ranking_difference` via LAG over the landing branch's own
# MAGIC    accumulated weeks (not the frozen branch -- see note below)
# MAGIC 6. Union the two already-finalized branches
# MAGIC 7. Write to `silver.ranking_individuals` -- same full overwrite as before
# MAGIC
# MAGIC Depends on notebook 01 (`Build Player Identity Dimension`) having run
# MAGIC first in this same job -- unchanged from before.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

# MAGIC %run ../00-common/09.mainranking-merge-helpers

# COMMAND ----------

landing_source_table = f"{catalog_name}.{bronze_schema}.ranking_individuals"
frozen_historical_table = f"{catalog_name}.{silver_schema}.main_ranking_historical_individuals_frozen"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.ranking_individuals"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the landing-CSV branch, snake_case, normalize
# MAGIC Unchanged from the original notebook.

# COMMAND ----------

landing_bronze_df = spark.table(landing_source_table)

landing_renamed_df = (
    rename_to_snake_case(landing_bronze_df)
        .withColumnRenamed('ittf_id', 'ittfid')
        .withColumnRenamed('sub_event_code', 'subevent_code')
        .withColumn('entity_type', F.lit('INDIVIDUAL'))
        .withColumn('is_active', F.col('is_active').cast('boolean'))
        .withColumn('_source_system', F.lit('LANDING_CSV'))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Read the frozen MainRanking branch (no recompute)

# COMMAND ----------

frozen_historical_df = spark.table(frozen_historical_table)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2b - Defensive overlap assertion
# MAGIC `bronze.main_ranking_historical` is loaded up to a fixed cutoff and
# MAGIC landing-CSV starts after it, so under normal operation these two sets of
# MAGIC weeks never intersect -- this notebook relies on that to skip re-running
# MAGIC dedup/backfill over the frozen branch every week. If that assumption is
# MAGIC ever violated (e.g. the historical loader is re-run with a higher
# MAGIC cutoff, or a landing file is retroactively published for an
# MAGIC already-historical week), **fail here rather than silently produce a
# MAGIC row this notebook never deduped or backfilled against the other
# MAGIC source.** If this ever fires for real, fall back to the pre-Step-8
# MAGIC notebook (still kept as `03-silver-transformation`'s Step 2b-merged
# MAGIC version) for that run while the overlap is investigated.

# COMMAND ----------

landing_weeks = {
    (r['ranking_year'], r['ranking_week'])
    for r in landing_renamed_df.select('ranking_year', 'ranking_week').distinct().collect()
}
frozen_weeks = {
    (r['ranking_year'], r['ranking_week'])
    for r in frozen_historical_df.select('ranking_year', 'ranking_week').distinct().collect()
}
overlap = landing_weeks & frozen_weeks
if overlap:
    raise ValueError(
        f"Landing and the frozen MainRanking branch both have data for "
        f"{sorted(overlap)} -- the zero-overlap assumption this optimized "
        f"notebook depends on no longer holds. Do not proceed silently: "
        f"re-run 00.Materialize MainRanking Silver Contribution.py if the "
        f"historical cutoff genuinely changed, or fall back to the "
        f"pre-Step-8 full-recompute notebook for this run."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Resolve `ittfid` against the player identity dimension
# MAGIC Landing branch only -- unchanged logic, just no longer re-run against the
# MAGIC frozen branch's 3.6M+ rows every week.

# COMMAND ----------

player_identity_df = spark.table(identity_table)

resolved_landing_df = resolve_identity(landing_renamed_df, 'ittfid', 'entity_type', player_identity_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Backfill geography for the landing branch
# MAGIC In practice a no-op -- landing-CSV rows already carry their own
# MAGIC geography -- kept for parity with the original notebook's column
# MAGIC handling and as a safety net if that ever isn't true for some row.

# COMMAND ----------

backfilled_landing_df = (
    resolved_landing_df
        .withColumn('player_name', F.coalesce(F.col('player_name'), F.col('identity_player_name')))
        .withColumn('country_code', F.coalesce(F.col('country_code'), F.col('identity_country_code')))
        .withColumn('continent_id', F.coalesce(F.col('continent_id'), F.col('identity_continent_id')))
        .withColumn('_dq_source_mismatch', F.lit(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Backfill previous-period columns, landing branch only
# MAGIC The landing-CSV schema already publishes its own `PreviousRank`/
# MAGIC `RankingPoints_Previous`/`RankingDifference` for essentially every row,
# MAGIC so this LAG is a no-op for almost all of them via the existing
# MAGIC coalesce-only-fills-null behavior in `backfill_previous_period_columns`
# MAGIC -- same as before. **Known, accepted, narrow edge case vs. the original
# MAGIC notebook:** if the very first landing week's own `PreviousRank` were
# MAGIC ever null in the source (it hasn't been in production so far), this
# MAGIC version would no longer backfill it from the frozen branch's last week --
# MAGIC the original notebook's union-wide LAG would have. Not built as a join
# MAGIC against the frozen branch's boundary row here, to keep this change a
# MAGIC pure performance win rather than adding new, untested join logic --
# MAGIC revisit only if this edge case is ever actually observed.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Backfill previous-period columns, seeded from frozen
# MAGIC **Fixed 2026-09-19 (post-parity-check finding):** the landing-only LAG
# MAGIC left `previous_rank`/`ranking_points__previous`/`ranking_difference` null
# MAGIC for any key whose true previous period sits in the frozen branch --
# MAGIC not just the literal first landing week, but any player who skipped a
# MAGIC landing week entirely (confirmed via the parity check: 122 individuals
# MAGIC across weeks 31-32). Fix: seed the LAG with each landing key's single
# MAGIC most recent frozen-branch row before computing, then drop the seed rows.
# MAGIC Seed set is one row per distinct landing key, not the full frozen
# MAGIC table, so this stays cheap.

# COMMAND ----------
from pyspark.sql.window import Window
landing_keys_df = backfilled_landing_df.select('ittfid', 'subevent_code', 'ranking_run_code').distinct()

frozen_seed_window = Window.partitionBy('ittfid', 'subevent_code', 'ranking_run_code').orderBy(
    F.col('ranking_year').desc(), F.col('ranking_week').desc()
)

frozen_seed_df = (
    frozen_historical_df
        .join(landing_keys_df, on=['ittfid', 'subevent_code', 'ranking_run_code'], how='inner')
        .withColumn('_seed_rn', F.row_number().over(frozen_seed_window))
        .filter(F.col('_seed_rn') == 1)
        .drop('_seed_rn')
        .select(*backfilled_landing_df.columns)
        .withColumn('_is_seed_row', F.lit(True))
)

landing_with_seed_df = (
    backfilled_landing_df
        .withColumn('_is_seed_row', F.lit(False))
        .unionByName(frozen_seed_df)
)

with_previous_landing_and_seed_df = backfill_previous_period_columns(
    landing_with_seed_df,
    key_cols=['ittfid', 'subevent_code', 'ranking_run_code'],
    order_cols=['ranking_year', 'ranking_week'],
    current_col='current_rank', current_prev_col='previous_rank',
    points_col='ranking_points_ytd', points_prev_col='ranking_points__previous',
    difference_col='ranking_difference',
)

with_previous_landing_df = (
    with_previous_landing_and_seed_df
        .filter(F.col('_is_seed_row') == False)  # noqa: E712
        .drop('_is_seed_row')
)

silver_landing_df = add_silver_metadata(with_previous_landing_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Union the two already-finalized branches

# COMMAND ----------

FINAL_COLUMNS = silver_landing_df.columns
assert set(frozen_historical_df.columns) == set(FINAL_COLUMNS), (
    f"Column mismatch between the landing branch's output and the frozen "
    f"historical table -- re-run 00.Materialize MainRanking Silver "
    f"Contribution.py after checking for an upstream schema change: "
    f"{set(FINAL_COLUMNS) ^ set(frozen_historical_df.columns)}"
)

silver_ranking_individuals_df = silver_landing_df.select(*FINAL_COLUMNS).unionByName(
    frozen_historical_df.select(*FINAL_COLUMNS)
)

display(silver_ranking_individuals_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Write to silver delta table
# MAGIC Unchanged from the original -- still a full `overwrite` of
# MAGIC `silver.ranking_individuals` every run. Only the expensive computation
# MAGIC upstream changed, not the write pattern.

# COMMAND ----------

(
    silver_ranking_individuals_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())

# COMMAND ----------

display(
    spark.table(target_table)
         .filter(F.col('identity_resolved') == False)  # noqa: E712
         .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data-quality signal: source disagreements on overlapping weeks
# MAGIC Unchanged from the original -- should read 0 under normal operation
# MAGIC now, since Step 2b's assertion would already have failed the run if a
# MAGIC real overlap existed.

# COMMAND ----------

display(
    spark.table(target_table)
         .filter(F.col('_dq_source_mismatch') == True)  # noqa: E712
         .count()
)