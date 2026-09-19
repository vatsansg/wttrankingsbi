# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Ranking Pairs (Step 8 optimized)
# MAGIC Same optimization as `02.Silver Ranking Individuals.py` in this folder --
# MAGIC see that notebook's header for the full rationale and the parity-check
# MAGIC requirement before this replaces the live one.
# MAGIC
# MAGIC 1. Read bronze, rename to snake_case (unchanged)
# MAGIC 2. Read the frozen `silver.main_ranking_historical_pairs_frozen` table
# MAGIC 2b. Defensive overlap assertion (same as individuals)
# MAGIC 3. Resolve `ittfid` (== `pair_id`) against `silver.player_identity` --
# MAGIC    landing branch only
# MAGIC 4. Backfill `previous_rank`/`ranking_difference` via LAG over the
# MAGIC    landing branch's own accumulated weeks only
# MAGIC 5. Union the two already-finalized branches
# MAGIC 6. Write to `silver.ranking_pairs`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

# MAGIC %run ../00-common/09.mainranking-merge-helpers

# COMMAND ----------

landing_source_table = f"{catalog_name}.{bronze_schema}.ranking_pairs"
frozen_historical_table = f"{catalog_name}.{silver_schema}.main_ranking_historical_pairs_frozen"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.ranking_pairs"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the landing-CSV branch, snake_case, normalize
# MAGIC Unchanged from the original notebook.

# COMMAND ----------

landing_bronze_df = spark.table(landing_source_table)

landing_renamed_df = (
    rename_to_snake_case(landing_bronze_df)
        .withColumnRenamed('ittf_id1', 'partner1_ittfid')
        .withColumnRenamed('ittf_id1d', 'partner2_ittfid')
        .withColumnRenamed('sub_event_code', 'subevent_code')
        .withColumn('ittfid', F.col('pair_id'))
        .withColumn('entity_type', F.lit('PAIR'))
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
# MAGIC #### Step 2b - Defensive overlap assertion (same rationale as
# MAGIC individuals -- see that notebook)

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
        f"Landing and the frozen MainRanking-pairs branch both have data for "
        f"{sorted(overlap)} -- the zero-overlap assumption this optimized "
        f"notebook depends on no longer holds. Do not proceed silently: "
        f"re-run 00.Materialize MainRanking Silver Contribution.py if the "
        f"historical cutoff genuinely changed, or fall back to the "
        f"pre-Step-8 full-recompute notebook for this run."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Resolve `ittfid` (== `pair_id`) against the player
# MAGIC identity dimension -- landing branch only

# COMMAND ----------

player_identity_df = spark.table(identity_table)

resolved_landing_df = (
    resolve_identity(landing_renamed_df, 'ittfid', 'entity_type', player_identity_df)
        .withColumn('_dq_source_mismatch', F.lit(False))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Backfill `previous_rank`/`ranking_difference`, landing
# MAGIC branch only. Same accepted narrow edge case as individuals -- see that
# MAGIC notebook's Step 5 note.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Backfill `previous_rank`/`ranking_difference`, seeded
# MAGIC from frozen. Same fix and rationale as individuals' Step 5 -- see that
# MAGIC notebook's comment.

# COMMAND ----------

landing_keys_df = resolved_landing_df.select('pair_id', 'subevent_code', 'ranking_run_code').distinct()

frozen_seed_window = Window.partitionBy('pair_id', 'subevent_code', 'ranking_run_code').orderBy(
    F.col('ranking_year').desc(), F.col('ranking_week').desc()
)

frozen_seed_df = (
    frozen_historical_df
        .join(landing_keys_df, on=['pair_id', 'subevent_code', 'ranking_run_code'], how='inner')
        .withColumn('_seed_rn', F.row_number().over(frozen_seed_window))
        .filter(F.col('_seed_rn') == 1)
        .drop('_seed_rn')
        .select(*resolved_landing_df.columns)
        .withColumn('_is_seed_row', F.lit(True))
)

landing_with_seed_df = (
    resolved_landing_df
        .withColumn('_is_seed_row', F.lit(False))
        .unionByName(frozen_seed_df)
)

with_previous_landing_and_seed_df = backfill_previous_period_columns(
    landing_with_seed_df,
    key_cols=['pair_id', 'subevent_code', 'ranking_run_code'],
    order_cols=['ranking_year', 'ranking_week'],
    current_col='current_rank', current_prev_col='previous_rank',
    points_col='points', points_prev_col='_temp_points_previous',
    difference_col='ranking_difference',
).drop('_temp_points_previous')

with_previous_landing_df = (
    with_previous_landing_and_seed_df
        .filter(F.col('_is_seed_row') == False)  # noqa: E712
        .drop('_is_seed_row')
)

silver_landing_df = add_silver_metadata(with_previous_landing_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Union the two already-finalized branches

# COMMAND ----------

FINAL_COLUMNS = silver_landing_df.columns
assert set(frozen_historical_df.columns) == set(FINAL_COLUMNS), (
    f"Column mismatch between the landing branch's output and the frozen "
    f"historical table -- re-run 00.Materialize MainRanking Silver "
    f"Contribution.py after checking for an upstream schema change: "
    f"{set(FINAL_COLUMNS) ^ set(frozen_historical_df.columns)}"
)

silver_ranking_pairs_df = silver_landing_df.select(*FINAL_COLUMNS).unionByName(
    frozen_historical_df.select(*FINAL_COLUMNS)
)

display(silver_ranking_pairs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Write to silver delta table
# MAGIC Unchanged -- still a full `overwrite` every run.

# COMMAND ----------

(
    silver_ranking_pairs_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())