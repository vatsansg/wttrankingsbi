# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Ranking Individuals
# MAGIC Source: `bronze.ranking_individuals` (already SEN+YOU unioned in bronze)
# MAGIC **+ `bronze.main_ranking_historical`, as of the MainRanking backfill
# MAGIC exercise (2026-09-12) -- see `README.md`'s "Design decisions" section.**
# MAGIC 1. Read bronze, rename to snake_case
# MAGIC 2. Read `bronze.main_ranking_historical`'s individual-discipline rows
# MAGIC    (MS/WS/MDI/WDI/XDI) and map them onto the *same* column shape --
# MAGIC    geography/audit columns this source doesn't have are left NULL here
# MAGIC    and backfilled in later steps, never guessed at this stage
# MAGIC 3. Union both sources, tagged `_source_system`
# MAGIC 4. Normalize the identity column to `ittfid` and resolve it against
# MAGIC    `silver.player_identity` -- once, on the unioned data
# MAGIC 5. Backfill geography from the identity resolution for rows that don't
# MAGIC    have their own (MainRanking-sourced rows only -- landing-CSV rows
# MAGIC    already carry their own and are left untouched)
# MAGIC 6. Dedupe on the natural key, preferring the landing-CSV row on a
# MAGIC    collision but flagging (not silently discarding) any rank/points
# MAGIC    disagreement between the two sources first
# MAGIC 7. Backfill `previous_rank` / `ranking_points__previous` /
# MAGIC    `ranking_difference` via a LAG over the final deduped series, for
# MAGIC    rows that don't already have their own published values
# MAGIC 8. Write to `silver.ranking_individuals`
# MAGIC
# MAGIC Depends on notebook 01 (`Build Player Identity Dimension`) having run
# MAGIC first in this same job -- see `03-jobs/`.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

# MAGIC %run ../00-common/09.mainranking-merge-helpers

# COMMAND ----------

landing_source_table = f"{catalog_name}.{bronze_schema}.ranking_individuals"
main_ranking_source_table = f"{catalog_name}.{bronze_schema}.main_ranking_historical"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.ranking_individuals"

INDIVIDUAL_SUBEVENT_CODES = ['MS', 'WS', 'MDI', 'WDI', 'XDI']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the landing-CSV branch, snake_case, normalize
# MAGIC Unchanged from before the MainRanking exercise, other than the added
# MAGIC `_source_system` tag.

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

LANDING_COLUMNS = landing_renamed_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Read the MainRanking branch, map onto the same column shape
# MAGIC `bronze.main_ranking_historical` has no geography/name columns and no
# MAGIC `RankingPosition` / `PreviousRank` / `RankingPoints_Previous` /
# MAGIC `RankingDifference` / `PublishDate` / `UpdatedDate` / `UpdatedBy` /
# MAGIC `IsActive` / `RankingPointsCareer` equivalents -- all left NULL here.
# MAGIC `RankingPos` and `RankingPoints` are confirmed (by direct hand-trace
# MAGIC against real 2026 week 31 data, where both sources overlap: all 13,944
# MAGIC matched rows agreed exactly) to be the same figures as `CurrentRank` and
# MAGIC `RankingPointsYTD` respectively on the landing-CSV side -- **not** a
# MAGIC guess.
# MAGIC
# MAGIC `CategoryCode` maps to *both* `category_code` and `ranking_run_code` --
# MAGIC the landing-CSV side gets `ranking_run_code` as a separate tag parsed
# MAGIC from which file (SEN/YOU) a row came from, but it's confirmed the same
# MAGIC value as that row's own `CategoryCode` column, so MainRanking's single
# MAGIC `CategoryCode` column correctly fills both.

# COMMAND ----------

main_ranking_bronze_df = spark.table(main_ranking_source_table)

main_ranking_individuals_df = (
    main_ranking_bronze_df
        .filter(F.col('RankingCategory').isin(INDIVIDUAL_SUBEVENT_CODES))
        .select(
            F.col('CompetitorId').cast('string').alias('ittfid'),
            F.lit(None).cast('string').alias('player_name'),
            F.lit(None).cast('string').alias('country_code'),
            F.lit(None).cast('string').alias('country_name'),
            F.lit(None).cast('string').alias('association_country_code'),
            F.lit(None).cast('string').alias('association_country_name'),
            F.lit(None).cast('string').alias('nationality_code'),
            F.lit(None).cast('string').alias('nationality_name'),
            F.lit(None).cast('string').alias('continent_id'),
            F.col('CategoryCode').alias('category_code'),
            F.when(F.col('AgeCategoryCode') == '', None).otherwise(F.col('AgeCategoryCode')).alias('age_category_code'),
            F.col('RankingCategory').alias('subevent_code'),
            F.col('RankingYear').alias('ranking_year'),
            F.col('RankingMonth').alias('ranking_month'),
            F.col('RankingWeek').alias('ranking_week'),
            F.lit(None).cast('decimal(10,2)').alias('ranking_points_career'),
            F.col('RankingPoints').alias('ranking_points_ytd'),
            F.lit(None).cast('int').alias('ranking_position'),
            F.col('RankingPos').alias('current_rank'),
            F.lit(None).cast('int').alias('previous_rank'),
            F.lit(None).cast('decimal(10,2)').alias('ranking_points__previous'),
            F.lit(None).cast('int').alias('ranking_difference'),
            F.lit(None).cast('timestamp').alias('publish_date'),
            F.lit(None).cast('timestamp').alias('updated_date'),
            F.lit(None).cast('string').alias('updated_by'),
            F.lit(None).cast('boolean').alias('is_active'),
            F.col('CategoryCode').alias('ranking_run_code'),
            F.col('_source_file'),
            F.col('_ingestion_timestamp'),
            F.lit('INDIVIDUAL').alias('entity_type'),
            F.lit('MAIN_RANKING_HISTORICAL').alias('_source_system'),
        )
)

# Column-order/set safety net: fail loudly here rather than have unionByName
# quietly do the wrong thing if the two branches ever drift apart.
assert set(main_ranking_individuals_df.columns) == set(LANDING_COLUMNS), (
    f"Column mismatch between landing-CSV branch and MainRanking branch: "
    f"{set(LANDING_COLUMNS) ^ set(main_ranking_individuals_df.columns)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Union both sources

# COMMAND ----------

unioned_df = landing_renamed_df.select(*LANDING_COLUMNS).unionByName(
    main_ranking_individuals_df.select(*LANDING_COLUMNS)
)

display(unioned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Resolve `ittfid` against the player identity dimension
# MAGIC Run once, on the unioned data -- not once per branch.

# COMMAND ----------

player_identity_df = spark.table(identity_table)

resolved_df = resolve_identity(unioned_df, 'ittfid', 'entity_type', player_identity_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Backfill geography for rows that don't have their own
# MAGIC `coalesce(existing, identity_*)` -- a no-op for landing-CSV rows (which
# MAGIC already have their own values), fills in MainRanking-sourced rows from
# MAGIC the identity dimension. `identity_continent_id` is a resolved current
# MAGIC snapshot from `player_identity`, not a point-in-time value for the
# MAGIC historical week -- documented as a known limitation in `README.md`.

# COMMAND ----------

backfilled_df = (
    resolved_df
        .withColumn('player_name', F.coalesce(F.col('player_name'), F.col('identity_player_name')))
        .withColumn('country_code', F.coalesce(F.col('country_code'), F.col('identity_country_code')))
        .withColumn('continent_id', F.coalesce(F.col('continent_id'), F.col('identity_continent_id')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Dedupe, preferring landing-CSV on a source collision, but
# MAGIC flagging (not silently discarding) a rank/points disagreement first

# COMMAND ----------

deduped_df = dedup_preferring_landing_csv(
    backfilled_df,
    key_cols=['ittfid', 'subevent_code', 'ranking_year', 'ranking_week', 'ranking_run_code'],
    compare_cols=['current_rank', 'ranking_points_ytd'],
    mismatch_col='_dq_source_mismatch',
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Backfill previous-period columns via LAG, on the final
# MAGIC deduped series -- computed AFTER dedup, not on the MainRanking branch
# MAGIC alone, so a week's "previous rank" always points at whichever row
# MAGIC actually survived dedup for the prior week, from either source.

# COMMAND ----------

with_previous_df = backfill_previous_period_columns(
    deduped_df,
    key_cols=['ittfid', 'subevent_code', 'ranking_run_code'],
    order_cols=['ranking_year', 'ranking_week'],
    current_col='current_rank', current_prev_col='previous_rank',
    points_col='ranking_points_ytd', points_prev_col='ranking_points__previous',
    difference_col='ranking_difference',
)

silver_ranking_individuals_df = add_silver_metadata(with_previous_df)

display(silver_ranking_individuals_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write to silver delta table
# MAGIC Still a full `overwrite` of `silver.ranking_individuals` every run --
# MAGIC unlike bronze, silver doesn't need to accumulate its own history across
# MAGIC runs, because it always recomputes completely from whatever bronze
# MAGIC currently holds (which, after the bronze accumulate-write fix, is now
# MAGIC the full multi-week history). This step's logic did not need an
# MAGIC accumulate-write change of its own -- only its two upstream bronze
# MAGIC sources did.

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
# MAGIC Rows where both sources existed for the same key and disagreed on
# MAGIC rank/points -- the landing-CSV value won, but this count should be
# MAGIC reviewed, not ignored. Also see `07.Validate All Silver Tables`.

# COMMAND ----------

display(
    spark.table(target_table)
         .filter(F.col('_dq_source_mismatch') == True)  # noqa: E712
         .count()
)
