# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Ranking Pairs
# MAGIC Source: `bronze.ranking_pairs` (already SEN+YOU unioned in bronze)
# MAGIC **+ `bronze.main_ranking_historical`, as of the MainRanking backfill
# MAGIC exercise (2026-09-12) -- same merge pattern as `02.Silver Ranking
# MAGIC Individuals.py`; see that notebook and `README.md`'s "Design decisions"
# MAGIC for the full rationale.**
# MAGIC 1. Read bronze, rename to snake_case
# MAGIC 2. Read `bronze.main_ranking_historical`'s pair-discipline rows
# MAGIC    (MD/WD/XD) and map them onto the same column shape -- both partners'
# MAGIC    geography (this source has none) left NULL here
# MAGIC 3. Union both sources, tagged `_source_system`
# MAGIC 4. Add a uniform `ittfid` column (== `pair_id`), resolve against
# MAGIC    `silver.player_identity` once, on the unioned data
# MAGIC 5. Dedupe on the natural key, preferring landing-CSV on a collision but
# MAGIC    flagging a rank/points disagreement first
# MAGIC 6. Backfill `previous_rank` / `ranking_difference` via LAG over the
# MAGIC    final deduped series (pairs have no `RankingPoints_Previous`-style
# MAGIC    stored column to backfill -- only individuals do)
# MAGIC 7. Write to `silver.ranking_pairs`
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

landing_source_table = f"{catalog_name}.{bronze_schema}.ranking_pairs"
main_ranking_source_table = f"{catalog_name}.{bronze_schema}.main_ranking_historical"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.ranking_pairs"

PAIR_SUBEVENT_CODES = ['MD', 'WD', 'XD']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the landing-CSV branch, snake_case, normalize
# MAGIC Unchanged from before the MainRanking exercise, other than the added
# MAGIC `_source_system` tag.

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

LANDING_COLUMNS = landing_renamed_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Read the MainRanking branch, map onto the same column shape
# MAGIC `CompetitorId` for an MD/WD/XD row is confirmed to be `Players_Doubles.
# MAGIC DoublesId` -- the same id space as the landing CSV's own `PairId` -- so
# MAGIC it maps directly to `pair_id`/`ittfid`, no polymorphism resolution
# MAGIC needed beyond what `resolve_identity` already does. Neither partner's
# MAGIC identity/geography is available from this source at all (MainRanking
# MAGIC carries no `Player1Id`/`Player2Id`) -- `partner1_ittfid`/
# MAGIC `partner2_ittfid` and both partners' geography are left NULL for
# MAGIC MainRanking-sourced rows; this means `gold.dim_pair`'s `is_cross_country`
# MAGIC split cannot be computed for these rows (documented as a known
# MAGIC limitation, not silently masked, in `README.md`).

# COMMAND ----------

main_ranking_bronze_df = spark.table(main_ranking_source_table)

_null_geo_cols = [
    F.lit(None).cast('string').alias('partner1_ittfid'),
    F.lit(None).cast('string').alias('player_name1'),
    F.lit(None).cast('string').alias('country_code1'),
    F.lit(None).cast('string').alias('country_name1'),
    F.lit(None).cast('string').alias('association_country_code1'),
    F.lit(None).cast('string').alias('association_country_name1'),
    F.lit(None).cast('string').alias('nationality_code1'),
    F.lit(None).cast('string').alias('nationality_name1'),
    F.lit(None).cast('string').alias('continent_id1'),
    F.lit(None).cast('string').alias('partner2_ittfid'),
    F.lit(None).cast('string').alias('player_name1d'),
    F.lit(None).cast('string').alias('country_code1d'),
    F.lit(None).cast('string').alias('country_name1d'),
    F.lit(None).cast('string').alias('association_country_code1d'),
    F.lit(None).cast('string').alias('association_country_name1d'),
    F.lit(None).cast('string').alias('nationality_code1d'),
    F.lit(None).cast('string').alias('nationality_name1d'),
    F.lit(None).cast('string').alias('continent_id1d'),
]

main_ranking_pairs_df = (
    main_ranking_bronze_df
        .filter(F.col('RankingCategory').isin(PAIR_SUBEVENT_CODES))
        .select(
            F.col('CompetitorId').cast('string').alias('pair_id'),
            *_null_geo_cols,
            F.col('CategoryCode').alias('category_code'),
            F.when(F.col('AgeCategoryCode') == '', None).otherwise(F.col('AgeCategoryCode')).alias('age_category_code'),
            F.col('RankingCategory').alias('subevent_code'),
            F.col('RankingYear').alias('ranking_year'),
            F.col('RankingMonth').alias('ranking_month'),
            F.col('RankingWeek').alias('ranking_week'),
            F.col('RankingPoints').alias('points'),
            F.lit(None).cast('int').alias('ranking_position'),
            F.col('RankingPos').alias('current_rank'),
            F.lit(None).cast('int').alias('previous_rank'),
            F.lit(None).cast('int').alias('ranking_difference'),
            F.lit(None).cast('timestamp').alias('publish_date'),
            F.lit(None).cast('timestamp').alias('updated_date'),
            F.lit(None).cast('string').alias('updated_by'),
            F.lit(None).cast('boolean').alias('is_active'),
            F.col('CategoryCode').alias('ranking_run_code'),
            F.col('_source_file'),
            F.col('_ingestion_timestamp'),
            F.col('CompetitorId').cast('string').alias('ittfid'),
            F.lit('PAIR').alias('entity_type'),
            F.lit('MAIN_RANKING_HISTORICAL').alias('_source_system'),
        )
)

assert set(main_ranking_pairs_df.columns) == set(LANDING_COLUMNS), (
    f"Column mismatch between landing-CSV branch and MainRanking branch: "
    f"{set(LANDING_COLUMNS) ^ set(main_ranking_pairs_df.columns)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Union both sources

# COMMAND ----------

unioned_df = landing_renamed_df.select(*LANDING_COLUMNS).unionByName(
    main_ranking_pairs_df.select(*LANDING_COLUMNS)
)

display(unioned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Resolve `ittfid` (== pair_id) against the player identity
# MAGIC dimension -- once, on the unioned data

# COMMAND ----------

player_identity_df = spark.table(identity_table)

resolved_df = resolve_identity(unioned_df, 'ittfid', 'entity_type', player_identity_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Dedupe, preferring landing-CSV on a source collision, but
# MAGIC flagging (not silently discarding) a rank/points disagreement first

# COMMAND ----------

deduped_df = dedup_preferring_landing_csv(
    resolved_df,
    key_cols=['pair_id', 'subevent_code', 'ranking_year', 'ranking_week', 'ranking_run_code'],
    compare_cols=['current_rank', 'points'],
    mismatch_col='_dq_source_mismatch',
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Backfill `previous_rank` / `ranking_difference` via LAG
# MAGIC Pairs have no persisted "points previous" column (the landing CSV's
# MAGIC pairs schema never had one -- only individuals' `RankingPoints_Previous`
# MAGIC does), so the LAG'd points value is computed into a throwaway column
# MAGIC used only to derive `ranking_difference`, then dropped.

# COMMAND ----------

with_previous_df = backfill_previous_period_columns(
    deduped_df,
    key_cols=['pair_id', 'subevent_code', 'ranking_run_code'],
    order_cols=['ranking_year', 'ranking_week'],
    current_col='current_rank', current_prev_col='previous_rank',
    points_col='points', points_prev_col='_temp_points_previous',
    difference_col='ranking_difference',
).drop('_temp_points_previous')

silver_ranking_pairs_df = add_silver_metadata(with_previous_df)

display(silver_ranking_pairs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Write to silver delta table
# MAGIC Still a full `overwrite` every run -- same reasoning as `02.Silver
# MAGIC Ranking Individuals.py` Step 8.

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
