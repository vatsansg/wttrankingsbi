# Databricks notebook source
# MAGIC %md
# MAGIC # Materialize MainRanking Silver Contribution
# MAGIC **Step 8 build (2026-09-19), per your decision to do this as part of
# MAGIC Step 8 rather than defer it.** Closes the review finding that every
# MAGIC weekly silver run was re-processing the full ~3.6M-row
# MAGIC `bronze.main_ranking_historical` table -- identity resolution, dedup
# MAGIC against landing, and a LAG-based previous-period backfill -- forever,
# MAGIC even though that source stopped changing once the one-off historical
# MAGIC backfill finished.
# MAGIC
# MAGIC **Why this is safe to freeze.** Two facts about the current design make
# MAGIC this a pure performance change, not a behavior change:
# MAGIC 1. `bronze.main_ranking_historical` is loaded up to a fixed cutoff
# MAGIC    (`p_cutoff_week`, default week 30) and landing-CSV data starts at
# MAGIC    week 31 -- **the two sources never share a `(ranking_year,
# MAGIC    ranking_week)` key** under normal operation, so
# MAGIC    `dedup_preferring_landing_csv`'s union-then-dedup was already a
# MAGIC    no-op for every row outside that one theoretical boundary.
# MAGIC 2. The landing-CSV schema already carries its own published
# MAGIC    `PreviousRank`/`RankingPoints_Previous`/`RankingDifference` -- the
# MAGIC    LAG in `backfill_previous_period_columns` only ever fills a NULL
# MAGIC    slot, and only MainRanking-sourced rows arrive with one. So the LAG's
# MAGIC    real work was always confined to the MainRanking branch's own
# MAGIC    internal week-to-week chain, which is fully self-contained (weeks
# MAGIC    21-30 only depend on each other).
# MAGIC
# MAGIC Both conclusions follow from the code as it stands today, not a new
# MAGIC assumption -- but they're exactly the kind of thing that's cheap to get
# MAGIC wrong, which is why the optimized weekly notebooks
# MAGIC (`02.Silver Ranking Individuals.py` / `03.Silver Ranking Pairs.py` in
# MAGIC this same folder) carry their own defensive assertion that fails loudly,
# MAGIC not silently, if a landing week ever does land inside the frozen range.
# MAGIC **Run the parity check in this folder's `README.md` once, comparing this
# MAGIC optimized path's output against the current (slow) notebooks' output on
# MAGIC real data, before replacing the live notebooks.**
# MAGIC
# MAGIC **Idempotent, safe to re-run.** Re-run this notebook only if
# MAGIC `bronze.main_ranking_historical` or `silver.player_identity` changes in a
# MAGIC way that should be reflected here (e.g. a corrected historical reload) --
# MAGIC not on any regular schedule. Full `overwrite` + `overwriteSchema` each
# MAGIC time, same write pattern as every other silver table in this project.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

# MAGIC %run ../00-common/09.mainranking-merge-helpers

# COMMAND ----------

main_ranking_source_table = f"{catalog_name}.{bronze_schema}.main_ranking_historical"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
individuals_target_table = f"{catalog_name}.{silver_schema}.main_ranking_historical_individuals_frozen"
pairs_target_table = f"{catalog_name}.{silver_schema}.main_ranking_historical_pairs_frozen"

INDIVIDUAL_SUBEVENT_CODES = ['MS', 'WS', 'MDI', 'WDI', 'XDI']
PAIR_SUBEVENT_CODES = ['MD', 'WD', 'XD']

main_ranking_bronze_df = spark.table(main_ranking_source_table)
player_identity_df = spark.table(identity_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Individuals branch
# MAGIC Steps 2, 4, 5, 7 of the original `02.Silver Ranking Individuals.py`,
# MAGIC applied to the MainRanking branch alone (never unioned with landing) --
# MAGIC identical mapping/resolution/backfill logic, just run once here instead
# MAGIC of every week against the full union.

# COMMAND ----------

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

resolved_individuals_df = resolve_identity(main_ranking_individuals_df, 'ittfid', 'entity_type', player_identity_df)

backfilled_individuals_df = (
    resolved_individuals_df
        .withColumn('player_name', F.coalesce(F.col('player_name'), F.col('identity_player_name')))
        .withColumn('country_code', F.coalesce(F.col('country_code'), F.col('identity_country_code')))
        .withColumn('continent_id', F.coalesce(F.col('continent_id'), F.col('identity_continent_id')))
        # dedup_preferring_landing_csv's mismatch column, always False here --
        # this branch is never unioned with landing before this point, so
        # there is nothing to compare against. Added for column-shape parity
        # with the weekly notebook's output (which unions this frozen table
        # with a fresh landing branch that also carries this column).
        .withColumn('_dq_source_mismatch', F.lit(False))
)

with_previous_individuals_df = backfill_previous_period_columns(
    backfilled_individuals_df,
    key_cols=['ittfid', 'subevent_code', 'ranking_run_code'],
    order_cols=['ranking_year', 'ranking_week'],
    current_col='current_rank', current_prev_col='previous_rank',
    points_col='ranking_points_ytd', points_prev_col='ranking_points__previous',
    difference_col='ranking_difference',
)

silver_individuals_frozen_df = add_silver_metadata(with_previous_individuals_df)

display(silver_individuals_frozen_df)

# COMMAND ----------

(
    silver_individuals_frozen_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(individuals_target_table)
)

display(spark.table(individuals_target_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pairs branch
# MAGIC Same pattern, mirroring the original `03.Silver Ranking Pairs.py`.

# COMMAND ----------

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

resolved_pairs_df = resolve_identity(main_ranking_pairs_df, 'ittfid', 'entity_type', player_identity_df)

flagged_pairs_df = resolved_pairs_df.withColumn('_dq_source_mismatch', F.lit(False))

with_previous_pairs_df = backfill_previous_period_columns(
    flagged_pairs_df,
    key_cols=['pair_id', 'subevent_code', 'ranking_run_code'],
    order_cols=['ranking_year', 'ranking_week'],
    current_col='current_rank', current_prev_col='previous_rank',
    points_col='points', points_prev_col='_temp_points_previous',
    difference_col='ranking_difference',
).drop('_temp_points_previous')

silver_pairs_frozen_df = add_silver_metadata(with_previous_pairs_df)

display(silver_pairs_frozen_df)

# COMMAND ----------

(
    silver_pairs_frozen_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(pairs_target_table)
)

display(spark.table(pairs_target_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Frozen week range (informational -- read by the weekly notebooks'
# MAGIC defensive overlap assertion, computed fresh there rather than stored,
# MAGIC since it's a cheap MIN/MAX over a small number of distinct weeks)

# COMMAND ----------

display(
    spark.table(individuals_target_table)
        .agg(
            F.min('ranking_year').alias('min_year'), F.max('ranking_year').alias('max_year'),
            F.min('ranking_week').alias('min_week_in_max_year'), F.max('ranking_week').alias('max_week'),
        )
)