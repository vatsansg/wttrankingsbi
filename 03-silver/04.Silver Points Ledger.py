# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Points Ledger
# MAGIC Source: `bronze.players_events_results_master` +
# MAGIC `_log` + `_log_archives` -- three tables, mutually exclusive by ranking
# MAGIC week (confirmed by WTT), unioned here into one full-history table.
# MAGIC
# MAGIC This is the centerpiece of Step 3: it's where `CompetitorId`'s
# MAGIC polymorphism (see the discovery doc's "Identity model" section) gets
# MAGIC resolved. `CompetitorId`'s *numeric value* already equals either
# MAGIC `competitors.PlayerID` or `players_doubles.DoublesId` -- `SubEventCode`
# MAGIC only tells you *which one*, so the id itself needs no lookup, just a
# MAGIC cast to string. `SubEventCode` is used purely to derive `entity_type`.
# MAGIC
# MAGIC 1. Read all 3 ledger tables, tag each with `ledger_source`, align the
# MAGIC    one column that differs (`PlayerEventResultID` vs. `..._LogId`) to a
# MAGIC    common `source_result_id` (lineage only -- see the discovery doc's
# MAGIC    note that this is not a stable cross-table join key)
# MAGIC 2. Union all 3, snake_case
# MAGIC 3. Derive `ittfid` (cast `competitor_id` to string) and `entity_type`
# MAGIC    from `subevent_code`; resolve both against `silver.player_identity`
# MAGIC 4. Dedupe on the business key (ittfid x subevent x ranking category x
# MAGIC    week x result position -- see discovery doc), write to
# MAGIC    `silver.points_ledger`
# MAGIC
# MAGIC Depends on notebook 01 (`Build Player Identity Dimension`) having run
# MAGIC first in this same job -- see `03-jobs/`.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

current_table = f"{catalog_name}.{bronze_schema}.players_events_results_master"
log_table = f"{catalog_name}.{bronze_schema}.players_events_results_master_log"
log_archives_table = f"{catalog_name}.{bronze_schema}.players_events_results_master_log_archives"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.points_ledger"

INDIVIDUAL_SUBEVENTS = ['MS', 'WS', 'MDI', 'WDI', 'XDI']
PAIR_SUBEVENTS = ['MD', 'WD', 'XD']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read all 3 ledger tables, align the surrogate-key column,
# MAGIC tag lineage

# COMMAND ----------

current_df = (
    spark.table(current_table)
         .withColumnRenamed('PlayerEventResultID', 'source_result_id')
         .withColumn('ledger_source', F.lit('CURRENT'))
         .withColumn('_ledger_source_rank', F.lit(1))
)

log_df = (
    spark.table(log_table)
         .withColumnRenamed('PlayersEventsResultsMaster_LogId', 'source_result_id')
         .withColumn('ledger_source', F.lit('LOG'))
         .withColumn('_ledger_source_rank', F.lit(2))
)

log_archives_df = (
    spark.table(log_archives_table)
         .withColumnRenamed('PlayersEventsResultsMaster_LogId', 'source_result_id')
         .withColumn('ledger_source', F.lit('LOG_ARCHIVES'))
         .withColumn('_ledger_source_rank', F.lit(3))
)

union_df = current_df.unionByName(log_df).unionByName(log_archives_df)

display(union_df.groupBy('ledger_source').count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Snake_case
# MAGIC `rename_to_snake_case` splits every capitalized word boundary, so
# MAGIC `SubEventCode` -> `sub_event_code` (not `subevent_code`) -- renamed
# MAGIC back to `subevent_code` here to match the term used everywhere else in
# MAGIC this project (the bronze reference tables are already named
# MAGIC `ref_subevents_codes` etc., treating "subevent" as one word).

# COMMAND ----------

renamed_df = rename_to_snake_case(union_df).withColumnRenamed('sub_event_code', 'subevent_code')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Derive `ittfid` + `entity_type`, resolve against the
# MAGIC player identity dimension

# COMMAND ----------

with_identity_cols_df = (
    renamed_df
        .withColumn('ittfid', F.col('competitor_id').cast('string'))
        .withColumn(
            'entity_type',
            F.when(F.col('subevent_code').isin(INDIVIDUAL_SUBEVENTS), F.lit('INDIVIDUAL'))
             .when(F.col('subevent_code').isin(PAIR_SUBEVENTS), F.lit('PAIR'))
             .otherwise(F.lit(None).cast('string')),
        )
)

player_identity_df = spark.table(identity_table)

resolved_df = resolve_identity(with_identity_cols_df, 'ittfid', 'entity_type', player_identity_df)

display(resolved_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Dedupe on the business key, write
# MAGIC Key = ittfid x subevent_code x ranking_category_code x ranking_year x
# MAGIC ranking_week x result_position (see discovery doc's "Points-ledger
# MAGIC table model" section for why -- the per-table surrogate id is lineage
# MAGIC only, not part of the grain). Ties broken by `_ledger_source_rank`
# MAGIC (CURRENT beats LOG beats LOG_ARCHIVES) -- expected to matter for zero
# MAGIC rows given the 3 source tables are mutually exclusive by week, kept as
# MAGIC an explicit safety net rather than an assumption.

# COMMAND ----------

# dedup_on_business_key orders its tiebreak columns DESCENDING, but a LOWER
# _ledger_source_rank should win (CURRENT=1 beats LOG=2 beats LOG_ARCHIVES=3)
# -- negate it first so "descending" picks rank 1 (CURRENT) ahead of rank 3.
deduped_df = dedup_on_business_key(
    resolved_df.withColumn('_ledger_source_rank', -F.col('_ledger_source_rank')),
    key_cols=['ittfid', 'subevent_code', 'ranking_category_code', 'ranking_year', 'ranking_week', 'result_position'],
    order_by_desc_cols=['_ledger_source_rank'],
).drop('_ledger_source_rank')

silver_points_ledger_df = add_silver_metadata(deduped_df)

display(silver_points_ledger_df)

# COMMAND ----------

(
    silver_points_ledger_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).groupBy('ledger_source', 'entity_type').count())

# COMMAND ----------

display(
    spark.table(target_table)
         .filter(F.col('identity_resolved') == False)  # noqa: E712
         .count()
)
