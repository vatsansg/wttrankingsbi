# Databricks notebook source
# MAGIC %md
# MAGIC # Build Player Identity Dimension
# MAGIC Source: `bronze.competitors` + `bronze.players_doubles`
# MAGIC
# MAGIC The confirmed identity model (see the discovery doc's "Identity model"
# MAGIC section): the points ledger's `CompetitorId` is polymorphic -- for
# MAGIC individual/individual-credit disciplines (MS, WS, MDI, WDI, XDI) it's
# MAGIC `competitors.PlayerID`; for pair disciplines (MD, WD, XD) it's
# MAGIC `players_doubles.DoublesId` (== the landing files' `PairId`). This
# MAGIC notebook merges both id spaces into one `ittfid` dimension so every
# MAGIC other silver notebook can resolve against a single table instead of
# MAGIC re-deriving this logic per table.
# MAGIC
# MAGIC Every other Step 3 notebook depends on this one running first -- see
# MAGIC the job definition in `03-jobs/`.
# MAGIC 1. Read `bronze.competitors` -> `entity_type = 'INDIVIDUAL'` rows
# MAGIC 2. Read `bronze.players_doubles`, joined twice to `bronze.competitors`
# MAGIC    (for each partner) -> `entity_type = 'PAIR'` rows
# MAGIC 3. Union into one dimension, `ittfid` always a STRING (source ids are
# MAGIC    BIGINT; landing files' `IttfId`/`PairId` are STRING -- STRING is the
# MAGIC    common type every join in Silver/Gold needs)
# MAGIC 4. Write to `silver.player_identity`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

competitors_table = f"{catalog_name}.{bronze_schema}.competitors"
players_doubles_table = f"{catalog_name}.{bronze_schema}.players_doubles"
target_table = f"{catalog_name}.{silver_schema}.player_identity"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Individuals: one row per `competitors.PlayerID`

# COMMAND ----------

competitors_df = spark.table(competitors_table)

individuals_df = competitors_df.select(
    F.col('PlayerID').cast('string').alias('ittfid'),
    F.lit('INDIVIDUAL').alias('entity_type'),
    F.col('PlayerName').cast('string').alias('player_name'),
    F.col('CountryCode').cast('string').alias('country_code'),
    F.col('NationalityCode').cast('string').alias('nationality_code'),
    F.lit(None).cast('string').alias('continent_id'),  # not on competitors; resolve via ref_countries if needed
    F.col('Gender').cast('string').alias('gender'),
    F.col('AgeCategoryCode').cast('string').alias('age_category_code'),
    # explicit cast (not just alias) -- bronze.competitors.IsRetired isn't
    # guaranteed to already be BOOLEAN (it wasn't: this union failed on a
    # real run with STRING vs. pairs_df's explicit boolean literal), so cast
    # rather than assume, same as every other column here now does.
    F.col('IsRetired').cast('boolean').alias('is_retired'),
    F.lit(None).cast('string').alias('partner1_ittfid'),
    F.lit(None).cast('string').alias('partner2_ittfid'),
    F.lit(None).cast('string').alias('subevent_code'),
    F.lit('competitors').alias('source_table'),
)

display(individuals_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Pairs: one row per `players_doubles.DoublesId`
# MAGIC A pair has no name/country of its own in the source -- `player_name`
# MAGIC here is derived (`Player1 / Player2`) by joining each partner back to
# MAGIC `competitors`; `country_code` is left null since a pair can be
# MAGIC cross-country (mixed doubles) with no single correct value. Consumers
# MAGIC that need a pair's per-partner country should join `partner1_ittfid`
# MAGIC / `partner2_ittfid` back to this same table.

# COMMAND ----------

players_doubles_df = spark.table(players_doubles_table)

p1 = competitors_df.select(
    F.col('PlayerID').alias('_p1_id'), F.col('PlayerName').alias('_p1_name')
)
p2 = competitors_df.select(
    F.col('PlayerID').alias('_p2_id'), F.col('PlayerName').alias('_p2_name')
)

pairs_df = (
    players_doubles_df
        .join(p1, players_doubles_df['Player1Id'] == p1['_p1_id'], 'left')
        .join(p2, players_doubles_df['Player2Id'] == p2['_p2_id'], 'left')
        .select(
            F.col('DoublesId').cast('string').alias('ittfid'),
            F.lit('PAIR').alias('entity_type'),
            F.concat_ws(' / ', F.col('_p1_name'), F.col('_p2_name')).alias('player_name'),
            F.lit(None).cast('string').alias('country_code'),
            F.lit(None).cast('string').alias('nationality_code'),
            F.lit(None).cast('string').alias('continent_id'),
            F.lit(None).cast('string').alias('gender'),
            F.col('AgeCategoryCode').cast('string').alias('age_category_code'),
            F.lit(None).cast('boolean').alias('is_retired'),
            F.col('Player1Id').cast('string').alias('partner1_ittfid'),
            F.col('Player2Id').cast('string').alias('partner2_ittfid'),
            F.col('SubEventCode').cast('string').alias('subevent_code'),
            F.lit('players_doubles').alias('source_table'),
        )
)

display(pairs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Union individuals + pairs, add metadata, write

# COMMAND ----------

player_identity_df = add_silver_metadata(individuals_df.unionByName(pairs_df))

display(player_identity_df)

# COMMAND ----------

(
    player_identity_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).groupBy('entity_type').count())
