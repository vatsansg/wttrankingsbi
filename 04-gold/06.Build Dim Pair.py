# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dim Pair
# MAGIC Source: `silver.player_identity` (WHERE `entity_type = 'PAIR'`)
# MAGIC
# MAGIC One row per pair `ittfid` (== `players_doubles.DoublesId` == the landing
# MAGIC files' `PairId`). Resolves each partner's country via `gold.dim_player`
# MAGIC (not `dim_country` directly) so the pair carries both a country AND a
# MAGIC continent per partner, and derives `is_cross_country` -- the flag behind
# MAGIC §03 dashboard 06 ("Doubles Partnerships", 21.5% cross-country in Mixed
# MAGIC Doubles).
# MAGIC
# MAGIC Depends on `05.Build Dim Player` having run first in this same job --
# MAGIC see `03-jobs/`.
# MAGIC 1. Read `silver.player_identity`, filter to `PAIR`
# MAGIC 2. Join `gold.dim_player` twice (partner1, partner2) for country/continent
# MAGIC 3. Derive `is_cross_country`
# MAGIC 4. Write to `gold.dim_pair`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

identity_table = f"{catalog_name}.{silver_schema}.player_identity"
player_table = f"{catalog_name}.{gold_schema}.dim_player"
target_table = f"{catalog_name}.{gold_schema}.dim_pair"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Pairs only, join dim_player twice for each partner

# COMMAND ----------

pairs_df = spark.table(identity_table).filter(F.col('entity_type') == 'PAIR')

p1 = spark.table(player_table).select(
    F.col('ittfid').alias('_p1_ittfid'),
    F.col('country_code').alias('partner1_country_code'),
    F.col('continent_name').alias('partner1_continent_name'),
)
p2 = spark.table(player_table).select(
    F.col('ittfid').alias('_p2_ittfid'),
    F.col('country_code').alias('partner2_country_code'),
    F.col('continent_name').alias('partner2_continent_name'),
)

with_partner_countries_df = (
    pairs_df
        .join(p1, pairs_df['partner1_ittfid'] == p1['_p1_ittfid'], 'left')
        .join(p2, pairs_df['partner2_ittfid'] == p2['_p2_ittfid'], 'left')
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Derive is_cross_country
# MAGIC Null-safe: a pair missing one partner's resolved country (e.g. the
# MAGIC partner isn't in `dim_player`) gets `is_cross_country = null`, not a
# MAGIC false "same country" -- distinct from a genuinely same-country pair.

# COMMAND ----------

dim_pair_df = with_partner_countries_df.select(
    F.col('ittfid'),
    F.col('player_name'),
    F.col('partner1_ittfid'),
    F.col('partner1_country_code'),
    F.col('partner1_continent_name'),
    F.col('partner2_ittfid'),
    F.col('partner2_country_code'),
    F.col('partner2_continent_name'),
    F.col('age_category_code'),
    F.col('subevent_code'),
    F.when(
        F.col('partner1_country_code').isNull() | F.col('partner2_country_code').isNull(),
        F.lit(None).cast('boolean'),
    ).otherwise(F.col('partner1_country_code') != F.col('partner2_country_code')).alias('is_cross_country'),
)

display(dim_pair_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to gold delta table

# COMMAND ----------

(
    add_gold_metadata(dim_pair_df)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())
