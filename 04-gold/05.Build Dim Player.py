# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dim Player
# MAGIC Source: `silver.player_identity` (WHERE `entity_type = 'INDIVIDUAL'`)
# MAGIC
# MAGIC One row per individual `ittfid`. Resolves `continent_name` via
# MAGIC `gold.dim_country` here (rather than at query time in every dashboard)
# MAGIC since `player_identity.continent_id` is intentionally left null in
# MAGIC Silver -- see `03-silver/01.Build Player Identity Dimension`'s comment:
# MAGIC "not on competitors; resolve via ref_countries if needed." This is that
# MAGIC resolution.
# MAGIC
# MAGIC Depends on `01.Build Dim Country` having run first in this same job --
# MAGIC see `03-jobs/`.
# MAGIC 1. Read `silver.player_identity`, filter to `INDIVIDUAL`
# MAGIC 2. Left-join `gold.dim_country` on `country_code` for continent
# MAGIC 3. Write to `gold.dim_player`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

identity_table = f"{catalog_name}.{silver_schema}.player_identity"
country_table = f"{catalog_name}.{gold_schema}.dim_country"
target_table = f"{catalog_name}.{gold_schema}.dim_player"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Individuals only, resolve continent via dim_country

# COMMAND ----------

individuals_df = spark.table(identity_table).filter(F.col('entity_type') == 'INDIVIDUAL')

country_df = spark.table(country_table).select(
    F.col('country_code').alias('_dc_country_code'),
    F.col('continent_id'),
    F.col('continent_code'),
    F.col('continent_name'),
)

dim_player_df = (
    individuals_df
        .join(country_df, individuals_df['country_code'] == country_df['_dc_country_code'], 'left')
        .select(
            individuals_df['ittfid'],
            individuals_df['player_name'],
            individuals_df['country_code'],
            individuals_df['nationality_code'],
            country_df['continent_id'],
            country_df['continent_code'],
            country_df['continent_name'],
            individuals_df['gender'],
            individuals_df['age_category_code'],
            individuals_df['is_retired'],
        )
)

display(dim_player_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Write to gold delta table

# COMMAND ----------

(
    add_gold_metadata(dim_player_df)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())