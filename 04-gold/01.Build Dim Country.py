# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dim Country
# MAGIC Source: `silver.ref_countries` + `silver.ref_continents`
# MAGIC
# MAGIC A conformed country dimension, one row per `country_code`, carrying its
# MAGIC continent directly (denormalized) so every dashboard that groups by
# MAGIC continent (§03 dashboard 01, "Global & Continental Pulse") doesn't need a
# MAGIC second join at query time.
# MAGIC 1. Read `silver.ref_countries`, left-join `silver.ref_continents` on
# MAGIC    `continent_id`
# MAGIC 2. Write to `gold.dim_country`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

countries_table = f"{catalog_name}.{silver_schema}.ref_countries"
continents_table = f"{catalog_name}.{silver_schema}.ref_continents"
target_table = f"{catalog_name}.{gold_schema}.dim_country"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Join countries to continents

# COMMAND ----------

countries_df = spark.table(countries_table)
continents_df = spark.table(continents_table).select(
    F.col('continent_id').alias('_c_continent_id'),
    F.col('continent_code').alias('continent_code'),
    F.col('continent_name').alias('continent_name'),
)

dim_country_df = (
    countries_df
        .join(continents_df, countries_df['continent_id'] == continents_df['_c_continent_id'], 'left')
        .select(
            F.col('country_code'),
            F.col('country_name'),
            F.col('continent_id'),
            F.col('continent_code'),
            F.col('continent_name'),
            F.col('tel_code'),
            F.col('individual_naming_convention'),
        )
)

display(dim_country_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Write to gold delta table

# COMMAND ----------

(
    add_gold_metadata(dim_country_df)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())