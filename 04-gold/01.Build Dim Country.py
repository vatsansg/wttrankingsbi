# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dim Country
# MAGIC Source: `silver.ref_countries` + `silver.ref_continents`
# MAGIC
# MAGIC A conformed country dimension, one row per `country_code`, carrying its
# MAGIC continent directly (denormalized) so every dashboard that groups by
# MAGIC continent (§03 dashboard 01, "Global & Continental Pulse") doesn't need a
# MAGIC second join at query time.
# MAGIC
# MAGIC **BUGFIX (post-first-Gold-run, caught by real FK validation failure):**
# MAGIC `silver.ref_countries` (sourced from `Countries_TTU`, the confirmed
# MAGIC single source of truth for the WTT/ITTF federation code space -- see the
# MAGIC discovery doc) turns out NOT to be a complete superset of every country
# MAGIC code that has ever appeared on a competitor's profile. Confirmed by hand
# MAGIC against the real exports: `FGU` (French Guiana) appears on real
# MAGIC `Competitors` rows, but is entirely absent from `Countries_TTU` -- it
# MAGIC only exists (marked inactive) in the separate, unused `dbo.Countries`
# MAGIC table. `Competitors.CountryCode` is what backfills geography for
# MAGIC MainRanking-sourced rows (Step 2b) and, more importantly, is also part
# MAGIC of the same identity data the real landing-CSV pipeline resolves through
# MAGIC -- so this isn't a Step-2b-only gap. Unlike `dim_subevent`'s fixed
# MAGIC 8-code universe, country codes aren't a small enough list to
# MAGIC hand-enumerate a fix for, so this is closed structurally instead: any
# MAGIC `country_code` actually used by `silver.ranking_individuals` (the one
# MAGIC table this FK check covers) that isn't already in the ref-table-derived
# MAGIC rows gets a placeholder `dim_country` row, rather than silently
# MAGIC orphaning every fact row that carries it.
# MAGIC 1. Read `silver.ref_countries`, left-join `silver.ref_continents` on
# MAGIC    `continent_id`
# MAGIC 2. Add a placeholder row for any `country_code` used in
# MAGIC    `silver.ranking_individuals` but missing from step 1
# MAGIC 3. Write to `gold.dim_country`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

countries_table = f"{catalog_name}.{silver_schema}.ref_countries"
continents_table = f"{catalog_name}.{silver_schema}.ref_continents"
individuals_table = f"{catalog_name}.{silver_schema}.ranking_individuals"
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

base_dim_country_df = (
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

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add a placeholder row for any code the ref table is
# MAGIC missing but the real data actually uses (see bugfix note above)

# COMMAND ----------

used_codes_df = (
    spark.table(individuals_table)
        .select(F.col('country_code').alias('_used_code'))
        .where(F.col('_used_code').isNotNull())
        .distinct()
)
known_codes_df = base_dim_country_df.select(F.col('country_code').alias('_known_code'))
missing_codes_df = used_codes_df.join(
    known_codes_df, used_codes_df['_used_code'] == known_codes_df['_known_code'], 'left_anti',
)
missing_rows_df = missing_codes_df.select(
    F.col('_used_code').alias('country_code'),
    F.lit('Unknown / not in reference table').alias('country_name'),
    F.lit(None).cast(base_dim_country_df.schema['continent_id'].dataType).alias('continent_id'),
    F.lit(None).cast('string').alias('continent_code'),
    F.lit(None).cast('string').alias('continent_name'),
    F.lit(None).cast(base_dim_country_df.schema['tel_code'].dataType).alias('tel_code'),
    F.lit(None).cast('string').alias('individual_naming_convention'),
)

dim_country_df = base_dim_country_df.unionByName(missing_rows_df)

display(dim_country_df)
display(missing_rows_df)  # sanity check -- should be a short list (e.g. FGU), not a large one

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
