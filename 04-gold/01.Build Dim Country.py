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
# MAGIC **BUGFIX #1 (post-first-Gold-run, caught by real FK validation failure):**
# MAGIC `silver.ref_countries` (originally sourced from `Countries_TTU`, later
# MAGIC repointed to `Countries` -- see bugfix #2 below) turns out NOT to be a
# MAGIC complete superset of every country code that has ever appeared on a
# MAGIC competitor's profile. Confirmed by hand against the real exports: `FGU`
# MAGIC (French Guiana) appeared on real `Competitors` rows but was, at the time,
# MAGIC absent from `Countries_TTU`. `Competitors.CountryCode` is what backfills
# MAGIC geography for MainRanking-sourced rows (Step 2b) and, more importantly,
# MAGIC is also part of the same identity data the real landing-CSV pipeline
# MAGIC resolves through -- so this isn't a Step-2b-only gap. Unlike
# MAGIC `dim_subevent`'s fixed 8-code universe, country codes aren't a small
# MAGIC enough list to hand-enumerate a fix for, so this is closed structurally
# MAGIC instead: any `country_code` actually used by `silver.ranking_individuals`
# MAGIC (the one table this FK check covers) that isn't already in the
# MAGIC ref-table-derived rows gets a placeholder `dim_country` row, rather than
# MAGIC silently orphaning every fact row that carries it.
# MAGIC
# MAGIC **NOT dormant -- confirmed still active 2026-09-13.** `FGU` (French
# MAGIC Guiana) itself is now covered directly by the consolidated `Countries`
# MAGIC table (bugfix #2), but a pre-flight check against the live source
# MAGIC (`Competitors` LEFT JOIN `Countries` on `CountryCode`) turned up three
# MAGIC *other* codes still missing: `ITTF`, `RWORLD`, `WTT` -- organizational /
# MAGIC neutral-flag codes (e.g. a refugee-team designation), not real countries,
# MAGIC so they were never going to be in a country reference table. This path
# MAGIC still fires for them: expect a handful of `dim_country` rows named
# MAGIC "Unknown / not in reference table" with NULL continent if any of these
# MAGIC three codes actually appear in `silver.ranking_individuals` -- which will
# MAGIC in turn show up as a small NULL-continent group in `v_continental_pulse`.
# MAGIC That NULL group is this safety net working as designed, not a
# MAGIC regression of bugfix #2 -- don't mistake it for the original bug
# MAGIC returning. This check only covers `ranking_individuals`; if a code ever
# MAGIC shows up in `ranking_pairs`-derived identities but never in
# MAGIC `ranking_individuals`, it would slip past this net -- worth widening to
# MAGIC union both sources if that's ever observed in practice.
# MAGIC
# MAGIC **BUGFIX #2 (2026-09-13, caught by a real query against
# MAGIC `v_continental_pulse` returning every continent field NULL):** every row
# MAGIC of `dim_country` had `continent_name`/`continent_code` = NULL. Root
# MAGIC cause: `silver.ref_countries.continent_id` (sourced from the now-removed
# MAGIC `Countries_TTU`) was an integer id-space (1-22-ish) that never
# MAGIC corresponded to `silver.ref_continents.continent_id` (the real 5-value
# MAGIC `AFR`/`AME`/`ASI`/`EUR`/`OCE` code) -- confirmed by checking the source
# MAGIC database directly: no bridging table existed anywhere for that integer
# MAGIC range. The actual fix was one level up the pipeline, not in this
# MAGIC notebook: the source DBA had consolidated `Countries_TTU` into the plain
# MAGIC `Countries` table, which stores `ContinentId` as a `varchar` holding the
# MAGIC real continent code directly. `00-common/05.reference-table-registry`
# MAGIC (bronze) now points `ref_countries` at `Countries` with its actual
# MAGIC (smaller) column list, so `silver.ref_countries.continent_id` is now the
# MAGIC correct string code and the join below resolves correctly without any
# MAGIC change to the join condition itself. The one change needed *here* is
# MAGIC dropping `tel_code`/`individual_naming_convention` from the selects below
# MAGIC -- `Countries` has no such columns (confirmed via
# MAGIC `INFORMATION_SCHEMA.COLUMNS`), so keeping them would just move the
# MAGIC column-not-found error here instead of fixing it.
# MAGIC 1. Read `silver.ref_countries`, left-join `silver.ref_continents` on
# MAGIC    `continent_id` (both sides now the same 5-value code space)
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