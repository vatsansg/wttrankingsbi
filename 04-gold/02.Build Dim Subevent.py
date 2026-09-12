# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dim Subevent
# MAGIC Source: `silver.ref_subevents_codes` (+ a hand-maintained label map --
# MAGIC see below)
# MAGIC
# MAGIC One row per `subevent_code` (MS, WS, MDI, WDI, XDI, MD, WD, XD -- the 8
# MAGIC confirmed values, per the discovery doc), with a human-readable label and
# MAGIC an `entity_type` flag (INDIVIDUAL / PAIR) so every dashboard that splits
# MAGIC by discipline (§03 dashboard 02, "Discipline Landscape") doesn't need to
# MAGIC repeat the classification logic already used in
# MAGIC `03-silver/04.Silver Points Ledger` (`INDIVIDUAL_SUBEVENTS` /
# MAGIC `PAIR_SUBEVENTS`).
# MAGIC
# MAGIC **Label map is hand-maintained, not sourced.** `ref_subevents_codes` and
# MAGIC `ref_subevents_codes_description` carry short/technical descriptions, not
# MAGIC the dashboard-friendly labels used throughout this Blueprint ("Men's
# MAGIC Singles", etc.) -- those are typed out here directly from the confirmed
# MAGIC 8-code list. If WTT's own subevent naming differs from these labels,
# MAGIC update `SUBEVENT_LABELS` below; nothing else needs to change.
# MAGIC
# MAGIC **BUGFIX (post-first-Gold-run, caught by real FK validation failure):**
# MAGIC `dbo.SubeventsCodes` -- the raw DB table `ref_subevents_codes` is sourced
# MAGIC from -- only lists actual event types: MD, MS, MT, WD, WS, WT, XD, XT (8
# MAGIC rows, hand-verified directly against `dbo_SubeventsCodes.csv`). It does
# MAGIC **not** contain MDI/WDI/XDI at all -- those 3 codes are a ranking-export
# MAGIC -only invention (the weekly `RankingIndividuals_*` CSVs' way of crediting
# MAGIC doubles points to an individual player) with no corresponding row in the
# MAGIC underlying event-type reference table. The original version of this
# MAGIC notebook joined the label map ONTO the DB reference table, so any code
# MAGIC the DB table didn't have (MDI/WDI/XDI) silently never made it into
# MAGIC `gold.dim_subevent` -- causing every `fact_ranking_individual` row for
# MAGIC one of those 3 codes (roughly half of all individual rows) to fail
# MAGIC `validate_all_gold_tables`'s `subevent_code` FK check as an orphan.
# MAGIC Fixed below with a **full outer join** instead of a left join, driven by
# MAGIC the union of both sides' codes -- so every code either source knows
# MAGIC about ends up with a `dim_subevent` row, regardless of which side (if
# MAGIC either) is missing it. MDI/WDI/XDI end up with a label but a null
# MAGIC `subevent_name`/`organization_code` (no real event-type row exists for
# MAGIC them); MT/WT/XT end up with real DB data but a null label (unused by any
# MAGIC fact today -- team events aren't in the weekly landing files -- but kept
# MAGIC rather than dropped, in case Phase 2's events domain ever needs them).
# MAGIC 1. Read `silver.ref_subevents_codes`
# MAGIC 2. Full-outer-join the hand-maintained label map
# MAGIC 3. Write to `gold.dim_subevent`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

source_table = f"{catalog_name}.{silver_schema}.ref_subevents_codes"
target_table = f"{catalog_name}.{gold_schema}.dim_subevent"

INDIVIDUAL_SUBEVENTS = ['MS', 'WS', 'MDI', 'WDI', 'XDI']
PAIR_SUBEVENTS = ['MD', 'WD', 'XD']

SUBEVENT_LABELS = {
    'MS':  "Men's Singles",
    'WS':  "Women's Singles",
    'MDI': "Men's Doubles (individual credit)",
    'WDI': "Women's Doubles (individual credit)",
    'XDI': "Mixed Doubles (individual credit)",
    'MD':  "Men's Doubles",
    'WD':  "Women's Doubles",
    'XD':  "Mixed Doubles",
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze-sourced silver reference table, attach labels
# MAGIC + entity_type

# COMMAND ----------

label_rows = [(code, label) for code, label in SUBEVENT_LABELS.items()]
labels_df = spark.createDataFrame(label_rows, ['_lbl_subevent_code', 'subevent_label'])

# NOTE: the bronze/silver column "SubEvent" (a short technical name,
# distinct from the "SubEventCode" -> subevent_code fix elsewhere) snake
# cases to "sub_event", not "subevent" -- rename_to_snake_case splits
# every capitalized boundary, and only subevent_code gets an explicit
# rename-back (matching this project's established naming for the
# CODE column specifically). Aliased to subevent_name here for clarity
# rather than carrying the "sub_event" spelling into gold.
ref_df = (
    spark.table(source_table)
        .select(
            F.col('subevent_code').alias('_ref_subevent_code'),
            F.col('sub_event').alias('subevent_name'),
            'organization_code',
        )
)

# FULL OUTER, not left -- see the bugfix note above. labels_df carries the 8
# codes the ranking export actually uses (including the 3 -- MDI/WDI/XDI --
# that have no row at all in the raw DB reference table); ref_df carries
# whatever the raw DB table has (including MT/WT/XT, which labels_df doesn't
# know about). Neither side alone is a complete code list; the union is.
dim_subevent_df = (
    labels_df
        .join(
            ref_df,
            labels_df['_lbl_subevent_code'] == ref_df['_ref_subevent_code'],
            'full_outer',
        )
        .withColumn('subevent_code', F.coalesce(F.col('_lbl_subevent_code'), F.col('_ref_subevent_code')))
        .withColumn(
            'entity_type',
            F.when(F.col('subevent_code').isin(INDIVIDUAL_SUBEVENTS), F.lit('INDIVIDUAL'))
             .when(F.col('subevent_code').isin(PAIR_SUBEVENTS), F.lit('PAIR'))
             .otherwise(F.lit(None).cast('string')),
        )
        .select(
            'subevent_code',
            'subevent_label',
            'entity_type',
            'subevent_name',
            'organization_code',
        )
)

display(dim_subevent_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Write to gold delta table

# COMMAND ----------

(
    add_gold_metadata(dim_subevent_df)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())
