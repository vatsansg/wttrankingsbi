# Databricks notebook source
# MAGIC %md
# MAGIC # Validate all silver tables
# MAGIC Final task in the silver transformation job. Runs after every silver
# MAGIC notebook (identity dimension, the 4 bespoke tables, and the reference
# MAGIC for-each) and fails the job loudly if any of the 17 silver tables is
# MAGIC missing, empty, or has a broken business key -- same intent as bronze's
# MAGIC `09.Validate All Bronze Tables`, plus a check specific to Silver: how
# MAGIC much of each fact table's `ittfid` failed to resolve against
# MAGIC `silver.player_identity`.
# MAGIC
# MAGIC Intentionally lightweight, proportionate to a 17-table silver layer --
# MAGIC not a large production test suite.
# MAGIC
# MAGIC **Change (2026-09-12, MainRanking historical backfill exercise):**
# MAGIC `silver.ranking_individuals`/`ranking_pairs` are now built from *two*
# MAGIC sources (the landing-CSV pipeline and, once loaded,
# MAGIC `bronze.main_ranking_historical`) -- see the modified `02`/`03`
# MAGIC notebooks. Check 4 is new: it surfaces the `_dq_source_mismatch` count
# MAGIC (rows where both sources disagreed on rank/points for the same week, and
# MAGIC the landing-CSV value won) as a visible data-quality signal, and prints
# MAGIC the distinct-week count on both tables so accumulation is easy to
# MAGIC confirm at a glance. A non-zero mismatch count is NOT a hard failure --
# MAGIC it's exactly the kind of disagreement the progressive-week test (Phase
# MAGIC B) exists to catch, and needs a human look rather than an automatic
# MAGIC pass/fail.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

# MAGIC %run ../00-common/07.silver-reference-registry

# COMMAND ----------

ALL_SILVER_TABLES = [
    f"{catalog_name}.{silver_schema}.player_identity",
    f"{catalog_name}.{silver_schema}.ranking_individuals",
    f"{catalog_name}.{silver_schema}.ranking_pairs",
    f"{catalog_name}.{silver_schema}.points_ledger",
    f"{catalog_name}.{silver_schema}.individuals_event_penalties",
] + [
    f"{catalog_name}.{silver_schema}.{table_key}" for table_key in sorted(SILVER_REFERENCE_TABLES)
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 1 - every table exists and has rows

# COMMAND ----------

failures = []
for full_table_name in ALL_SILVER_TABLES:
    try:
        row_count = spark.table(full_table_name).count()
        if row_count == 0:
            failures.append(f"{full_table_name}: 0 rows")
        else:
            print(f"OK    {full_table_name}: {row_count} rows")
    except Exception as e:
        failures.append(f"{full_table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 2 - no null business keys on the load-bearing tables

# COMMAND ----------

null_key_checks = [
    (f"{catalog_name}.{silver_schema}.player_identity", "ittfid"),
    (f"{catalog_name}.{silver_schema}.ranking_individuals", "ittfid"),
    (f"{catalog_name}.{silver_schema}.ranking_pairs", "pair_id"),
    (f"{catalog_name}.{silver_schema}.points_ledger", "ittfid"),
]

for full_table_name, key_col in null_key_checks:
    null_count = spark.table(full_table_name).where(F.col(key_col).isNull()).count()
    if null_count > 0:
        failures.append(f"{full_table_name}.{key_col} has {null_count} null value(s)")
    else:
        print(f"OK    {full_table_name}.{key_col}: no nulls")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 3 - identity resolution rate
# MAGIC Not a hard failure by itself (an unresolved `ittfid` is a data quality
# MAGIC signal for Gold to decide how to handle, per `06.silver-helpers`'
# MAGIC `resolve_identity` docstring) -- but a HIGH unresolved rate would mean
# MAGIC something upstream broke (e.g. the identity dimension ran against a
# MAGIC stale/empty `bronze.competitors`), so it's still worth surfacing loudly.
# MAGIC Threshold: fail if more than 5% of any table's rows are unresolved.
# MAGIC
# MAGIC **Known allowance -- `points_ledger` PAIR rows.** A production run
# MAGIC confirmed a real, documented data-quality gap: `bronze.players_doubles`
# MAGIC is missing ~35% of the *distinct pairs* `points_ledger` has ever
# MAGIC referenced (9,342 of 26,624), because that bronze extract appears to be
# MAGIC current/active-pairs only rather than the full historical
# MAGIC `Players_Doubles` table -- see the README's "Pair identity coverage
# MAGIC gap" section for the full diagnosis.
# MAGIC
# MAGIC The *row-level* percentage is higher than the distinct-id percentage
# MAGIC (confirmed on a real run: 4,004,650 / 6,737,868 PAIR rows = 59.4%, not
# MAGIC ~35%) -- expected, not a separate problem: missing pairs skew toward
# MAGIC long-lived ones that accumulated far more historical rows before
# MAGIC dropping out of an active-only extract (confirmed: unresolved pairs
# MAGIC average ~429 rows/pair vs. ~158 rows/pair for resolved ones, and 73.5%
# MAGIC of unresolved rows sit in `_log_archives`, the longest-history source).
# MAGIC A pair active for 5+ years before retiring contributes far more ledger
# MAGIC rows than one active for a single season, so a minority of *distinct*
# MAGIC missing pairs still accounts for a majority of *rows*.
# MAGIC
# MAGIC The gap is 100% concentrated in `entity_type = 'PAIR'` (confirmed: 0
# MAGIC unresolved `INDIVIDUAL` rows), so `points_ledger` is checked per
# MAGIC `entity_type` below: `INDIVIDUAL` stays at the standard 5% threshold,
# MAGIC `PAIR` gets a separate, explicit, higher threshold (set from the
# MAGIC observed 59.4% plus a safety margin) so this *known, root-caused* issue
# MAGIC doesn't fail every run until the source DB extract is fixed. Tighten
# MAGIC `PAIR_KNOWN_GAP_THRESHOLD_PCT` back down to 5.0 once that extract pulls
# MAGIC full pair history.
# MAGIC
# MAGIC **`ranking_pairs` note (MainRanking backfill exercise):** a hand-check
# MAGIC against the actual `dbo_MainRanking.csv`/`dbo_Players_Doubles.csv`
# MAGIC extracts, before this ran for real, found MainRanking's own pair rows
# MAGIC resolve at ~99.97% against that `players_doubles` snapshot -- i.e. the
# MAGIC historical backfill was NOT expected to push `ranking_pairs` anywhere
# MAGIC near the `points_ledger` PAIR gap. `ranking_pairs` is deliberately kept
# MAGIC at the standard 5% threshold below rather than pre-emptively copying
# MAGIC `points_ledger`'s allowance -- if this run's real numbers disagree with
# MAGIC that hand-check, that's worth knowing about via a real failure, not
# MAGIC hiding behind a loosened threshold.

# COMMAND ----------

identity_resolution_checks = [
    f"{catalog_name}.{silver_schema}.ranking_individuals",
    f"{catalog_name}.{silver_schema}.ranking_pairs",
    f"{catalog_name}.{silver_schema}.individuals_event_penalties",
]

UNRESOLVED_THRESHOLD_PCT = 5.0
PAIR_KNOWN_GAP_THRESHOLD_PCT = 65.0  # explicit allowance for the documented bronze.players_doubles coverage gap in points_ledger only -- see README

for full_table_name in identity_resolution_checks:
    df = spark.table(full_table_name)
    total = df.count()
    unresolved = df.filter(F.col('identity_resolved') == False).count()  # noqa: E712
    pct = (unresolved / total * 100) if total else 0.0
    if pct > UNRESOLVED_THRESHOLD_PCT:
        failures.append(
            f"{full_table_name}: {unresolved}/{total} rows ({pct:.1f}%) failed identity "
            f"resolution -- exceeds the {UNRESOLVED_THRESHOLD_PCT}% threshold"
        )
    else:
        print(f"OK    {full_table_name}: {unresolved}/{total} rows ({pct:.1f}%) unresolved")

# points_ledger is checked separately, split by entity_type -- the known
# pair-identity gap (see above) only affects PAIR rows, so INDIVIDUAL keeps
# the standard threshold while PAIR gets the documented allowance instead of
# being lumped into one table-wide percentage.
points_ledger_table = f"{catalog_name}.{silver_schema}.points_ledger"
points_ledger_df = spark.table(points_ledger_table)

for entity_type, threshold in [('INDIVIDUAL', UNRESOLVED_THRESHOLD_PCT), ('PAIR', PAIR_KNOWN_GAP_THRESHOLD_PCT)]:
    subset = points_ledger_df.filter(F.col('entity_type') == entity_type)
    total = subset.count()
    unresolved = subset.filter(F.col('identity_resolved') == False).count()  # noqa: E712
    pct = (unresolved / total * 100) if total else 0.0
    label = f"{points_ledger_table} [{entity_type}]"
    if pct > threshold:
        failures.append(
            f"{label}: {unresolved}/{total} rows ({pct:.1f}%) failed identity "
            f"resolution -- exceeds the {threshold}% threshold"
        )
    else:
        note = " (known bronze.players_doubles coverage gap -- see README)" if entity_type == 'PAIR' else ""
        print(f"OK    {label}: {unresolved}/{total} rows ({pct:.1f}%) unresolved{note}")

# A row whose subevent_code didn't map to either INDIVIDUAL or PAIR (see
# INDIVIDUAL_SUBEVENTS/PAIR_SUBEVENTS in 04) has a null entity_type and is
# silently excluded from the two loops above -- confirmed 0 such rows in
# production today, so any appearing here is a new regression, not the known
# pair-identity gap, and always fails regardless of count.
unmapped_subevent_count = points_ledger_df.filter(F.col('entity_type').isNull()).count()
if unmapped_subevent_count > 0:
    failures.append(
        f"{points_ledger_table}: {unmapped_subevent_count} row(s) have an unmapped "
        f"subevent_code (entity_type is null) -- INDIVIDUAL_SUBEVENTS/PAIR_SUBEVENTS "
        f"in 04.Silver Points Ledger may need updating"
    )
else:
    print(f"OK    {points_ledger_table}: no rows with an unmapped subevent_code")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 4 (new) - MainRanking merge data-quality signals
# MAGIC Informational, not a hard failure (see header note): the
# MAGIC `_dq_source_mismatch` count, and the distinct-week count on both tables
# MAGIC so accumulation across the landing-CSV and MainRanking sources is
# MAGIC visible at a glance.

# COMMAND ----------

for full_table_name in [
    f"{catalog_name}.{silver_schema}.ranking_individuals",
    f"{catalog_name}.{silver_schema}.ranking_pairs",
]:
    df = spark.table(full_table_name)
    total = df.count()
    mismatch_count = df.filter(F.col('_dq_source_mismatch') == True).count()  # noqa: E712
    week_count = df.select('ranking_year', 'ranking_week').distinct().count()
    source_breakdown = (
        df.groupBy('_source_system').count().collect()
        if '_source_system' in df.columns else []
    )
    print(
        f"INFO  {full_table_name}: {total} rows, {week_count} distinct week(s), "
        f"{mismatch_count} source-mismatch row(s), by source: "
        f"{ {r['_source_system']: r['count'] for r in source_breakdown} }"
    )
    if mismatch_count > 0:
        print(
            f"      ^ {mismatch_count} row(s) had disagreeing rank/points between "
            f"LANDING_CSV and MAIN_RANKING_HISTORICAL for the same week -- the "
            f"LANDING_CSV value won, but this should be reviewed by hand, not ignored."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Result

# COMMAND ----------

if failures:
    raise ValueError("Silver validation FAILED:\n" + "\n".join(f"  - {f}" for f in failures))

print(f"Silver validation passed: {len(ALL_SILVER_TABLES)} tables checked.")
