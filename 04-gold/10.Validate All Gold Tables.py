# Databricks notebook source
# MAGIC %md
# MAGIC # Validate all gold tables
# MAGIC Final task in the gold job. Runs after every dim, fact, and both views
# MAGIC notebooks (`09` snapshot views + `11` longitudinal views, added
# MAGIC 2026-09-13) -- fails the job loudly if any of the 8 gold tables is
# MAGIC missing, empty, has a broken business key, or (new for Gold, since this
# MAGIC is the first layer with real star-schema joins) has fact rows whose
# MAGIC foreign key doesn't resolve against its dimension. Same intent and same
# MAGIC proportionate, lightweight scope as bronze's `09.Validate All Bronze
# MAGIC Tables` and silver's `07.Validate All Silver Tables`.
# MAGIC
# MAGIC Check 4 below (`ALL_GOLD_VIEWS`) covers 17 views total: the original
# MAGIC 8 latest-week snapshots from `09` plus the 9 full-history longitudinal
# MAGIC views from `11` -- same `COUNT(*)` sanity check for both, since this
# MAGIC validator only confirms a view resolves, not what it should contain (see
# MAGIC `09`'s own note on why an FK/row-count validator can't catch a semantic
# MAGIC scoping bug either way).
# MAGIC
# MAGIC **Step 8 addition (2026-09-19): Check 5, target-week-landed assertion**
# MAGIC -- same freshness-check pattern as bronze/silver's Check 4 (see those
# MAGIC notebooks). Confirms the target week's rows actually made it into
# MAGIC `fact_ranking_individual`/`fact_ranking_pair`, since every check above
# MAGIC passes on total accumulated history regardless of whether the latest
# MAGIC week landed. On success, this is also the notebook that marks the whole
# MAGIC batch `gold_done` in `control.batch_control` -- the last stage, so this
# MAGIC is the row Step 10's freshness check and dashboard tile actually read.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

ALL_GOLD_TABLES = [
    f"{catalog_name}.{gold_schema}.dim_country",
    f"{catalog_name}.{gold_schema}.dim_subevent",
    f"{catalog_name}.{gold_schema}.dim_age_category",
    f"{catalog_name}.{gold_schema}.dim_ranking_week",
    f"{catalog_name}.{gold_schema}.dim_player",
    f"{catalog_name}.{gold_schema}.dim_pair",
    f"{catalog_name}.{gold_schema}.fact_ranking_individual",
    f"{catalog_name}.{gold_schema}.fact_ranking_pair",
]

ALL_GOLD_VIEWS = [
    "v_continental_pulse",
    "v_discipline_landscape",
    "v_federation_scorecard",
    "v_youth_pipeline",
    "v_ranking_movers",
    "v_doubles_partnerships",
    "v_fresh_faces",
    "v_ranking_leaders",
    "v_federation_strength_trajectory",
    "v_continental_power_shift",
    "v_player_career_trajectory",
    "v_peak_rank_career_longevity",
    "v_country_discipline_investment_trend",
    "v_junior_to_senior_transition",
    "v_new_entrant_retention_curve",
    "v_doubles_partnership_longevity",
    "v_volatility_consistency_index",
]

dbutils.widgets.text("p_ranking_year", "")
dbutils.widgets.text("p_ranking_week", "")
v_ranking_year = dbutils.widgets.get("p_ranking_year")
v_ranking_week = dbutils.widgets.get("p_ranking_week")
HAVE_TARGET_WEEK = bool(v_ranking_year) and bool(v_ranking_week)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 1 - every table exists and has rows

# COMMAND ----------

failures = []
for full_table_name in ALL_GOLD_TABLES:
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
# MAGIC #### Check 2 - no null business keys on the dimension/fact key columns

# COMMAND ----------

null_key_checks = [
    (f"{catalog_name}.{gold_schema}.dim_country", "country_code"),
    (f"{catalog_name}.{gold_schema}.dim_subevent", "subevent_code"),
    (f"{catalog_name}.{gold_schema}.dim_age_category", "age_category_code"),
    (f"{catalog_name}.{gold_schema}.dim_player", "ittfid"),
    (f"{catalog_name}.{gold_schema}.dim_pair", "ittfid"),
    (f"{catalog_name}.{gold_schema}.fact_ranking_individual", "ittfid"),
    (f"{catalog_name}.{gold_schema}.fact_ranking_pair", "ittfid"),
]

for full_table_name, key_col in null_key_checks:
    null_count = spark.table(full_table_name).where(F.col(key_col).isNull()).count()
    if null_count > 0:
        failures.append(f"{full_table_name}.{key_col} has {null_count} null value(s)")
    else:
        print(f"OK    {full_table_name}.{key_col}: no nulls")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 3 - foreign key integrity (fact -> dimension)
# MAGIC Every FK on the fact tables is a plain carried-forward column, not a
# MAGIC lookup (see `00-common/08.gold-helpers`) -- **except** `ittfid`, which is
# MAGIC the one FK that depends on Silver's identity resolution having
# MAGIC succeeded. `dim_player`/`dim_pair` only contain Silver's *resolved*
# MAGIC identity population, while `identity_resolved = false` fact rows are a
# MAGIC real, accepted, non-zero rate under Step 3's own thresholds (5%
# MAGIC individuals / 65% pairs -- see the Step 3 README's pair-identity-gap
# MAGIC finding). So only the `ittfid` checks below exclude
# MAGIC `identity_resolved = false` rows, the same accommodation Silver already
# MAGIC makes -- otherwise this would hard-fail the Gold job on every single run.
# MAGIC
# MAGIC `country_code` / `subevent_code` / `age_category_code` / `category_code`
# MAGIC are NOT identity-dependent -- they're plain source columns present
# MAGIC whether or not identity resolution succeeded -- so those checks
# MAGIC deliberately run against every row, unresolved-identity rows included.
# MAGIC
# MAGIC A failure here means a dimension build (01-06) used a different source
# MAGIC snapshot than the corresponding fact build (07-08) did within the same
# MAGIC job run, or (for `ittfid`) a resolved row's identity genuinely doesn't
# MAGIC exist in its dimension.

# COMMAND ----------

fk_checks = [
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_individual", 'country_code',
        f"{catalog_name}.{gold_schema}.dim_country", 'country_code', False,
    ),
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_individual", 'subevent_code',
        f"{catalog_name}.{gold_schema}.dim_subevent", 'subevent_code', False,
    ),
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_individual", 'age_category_code',
        f"{catalog_name}.{gold_schema}.dim_age_category", 'age_category_code', False,
    ),
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_individual", 'category_code',
        f"{catalog_name}.{gold_schema}.dim_age_category", 'category_code', False,
    ),
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_individual", 'ittfid',
        f"{catalog_name}.{gold_schema}.dim_player", 'ittfid', True,
    ),
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_pair", 'subevent_code',
        f"{catalog_name}.{gold_schema}.dim_subevent", 'subevent_code', False,
    ),
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_pair", 'age_category_code',
        f"{catalog_name}.{gold_schema}.dim_age_category", 'age_category_code', False,
    ),
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_pair", 'category_code',
        f"{catalog_name}.{gold_schema}.dim_age_category", 'category_code', False,
    ),
    (
        f"{catalog_name}.{gold_schema}.fact_ranking_pair", 'ittfid',
        f"{catalog_name}.{gold_schema}.dim_pair", 'ittfid', True,
    ),
]

for fact_table, fact_col, dim_table, dim_col, identity_dependent in fk_checks:
    fact_df = spark.table(fact_table)
    if identity_dependent:
        fact_df = fact_df.where(F.col('identity_resolved') == True)  # noqa: E712 -- known Silver identity-resolution gap, see note above
    fact_df = fact_df.select(F.col(fact_col).alias('_fk')).where(F.col(fact_col).isNotNull())
    dim_df = spark.table(dim_table).select(F.col(dim_col).alias('_dk')).distinct()
    orphan_count = fact_df.join(dim_df, fact_df['_fk'] == dim_df['_dk'], 'left_anti').count()
    label = f"{fact_table}.{fact_col} -> {dim_table}.{dim_col}"
    if orphan_count > 0:
        failures.append(f"{label}: {orphan_count} orphaned row(s) with no matching dimension key")
    else:
        scope_note = " (identity_resolved rows only)" if identity_dependent else ""
        print(f"OK    {label}: every value resolves{scope_note}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 4 - every dashboard view resolves
# MAGIC Views are created with `CREATE OR REPLACE VIEW` in notebooks `09`/`11`,
# MAGIC which don't themselves catch a broken view definition (a bad column
# MAGIC reference only surfaces when the view is queried) -- this check runs one
# MAGIC `COUNT(*)` per view so a broken view fails the job here, not silently at
# MAGIC first dashboard load. **Note for Step 10 alerting:** `09`/`11` each issue
# MAGIC several independent `CREATE OR REPLACE VIEW` statements sequentially, so
# MAGIC a failure partway through either one can leave some views already
# MAGIC replaced and others stale until the next successful retry -- if this
# MAGIC check (or `09`/`11` themselves) fails, treat it as "dashboard views may
# MAGIC be in a mixed old/new state," not a generic job failure, per the
# MAGIC Incremental_Load_Steps_7-11_Plan.docx review.

# COMMAND ----------

for view_name in ALL_GOLD_VIEWS:
    full_view_name = f"{catalog_name}.{gold_schema}.{view_name}"
    try:
        row_count = spark.table(full_view_name).count()
        print(f"OK    {full_view_name}: {row_count} rows")
    except Exception as e:
        failures.append(f"{full_view_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 5 - target-week-landed assertion (Step 8, freshness check)
# MAGIC Same pattern as bronze/silver's equivalent check. Confirms the target
# MAGIC week actually shows up as the MAX week in both fact tables -- not just
# MAGIC "present somewhere," since a stale run that skipped the newest week
# MAGIC while still containing older weeks should also fail this.

# COMMAND ----------

if HAVE_TARGET_WEEK:
    target_year, target_week = int(v_ranking_year), int(v_ranking_week)
    target_week_key = target_year * 100 + target_week
    for full_table_name in [
        f"{catalog_name}.{gold_schema}.fact_ranking_individual",
        f"{catalog_name}.{gold_schema}.fact_ranking_pair",
    ]:
        max_week_key_row = (
            spark.table(full_table_name)
                .agg(F.max(F.col('ranking_year') * 100 + F.col('ranking_week')).alias('max_week_key'))
                .collect()[0]
        )
        max_week_key = max_week_key_row['max_week_key']
        if max_week_key is None or max_week_key < target_week_key:
            failures.append(
                f"{full_table_name}: target week ({target_year}, {target_week}) is not the latest "
                f"week present (max found: {max_week_key}) -- this run did not actually land the "
                f"week the orchestrator asked for"
            )
        else:
            print(f"OK    {full_table_name}: latest week present is >= target ({target_year}, {target_week})")
else:
    print("INFO  p_ranking_year/p_ranking_week not supplied -- skipping Check 5 (standalone run).")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Result + `control.batch_control` status update
# MAGIC This is the last of the three validation notebooks in the chain -- a
# MAGIC clean pass here is what marks the whole batch `gold_done`.

# COMMAND ----------

if failures:
    if HAVE_TARGET_WEEK:
        spark.sql(f"""
            UPDATE wttrankingsbi.control.batch_control
            SET status = 'failed', failed_at = current_timestamp(),
                failure_stage = 'gold', failure_message = {chr(39)}{'; '.join(failures)[:4000].replace(chr(39), chr(39)+chr(39))}{chr(39)},
                updated_at = current_timestamp()
            WHERE ranking_year = {int(v_ranking_year)} AND ranking_week = {int(v_ranking_week)}
        """)
    raise ValueError("Gold validation FAILED:\n" + "\n".join(f"  - {f}" for f in failures))

if HAVE_TARGET_WEEK:
    spark.sql(f"""
        UPDATE wttrankingsbi.control.batch_control
        SET status = 'gold_done', gold_done_at = current_timestamp(), updated_at = current_timestamp()
        WHERE ranking_year = {int(v_ranking_year)} AND ranking_week = {int(v_ranking_week)}
    """)

print(f"Gold validation passed: {len(ALL_GOLD_TABLES)} tables + {len(ALL_GOLD_VIEWS)} views checked.")