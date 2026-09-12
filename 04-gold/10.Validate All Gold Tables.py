# Databricks notebook source
# MAGIC %md
# MAGIC # Validate all gold tables
# MAGIC Final task in the gold job. Runs after every dim, fact, and the views
# MAGIC notebook -- fails the job loudly if any of the 8 gold tables is missing,
# MAGIC empty, has a broken business key, or (new for Gold, since this is the
# MAGIC first layer with real star-schema joins) has fact rows whose foreign key
# MAGIC doesn't resolve against its dimension. Same intent and same
# MAGIC proportionate, lightweight scope as bronze's `09.Validate All Bronze
# MAGIC Tables` and silver's `07.Validate All Silver Tables`.

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
]

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
# MAGIC New for Gold -- the first layer with real star-schema joins. Every FK on
# MAGIC the fact tables is a plain carried-forward column, not a lookup (see
# MAGIC `00-common/08.gold-helpers`) -- **except** `ittfid`, which is the one FK
# MAGIC that depends on Silver's identity resolution having succeeded.
# MAGIC `dim_player`/`dim_pair` only contain Silver's *resolved* identity
# MAGIC population, while `identity_resolved = false` fact rows are a real,
# MAGIC accepted, non-zero rate under Step 3's own thresholds (5% individuals /
# MAGIC 65% pairs -- see the Step 3 README's pair-identity-gap finding). So only
# MAGIC the `ittfid` checks below exclude `identity_resolved = false` rows, the
# MAGIC same accommodation Silver already makes -- otherwise this would hard-fail
# MAGIC the Gold job on every single run.
# MAGIC
# MAGIC `country_code` / `subevent_code` / `age_category_code` / `category_code`
# MAGIC are NOT identity-dependent -- they're plain source columns present
# MAGIC whether or not identity resolution succeeded -- so those checks
# MAGIC deliberately run against every row, unresolved-identity rows included.
# MAGIC Narrowing them the same way as `ittfid` would silently stop checking
# MAGIC these FKs on the ~5-65% of rows where identity didn't resolve, masking a
# MAGIC real dimension-snapshot mismatch on exactly that slice.
# MAGIC
# MAGIC A failure here means a dimension build (01-06) used a different source
# MAGIC snapshot than the corresponding fact build (07-08) did within the same
# MAGIC job run, or (for `ittfid`) a resolved row's identity genuinely doesn't
# MAGIC exist in its dimension.

# COMMAND ----------

# fk_col: (fact_table, fact_col, dim_table, dim_col, identity_dependent)
# identity_dependent=True -> exclude identity_resolved=false rows before the
# left-anti join (ittfid only -- see note above).
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
# MAGIC Views are created with `CREATE OR REPLACE VIEW` in notebook `09`, which
# MAGIC doesn't itself catch a broken view definition (a bad column reference
# MAGIC only surfaces when the view is queried) -- this check runs one
# MAGIC `COUNT(*)` per view so a broken view fails the job here, not silently at
# MAGIC first dashboard load.

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
# MAGIC #### Result

# COMMAND ----------

if failures:
    raise ValueError("Gold validation FAILED:\n" + "\n".join(f"  - {f}" for f in failures))

print(f"Gold validation passed: {len(ALL_GOLD_TABLES)} tables + {len(ALL_GOLD_VIEWS)} views checked.")
