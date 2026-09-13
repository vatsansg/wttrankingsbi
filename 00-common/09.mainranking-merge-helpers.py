# Databricks notebook source
# MAGIC %md
# MAGIC ## 09. MainRanking merge helpers
# MAGIC Shared by the modified `03-silver/02.Silver Ranking Individuals.py` and
# MAGIC `03.Silver Ranking Pairs.py` -- the logic for folding
# MAGIC `bronze.main_ranking_historical` (a bulk, geography-less, audit-column-
# MAGIC less historical source) into the same silver tables the real weekly
# MAGIC landing-CSV pipeline builds, without one source silently corrupting the
# MAGIC other. Three review findings from the design pass drove the shape of
# MAGIC this file specifically -- see `README.md`'s "Design decisions" section
# MAGIC for the full writeup, this is the short version:
# MAGIC 1. `previous_period` LAG values must be computed **after** the union +
# MAGIC    dedup of both sources, on the final series -- computing it on the
# MAGIC    MainRanking branch alone before the union would hand a week-32 row a
# MAGIC    "previous rank" from a week-31 figure that isn't the one that actually
# MAGIC    survives dedup for week 31.
# MAGIC 2. Dedup on a source collision (both a landing-CSV row and a
# MAGIC    MainRanking-derived row exist for the same key -- true for every week
# MAGIC    both were loaded for) prefers the landing-CSV row, but only *after*
# MAGIC    comparing the two sources' rank/points and flagging a disagreement --
# MAGIC    silently picking one without checking would defeat the entire point of
# MAGIC    the progressive-week test this merge exists to support.
# MAGIC 3. Every derived/backfilled column is filled via `coalesce(existing,
# MAGIC    derived)`, never an overwrite -- a landing-CSV row's own
# MAGIC    already-published `previous_rank` etc. is authoritative and must never
# MAGIC    be replaced by a value this notebook computed.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def dedup_preferring_landing_csv(df, key_cols, source_col='_source_system',
                                  compare_cols=None, mismatch_col='_dq_source_mismatch'):
    """Collapse to one row per key_cols. When both 'LANDING_CSV' and
    'MAIN_RANKING_HISTORICAL' rows exist for the same key, keep the LANDING_CSV
    row -- but first flag (not fail) any case where compare_cols disagree
    between the two sources beyond exact match, via a boolean mismatch_col on
    the surviving row, so 07.Validate All Silver Tables can surface it as a
    data-quality count rather than it being silently swallowed. When only one
    source has a row for a key, that row passes through with mismatch_col=False.
    """
    compare_cols = compare_cols or []
    key_window = Window.partitionBy(*key_cols)

    with_group_size = df.withColumn('_group_size', F.count('*').over(key_window))

    mismatch_expr = F.lit(False)
    if compare_cols:
        for c in compare_cols:
            landing_val = F.max(F.when(F.col(source_col) == 'LANDING_CSV', F.col(c))).over(key_window)
            main_val = F.max(F.when(F.col(source_col) == 'MAIN_RANKING_HISTORICAL', F.col(c))).over(key_window)
            col_mismatch = (
                (F.col('_group_size') > 1)
                & landing_val.isNotNull() & main_val.isNotNull()
                & (landing_val != main_val)
            )
            mismatch_expr = mismatch_expr | F.coalesce(col_mismatch, F.lit(False))

    flagged_df = with_group_size.withColumn(mismatch_col, mismatch_expr)

    # Preference order: LANDING_CSV first when both exist, else whichever exists.
    pref_window = Window.partitionBy(*key_cols).orderBy(
        F.when(F.col(source_col) == 'LANDING_CSV', 0).otherwise(1)
    )
    deduped = (
        flagged_df
            .withColumn('_rn', F.row_number().over(pref_window))
            .filter(F.col('_rn') == 1)
            .drop('_rn', '_group_size')
    )
    return deduped


def backfill_previous_period_columns(df, key_cols, order_cols,
                                      current_col, current_prev_col,
                                      points_col, points_prev_col,
                                      difference_col):
    """After union + dedup, derive `<x>_prev` / difference columns via
    LAG(1) over (key_cols, ordered by order_cols) and coalesce them into the
    existing columns -- never overwrites a value the row already had (e.g. a
    landing-CSV row's own already-published PreviousRank/RankingDifference).
    Only rows that came in with a NULL there (MainRanking-sourced rows, or a
    landing-CSV row's first-ever appearance in this key) get the derived
    value.

    BUGFIX (caught on the real Step 2b silver run against pairs data):
    `current_prev_col`/`points_prev_col` are assumed to already exist on `df`
    so the coalesce can prefer a landing-CSV row's own published value over
    the derived one. That's true for individuals (`ranking_points__previous`
    is a real landing-CSV column) but NOT for pairs -- the pairs CSV schema
    never had a persisted "points previous" column at all (only individuals'
    RankingPoints_Previous does), so the caller passes a throwaway column
    name (e.g. `_temp_points_previous`) that was never created, and
    `F.col(points_prev_col)` failed with UNRESOLVED_COLUMN. Fixed by adding
    any named "previous" column that doesn't already exist on `df` as an
    all-NULL column (typed to match its `current_col`/`points_col`
    counterpart) before the coalesce -- every row then has nothing to prefer
    over the LAG-derived value, which is exactly correct since there was
    never an existing value in the first place.
    """
    for prev_col, like_col in ((current_prev_col, current_col), (points_prev_col, points_col)):
        if prev_col not in df.columns:
            df = df.withColumn(prev_col, F.lit(None).cast(df.schema[like_col].dataType))

    w = Window.partitionBy(*key_cols).orderBy(*order_cols)
    lag_current = F.lag(current_col, 1).over(w)
    lag_points = F.lag(points_col, 1).over(w)

    out = (
        df
            .withColumn(current_prev_col, F.coalesce(F.col(current_prev_col), lag_current))
            .withColumn(points_prev_col, F.coalesce(F.col(points_prev_col), lag_points))
    )
    derived_difference = F.col(points_col) - F.col(points_prev_col)
    out = out.withColumn(
        difference_col,
        F.coalesce(F.col(difference_col), derived_difference.cast('int'))
        if difference_col in df.columns else derived_difference.cast('int'),
    )
    return out
