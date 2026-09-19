# Databricks notebook source
# MAGIC %md
# MAGIC # Parallel Run Spot Check
# MAGIC Step 11's validation tool. Run this **by hand, once per week**, after
# MAGIC the Step 9 unified pipeline job (`wtt_ranking_unified_pipeline`) has
# MAGIC completed a real week -- this is the "independently spot-check its
# MAGIC output" half of `Next_Steps_and_API_Guide.md` §8 Step 11, done as a
# MAGIC repeatable notebook instead of re-typing §3's spot-check queries by
# MAGIC hand each time.
# MAGIC
# MAGIC **Deliberately not wired into any job.** Step 11 is a transitional,
# MAGIC human-in-the-loop phase by design -- the whole point is a person
# MAGIC looking at the result each week before trusting the automation
# MAGIC further, not another automated gate. Once 2-3 weeks in a row come back
# MAGIC clean (or clean-with-expected-warnings), that's the signal to close
# MAGIC out Step 11 and retire the by-hand §8 walkthrough as primary.
# MAGIC
# MAGIC Checks the **latest `gold_done` week** in `control.batch_control` by
# MAGIC default (override with the `p_ranking_year`/`p_ranking_week` widgets
# MAGIC to check a specific earlier week instead). Every check's result is
# MAGIC logged to `control.parallel_run_validation_log` (created here if it
# MAGIC doesn't exist yet) so the cutover decision has a written trail, not
# MAGIC just "I looked at it and it seemed fine."
# MAGIC
# MAGIC This does NOT duplicate Step 10's freshness job (staleness /
# MAGIC stuck-in-progress / gold-control consistency) -- those already run
# MAGIC daily on their own. This notebook is about **correctness of that
# MAGIC week's actual numbers**, the same spirit as `Next_Steps_and_API_Guide.md`
# MAGIC §3's spot-check queries.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

dbutils.widgets.text("p_ranking_year", "")
dbutils.widgets.text("p_ranking_week", "")

p_ranking_year = dbutils.widgets.get("p_ranking_year").strip()
p_ranking_week = dbutils.widgets.get("p_ranking_week").strip()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 0 -- create the log table if this is the first run, resolve target week

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS wttrankingsbi.control.parallel_run_validation_log (
        ranking_year INT,
        ranking_week INT,
        checked_at TIMESTAMP,
        check_name STRING,
        status STRING,
        detail STRING
    ) USING DELTA
""")

if p_ranking_year and p_ranking_week:
    target_year, target_week = int(p_ranking_year), int(p_ranking_week)
    print(f"INFO  Checking explicitly requested week: ({target_year}, {target_week})")
else:
    latest_df = spark.sql("""
        SELECT ranking_year, ranking_week
        FROM wttrankingsbi.control.batch_control
        WHERE status = 'gold_done'
        ORDER BY ranking_year DESC, ranking_week DESC
        LIMIT 1
    """)
    rows = latest_df.collect()
    if not rows:
        raise ValueError("No gold_done row in control.batch_control -- nothing to validate yet.")
    target_year, target_week = rows[0]['ranking_year'], rows[0]['ranking_week']
    print(f"INFO  No week specified -- checking the latest gold_done week: "
          f"({target_year}, {target_week})")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Helper -- log + print each check's result

# COMMAND ----------

from datetime import datetime

check_results = []

def record(check_name, status, detail):
    """status: 'PASS' | 'WARN' | 'FAIL'"""
    check_results.append((check_name, status, detail))
    marker = {"PASS": "OK   ", "WARN": "WARN ", "FAIL": "FAIL "}[status]
    print(f"{marker} {check_name}: {detail}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check A -- both fact tables have rows for this week

# COMMAND ----------

ind_count = spark.sql(f"""
    SELECT COUNT(*) AS n FROM wttrankingsbi.gold.fact_ranking_individual
    WHERE ranking_year = {target_year} AND ranking_week = {target_week}
""").collect()[0]['n']

pair_count = spark.sql(f"""
    SELECT COUNT(*) AS n FROM wttrankingsbi.gold.fact_ranking_pair
    WHERE ranking_year = {target_year} AND ranking_week = {target_week}
""").collect()[0]['n']

if ind_count > 0 and pair_count > 0:
    record("fact_row_counts", "PASS",
           f"fact_ranking_individual={ind_count}, fact_ranking_pair={pair_count}")
else:
    record("fact_row_counts", "FAIL",
           f"fact_ranking_individual={ind_count}, fact_ranking_pair={pair_count} -- "
           f"one or both are empty for ({target_year}, {target_week})")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check B -- dim_country grain (no duplicate country_code -- would fan
# MAGIC out every downstream join)

# COMMAND ----------

dup_country_df = spark.sql("""
    SELECT country_code, COUNT(*) AS n
    FROM wttrankingsbi.gold.dim_country
    GROUP BY country_code HAVING COUNT(*) > 1
""")
dup_country_rows = dup_country_df.collect()

if not dup_country_rows:
    record("dim_country_grain", "PASS", "no duplicate country_code values")
else:
    record("dim_country_grain", "FAIL",
           f"{len(dup_country_rows)} duplicate country_code value(s): "
           f"{[r['country_code'] for r in dup_country_rows]}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check C -- v_continental_pulse has no NULL-continent rows beyond the
# MAGIC known ITTF/RWORLD/WTT placeholder group (see §1 of Next_Steps_and_API_Guide.md)

# COMMAND ----------

null_continent_df = spark.sql("""
    SELECT ranking_run_code, SUM(ranked_individuals) AS null_continent_individuals
    FROM wttrankingsbi.gold.v_continental_pulse
    WHERE continent_name IS NULL
    GROUP BY ranking_run_code
""")
null_continent_rows = null_continent_df.collect()
total_null_continent = sum(r['null_continent_individuals'] or 0 for r in null_continent_rows)

# A small non-zero count is expected (the ITTF/RWORLD/WTT placeholder codes) --
# only flag if it looks like it's grown materially past that known baseline.
NULL_CONTINENT_WARN_THRESHOLD = 50

if total_null_continent == 0:
    record("continental_pulse_nulls", "PASS", "no NULL-continent rows")
elif total_null_continent <= NULL_CONTINENT_WARN_THRESHOLD:
    record("continental_pulse_nulls", "PASS",
           f"{total_null_continent} individual(s) with NULL continent -- within the "
           f"known ITTF/RWORLD/WTT placeholder baseline")
else:
    record("continental_pulse_nulls", "WARN",
           f"{total_null_continent} individual(s) with NULL continent -- above the "
           f"expected placeholder baseline ({NULL_CONTINENT_WARN_THRESHOLD}), worth a look")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check D -- previous_rank population rate (regression check for the
# MAGIC Step 8 frozen-seed LAG fix -- this is exactly the bug that fix closed,
# MAGIC so it's the single highest-value check to keep re-running on every new
# MAGIC week)

# COMMAND ----------

prev_rank_df = spark.sql(f"""
    SELECT
        ranking_run_code,
        COUNT(*) AS total_rows,
        SUM(CASE WHEN previous_rank IS NOT NULL THEN 1 ELSE 0 END) AS with_previous_rank
    FROM wttrankingsbi.gold.fact_ranking_individual
    WHERE ranking_year = {target_year} AND ranking_week = {target_week}
    GROUP BY ranking_run_code
""")
prev_rank_rows = prev_rank_df.collect()

# A brand-new entrant naturally has no previous_rank, so 100% is not expected --
# but a healthy established week should still be comfortably above half.
PREVIOUS_RANK_WARN_THRESHOLD_PCT = 50.0

any_prev_rank_warn = False
for r in prev_rank_rows:
    pct = (r['with_previous_rank'] / r['total_rows'] * 100) if r['total_rows'] else 0
    detail = (f"{r['ranking_run_code']}: {r['with_previous_rank']}/{r['total_rows']} "
              f"({pct:.1f}%) have previous_rank populated")
    if pct >= PREVIOUS_RANK_WARN_THRESHOLD_PCT:
        record("previous_rank_population", "PASS", detail)
    else:
        any_prev_rank_warn = True
        record("previous_rank_population", "WARN",
               detail + f" -- below the {PREVIOUS_RANK_WARN_THRESHOLD_PCT}% expectation; "
               f"if this is NOT the first week of a new season/category, re-check the "
               f"frozen-seed LAG logic in 03-silver/02 and 03 (this is exactly the bug "
               f"Step 8 fixed)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check E -- week-over-week player count sanity (catches a
# MAGIC catastrophic silent data loss or duplication that wouldn't otherwise
# MAGIC raise an error)

# COMMAND ----------

wow_df = spark.sql(f"""
    WITH weeks AS (
        SELECT DISTINCT ranking_year, ranking_week
        FROM wttrankingsbi.gold.fact_ranking_individual
        WHERE (ranking_year * 100 + ranking_week) <= {target_year * 100 + target_week}
        ORDER BY ranking_year DESC, ranking_week DESC
        LIMIT 2
    ),
    counts AS (
        SELECT f.ranking_year, f.ranking_week, f.ranking_run_code,
               COUNT(DISTINCT f.ittfid) AS player_count
        FROM wttrankingsbi.gold.fact_ranking_individual f
        JOIN weeks w ON f.ranking_year = w.ranking_year AND f.ranking_week = w.ranking_week
        GROUP BY f.ranking_year, f.ranking_week, f.ranking_run_code
    )
    SELECT * FROM counts ORDER BY ranking_run_code, ranking_year DESC, ranking_week DESC
""")
wow_rows = wow_df.collect()

by_run = {}
for r in wow_rows:
    by_run.setdefault(r['ranking_run_code'], []).append(r)

WOW_CHANGE_WARN_THRESHOLD_PCT = 25.0

if not by_run:
    record("week_over_week_player_count", "WARN", "no prior week available to compare against")
else:
    for run_code, rs in by_run.items():
        if len(rs) < 2:
            record("week_over_week_player_count", "WARN",
                   f"{run_code}: only one week of history available, nothing to compare")
            continue
        current, previous = rs[0], rs[1]
        pct_change = ((current['player_count'] - previous['player_count'])
                       / previous['player_count'] * 100) if previous['player_count'] else 0
        detail = (f"{run_code}: {previous['player_count']} "
                  f"({previous['ranking_year']}/{previous['ranking_week']}) -> "
                  f"{current['player_count']} ({current['ranking_year']}/{current['ranking_week']}), "
                  f"{pct_change:+.1f}%")
        if abs(pct_change) <= WOW_CHANGE_WARN_THRESHOLD_PCT:
            record("week_over_week_player_count", "PASS", detail)
        else:
            record("week_over_week_player_count", "WARN",
                   detail + f" -- swing exceeds ±{WOW_CHANGE_WARN_THRESHOLD_PCT}%, worth a look")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Log results and summarize

# COMMAND ----------

now = datetime.utcnow()
log_rows = [(target_year, target_week, now, name, status, detail)
            for name, status, detail in check_results]

spark.createDataFrame(
    log_rows,
    schema="ranking_year INT, ranking_week INT, checked_at TIMESTAMP, "
           "check_name STRING, status STRING, detail STRING"
).write.mode("append").saveAsTable("wttrankingsbi.control.parallel_run_validation_log")

fail_count = sum(1 for _, status, _ in check_results if status == "FAIL")
warn_count = sum(1 for _, status, _ in check_results if status == "WARN")

print()
print(f"=== Summary for ({target_year}, {target_week}): "
      f"{len(check_results)} checks, {fail_count} FAIL, {warn_count} WARN ===")

if fail_count > 0:
    raise ValueError(
        f"PARALLEL RUN SPOT CHECK FAILED for ({target_year}, {target_week}): "
        f"{fail_count} check(s) failed. See the printed detail above and the "
        f"logged rows in control.parallel_run_validation_log."
    )

print("INFO  No hard failures. Review any WARNs above before counting this as "
      "one of the 2-3 clean weeks for the Step 11 cutover decision.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### How many weeks have gone clean so far?
# MAGIC Run this cell any time to see the cutover-decision tally -- a week counts
# MAGIC as "clean" here if it has zero FAIL rows logged (WARNs still require a
# MAGIC human read before counting it, per the note above).

# COMMAND ----------

display(spark.sql("""
    SELECT
        ranking_year, ranking_week,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS fail_count,
        SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warn_count,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS pass_count,
        MAX(checked_at) AS last_checked_at
    FROM wttrankingsbi.control.parallel_run_validation_log
    GROUP BY ranking_year, ranking_week
    ORDER BY ranking_year DESC, ranking_week DESC
"""))