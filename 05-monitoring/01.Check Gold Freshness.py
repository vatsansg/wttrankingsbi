# Databricks notebook source
# MAGIC %md
# MAGIC # Check Gold Freshness
# MAGIC Step 10's freshness monitor. Runs independently of the Step 9 unified
# MAGIC pipeline job (its own job, its own schedule -- see
# MAGIC `03-jobs/wtt_ranking_freshness_check_job.yaml`) specifically so it can
# MAGIC catch the case a job-failure alert can't: **the pipeline job never
# MAGIC triggering at all** (schedule paused, deleted, workspace issue, etc.).
# MAGIC A check that only runs as the last task of the pipeline it's checking
# MAGIC would never fire in that scenario.
# MAGIC
# MAGIC Three checks, all against `control.batch_control` and `gold.fact_ranking_individual`
# MAGIC only (no bronze/silver reads -- this should be cheap and fast on every run):
# MAGIC
# MAGIC 1. **Staleness** -- the most recent `gold_done` row in `batch_control` was
# MAGIC    updated within the last `STALENESS_THRESHOLD_DAYS` days.
# MAGIC 2. **Stuck-in-progress** -- no row has been sitting in `pending` /
# MAGIC    `bronze_done` / `silver_done` (i.e. started but never reached
# MAGIC    `gold_done` or `failed`) for longer than `STUCK_THRESHOLD_DAYS` --
# MAGIC    catches a run that silently hung or a task that got skipped instead
# MAGIC    of failing loudly.
# MAGIC 3. **Gold/control consistency** -- `gold.fact_ranking_individual`'s actual
# MAGIC    max `(ranking_year, ranking_week)` matches the latest `gold_done` row
# MAGIC    in `batch_control` -- catches `batch_control` being stamped `gold_done`
# MAGIC    without the gold write actually landing (shouldn't happen given how
# MAGIC    `10.Validate All Gold Tables` is wired, but cheap to double-check
# MAGIC    independently rather than trust one signal).
# MAGIC
# MAGIC Raises (fails the task, triggering the job's `on_failure` email) if any
# MAGIC check fails. Prints `OK` and exits cleanly otherwise.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %md
# MAGIC #### Thresholds
# MAGIC The pipeline runs weekly (Tuesday 06:00 Asia/Singapore). 8 days gives a
# MAGIC one-day buffer past the next scheduled run before flagging staleness --
# MAGIC tight enough to catch a missed week within a day, loose enough not to
# MAGIC false-positive on ordinary schedule jitter. 3 days for "stuck" is
# MAGIC generous for a job whose full chain normally finishes in well under an
# MAGIC hour -- anything genuinely stuck that long needs a human regardless of
# MAGIC why.

# COMMAND ----------

STALENESS_THRESHOLD_DAYS = 8
STUCK_THRESHOLD_DAYS = 3

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 1 -- staleness

# COMMAND ----------

latest_gold_done_df = spark.sql("""
    SELECT ranking_year, ranking_week, updated_at,
           datediff(current_timestamp(), updated_at) AS days_since_update
    FROM wttrankingsbi.control.batch_control
    WHERE status = 'gold_done'
    ORDER BY ranking_year DESC, ranking_week DESC
    LIMIT 1
""")

latest_gold_done_rows = latest_gold_done_df.collect()

if not latest_gold_done_rows:
    raise ValueError(
        "FRESHNESS CHECK FAILED (Check 1): no row with status='gold_done' "
        "exists in control.batch_control at all. Either the pipeline has "
        "never completed a run, or the control table is empty/misconfigured."
    )

latest = latest_gold_done_rows[0]
days_since_update = latest['days_since_update']

print(f"INFO  Latest gold_done: ranking_year={latest['ranking_year']}, "
      f"ranking_week={latest['ranking_week']}, updated_at={latest['updated_at']} "
      f"({days_since_update} day(s) ago)")

if days_since_update > STALENESS_THRESHOLD_DAYS:
    raise ValueError(
        f"FRESHNESS CHECK FAILED (Check 1 -- staleness): the most recent "
        f"gold_done row is {days_since_update} day(s) old (threshold: "
        f"{STALENESS_THRESHOLD_DAYS}). Latest processed week: "
        f"({latest['ranking_year']}, {latest['ranking_week']}). Either the "
        f"unified pipeline job didn't fire, or it's failing before "
        f"reaching gold_done -- check the job run history."
    )

print(f"OK    Check 1 (staleness): {days_since_update} day(s) since last "
      f"gold_done, within {STALENESS_THRESHOLD_DAYS}-day threshold.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 2 -- stuck-in-progress

# COMMAND ----------

stuck_df = spark.sql(f"""
    SELECT ranking_year, ranking_week, status, updated_at,
           datediff(current_timestamp(), updated_at) AS days_since_update
    FROM wttrankingsbi.control.batch_control
    WHERE status IN ('pending', 'bronze_done', 'silver_done')
      AND datediff(current_timestamp(), updated_at) > {STUCK_THRESHOLD_DAYS}
    ORDER BY updated_at ASC
""")

stuck_rows = stuck_df.collect()

if stuck_rows:
    details = "; ".join(
        f"({r['ranking_year']}, {r['ranking_week']}) stuck at '{r['status']}' "
        f"for {r['days_since_update']} day(s)"
        for r in stuck_rows
    )
    raise ValueError(
        f"FRESHNESS CHECK FAILED (Check 2 -- stuck-in-progress): "
        f"{len(stuck_rows)} week(s) have been sitting in an intermediate "
        f"status for longer than {STUCK_THRESHOLD_DAYS} day(s): {details}. "
        f"This usually means a run hung or a task was skipped instead of "
        f"failing loudly -- check the job run history for that week."
    )

print(f"OK    Check 2 (stuck-in-progress): no row stuck in an intermediate "
      f"status past {STUCK_THRESHOLD_DAYS} day(s).")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check 3 -- gold/control consistency

# COMMAND ----------

gold_max_df = spark.sql("""
    SELECT MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM wttrankingsbi.gold.fact_ranking_individual
""")
gold_max_key = gold_max_df.collect()[0]['max_week_key']
control_max_key = latest['ranking_year'] * 100 + latest['ranking_week']

if gold_max_key is None:
    raise ValueError(
        "FRESHNESS CHECK FAILED (Check 3 -- consistency): "
        "gold.fact_ranking_individual is empty."
    )

if gold_max_key != control_max_key:
    raise ValueError(
        f"FRESHNESS CHECK FAILED (Check 3 -- consistency): "
        f"batch_control's latest gold_done week key is {control_max_key} "
        f"but gold.fact_ranking_individual's actual max week key is "
        f"{gold_max_key}. batch_control may have been stamped gold_done "
        f"without the gold write actually landing, or a later week was "
        f"loaded into gold outside the normal pipeline."
    )

print(f"OK    Check 3 (consistency): gold.fact_ranking_individual's max "
      f"week key ({gold_max_key}) matches batch_control's latest gold_done "
      f"row ({control_max_key}).")

# COMMAND ----------

print("INFO  All freshness checks passed.")