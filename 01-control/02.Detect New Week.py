# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Detect New Week
# MAGIC Step 8's orchestration entry point. Scans the landing volume for a
# MAGIC `"<year> - <week>"` folder that `control.batch_control` doesn't already
# MAGIC have a row for, picks the oldest one (FIFO -- in case more than one
# MAGIC queued up between runs), inserts a `pending` row, and emits
# MAGIC `ranking_year`/`ranking_week` via `dbutils.jobs.taskValues.set` for the
# MAGIC downstream bronze ingest tasks (`01`/`02`) and every layer's validation
# MAGIC notebook (which needs the target week for its freshness assertion --
# MAGIC see the patched `09`/`07`/`10` in this folder set) to pick up.
# MAGIC
# MAGIC **Exits cleanly, not as a failure, when no new week is found** -- per the
# MAGIC 2026-09-15 decision in `Next_Steps_and_API_Guide.md` §8 (a cron trigger
# MAGIC can fire slightly ahead of the real ADF landing drop; "nothing new yet"
# MAGIC is the normal case, not an error).
# MAGIC
# MAGIC Run manually today (Step 8 is still "run by hand"); Step 9 makes this the
# MAGIC first task in the single scheduled job.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

import re
from datetime import datetime

WEEK_FOLDER_PATTERN = re.compile(r'^(\d{4}) - (\d{1,2})$')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - List candidate weeks from the landing volume
# MAGIC Explicitly matches the `"YYYY - WW"` folder-name pattern rather than
# MAGIC assuming every subdirectory is a week -- the landing volume also holds a
# MAGIC `historical/` subfolder (the one-off MainRanking CSV, notebook
# MAGIC `02-bronze/10`), which this pattern naturally excludes without needing a
# MAGIC special-case skip.

# COMMAND ----------

try:
    landing_entries = dbutils.fs.ls(landing_folder_path)
except Exception as e:
    raise ValueError(f"Could not list landing volume at {landing_folder_path}: {e}")

candidate_weeks = []
for entry in landing_entries:
    if not entry.isDir():
        continue
    folder_name = entry.name.rstrip('/')
    m = WEEK_FOLDER_PATTERN.match(folder_name)
    if m:
        candidate_weeks.append((int(m.group(1)), int(m.group(2))))

candidate_weeks.sort()  # oldest first
print(f"INFO  {len(candidate_weeks)} week-shaped folder(s) found under {landing_folder_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Compare against `control.batch_control`, pick the oldest
# MAGIC unprocessed week
# MAGIC "Unprocessed" = no row at all yet for that `(ranking_year, ranking_week)`
# MAGIC -- a row in any status (including `failed`) counts as "already handled by
# MAGIC this table" so a failed week doesn't get silently re-detected as brand
# MAGIC new; re-running a failed week is a deliberate manual action (update its
# MAGIC row back to `pending`, or delete it), not something detect does on its
# MAGIC own.

# COMMAND ----------

already_known_df = spark.sql("""
    SELECT DISTINCT ranking_year, ranking_week
    FROM wttrankingsbi.control.batch_control
""")
already_known = {(r['ranking_year'], r['ranking_week']) for r in already_known_df.collect()}

new_weeks = [w for w in candidate_weeks if w not in already_known]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Insert the pending row (if any) and emit taskValues
# MAGIC Test this by pointing it at a workspace where week 31 is already loaded
# MAGIC and already has a `batch_control` row -- confirm it reports "already
# MAGIC processed" for week 31 rather than re-detecting it, per Step 8's
# MAGIC required manual test.

# COMMAND ----------

if not new_weeks:
    print("INFO  No new week found -- nothing in the landing volume is unprocessed. Exiting cleanly (not a failure).")
    dbutils.jobs.taskValues.set(key="found", value=False)
    dbutils.notebook.exit("no_new_week")

target_year, target_week = new_weeks[0]

if len(new_weeks) > 1:
    print(f"WARN  {len(new_weeks)} unprocessed weeks queued ({new_weeks}) -- "
          f"processing the oldest ({target_year}, {target_week}) this run; "
          f"the rest will be picked up on subsequent runs.")

spark.sql(f"""
    INSERT INTO wttrankingsbi.control.batch_control
        (ranking_year, ranking_week, status, detected_at, updated_at)
    VALUES
        ({target_year}, {target_week}, 'pending', current_timestamp(), current_timestamp())
""")

print(f"INFO  New week detected and recorded: ({target_year}, {target_week})")

dbutils.jobs.taskValues.set(key="found", value=True)
dbutils.jobs.taskValues.set(key="ranking_year", value=str(target_year))
dbutils.jobs.taskValues.set(key="ranking_week", value=str(target_week))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manual test procedure (do this before Step 9 wires this into a job)
# MAGIC 1. Run this notebook by hand against your current workspace, where weeks
# MAGIC    up through the latest one you've loaded already have rows in
# MAGIC    `batch_control` (backfill those rows once by hand for whatever you've
# MAGIC    already loaded manually via Step 7's by-hand walkthrough, before
# MAGIC    relying on this notebook -- `control.batch_control` starts empty and
# MAGIC    has no memory of runs done before Step 8 existed).
# MAGIC 2. Confirm it prints `No new week found` if every landing folder already
# MAGIC    has a row -- this is the "already processed" idempotency check.
# MAGIC 3. Drop a new `"<year> - <week>"` folder (or point `landing_folder_path`
# MAGIC    at a test volume with one) and re-run -- confirm it detects exactly
# MAGIC    that one week, inserts one `pending` row, and does not touch any
# MAGIC    existing row.