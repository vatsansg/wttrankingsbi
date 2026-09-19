-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup Control Schema
-- MAGIC Step 8 replacement for the old, unadapted `01-setup/02.Setup Batch
-- MAGIC Events.sql` (removed from the live workspace 2026-09-19 -- it created
-- MAGIC its table in the `formula1` catalog on the F1 course's own storage
-- MAGIC account, never rewired for this project). This is the real
-- MAGIC `control.batch_control` table, built for `wttrankingsbi` from scratch.
-- MAGIC
-- MAGIC One row per `(ranking_year, ranking_week)` ever detected. `02.Detect New
-- MAGIC Week` inserts the `pending` row; each layer's validation notebook (the
-- MAGIC patched `09`/`07`/`10` in this folder set) stamps its own `_done_at`
-- MAGIC column and flips `status` forward once its target-week assertion passes.
-- MAGIC A `failed` row plus `failure_stage`/`failure_message` is what Step 10's
-- MAGIC freshness check and alerting read to tell "never ran" apart from "ran and
-- MAGIC broke partway".
-- MAGIC
-- MAGIC Run this notebook once, by hand, before building anything else in Step
-- MAGIC 8. Safe to re-run (every statement is `IF NOT EXISTS`).

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS wttrankingsbi.control
    MANAGED LOCATION 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/control';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS wttrankingsbi.control.batch_control
(
    batch_id            BIGINT GENERATED ALWAYS AS IDENTITY,
    ranking_year        INT NOT NULL COMMENT 'From the landing folder name "<year> - <week>"',
    ranking_week        INT NOT NULL COMMENT 'From the landing folder name "<year> - <week>"',
    status              STRING NOT NULL COMMENT 'pending -> bronze_done -> silver_done -> gold_done, or failed at any stage',
    detected_at         TIMESTAMP COMMENT 'When 02.Detect New Week first saw this week and inserted this row',
    bronze_done_at      TIMESTAMP COMMENT 'Stamped by the patched 09.Validate All Bronze Tables once its target-week assertion passes',
    silver_done_at      TIMESTAMP COMMENT 'Stamped by the patched 07.Validate All Silver Tables once its target-week assertion passes',
    gold_done_at        TIMESTAMP COMMENT 'Stamped by the patched 10.Validate All Gold Tables once its target-week assertion passes',
    failed_at           TIMESTAMP,
    failure_stage       STRING COMMENT 'bronze / silver / gold -- which validation notebook raised the failure',
    failure_message     STRING,
    updated_at          TIMESTAMP
)
USING DELTA
COMMENT 'Step 8 orchestration control table -- one row per ranking week the detect-new-week notebook has ever seen. Not physically unique-constrained (Delta does not enforce this cheaply); 02.Detect New Week is responsible for never inserting a second pending row for a (ranking_year, ranking_week) that already has one.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Smoke test -- confirm the table is writable, then clean up
-- MAGIC Mirrors the pattern already used (and since commented-out, per your
-- MAGIC 2026-09-19 change) in `01-setup/01.Setup Project Environment.sql`'s own
-- MAGIC storage-credential smoke test -- same idea, this table's own schema/
-- MAGIC location instead.

-- COMMAND ----------

-- INSERT INTO wttrankingsbi.control.batch_control
--     (ranking_year, ranking_week, status, detected_at, updated_at)
-- VALUES (1900, 1, '_smoke_test', current_timestamp(), current_timestamp());

-- SELECT * FROM wttrankingsbi.control.batch_control WHERE status = '_smoke_test';

-- DELETE FROM wttrankingsbi.control.batch_control WHERE status = '_smoke_test';

-- COMMAND ----------

INSERT INTO wttrankingsbi.control.batch_control
    (ranking_year, ranking_week, status, detected_at, bronze_done_at,
     silver_done_at, gold_done_at, updated_at)
VALUES
    (2026, 31, 'gold_done', current_timestamp(), current_timestamp(),
     current_timestamp(), current_timestamp(), current_timestamp()),
    (2026, 32, 'gold_done', current_timestamp(), current_timestamp(),
     current_timestamp(), current_timestamp(), current_timestamp());

-- COMMAND ----------

--  CREATE TABLE wttrankingsbi.silver._parity_check_individuals_old AS
--    SELECT * FROM wttrankingsbi.silver.ranking_individuals;
--    CREATE TABLE wttrankingsbi.silver._parity_check_pairs_old AS
--    SELECT * FROM wttrankingsbi.silver.ranking_pairs;

-- COMMAND ----------

-- SELECT 'individuals' AS tbl, COUNT(*) AS mismatched_rows FROM (
--     SELECT * EXCEPT (_silver_updated_timestamp) FROM wttrankingsbi.silver.ranking_individuals
--     MINUS
--     SELECT * EXCEPT (_silver_updated_timestamp) FROM wttrankingsbi.silver._parity_check_individuals_old
-- )
-- UNION ALL
-- SELECT 'individuals_reverse', COUNT(*) FROM (
--     SELECT * EXCEPT (_silver_updated_timestamp) FROM wttrankingsbi.silver._parity_check_individuals_old
--     MINUS
--     SELECT * EXCEPT (_silver_updated_timestamp) FROM wttrankingsbi.silver.ranking_individuals
-- )
-- UNION ALL
-- SELECT 'pairs', COUNT(*) FROM (
--     SELECT * EXCEPT (_silver_updated_timestamp) FROM wttrankingsbi.silver.ranking_pairs
--     MINUS
--     SELECT * EXCEPT (_silver_updated_timestamp) FROM wttrankingsbi.silver._parity_check_pairs_old
-- )
-- UNION ALL
-- SELECT 'pairs_reverse', COUNT(*) FROM (
--     SELECT * EXCEPT (_silver_updated_timestamp) FROM wttrankingsbi.silver._parity_check_pairs_old
--     MINUS
--     SELECT * EXCEPT (_silver_updated_timestamp) FROM wttrankingsbi.silver.ranking_pairs
-- );

-- COMMAND ----------

-- drop table wttrankingsbi.silver._parity_check_individuals_old;

-- COMMAND ----------

-- drop table wttrankingsbi.silver._parity_check_pairs_old