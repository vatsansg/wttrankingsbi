-- =====================================================================================================
-- WTT Rankings BI -- Phase 12 cut-over: reconciliation checks, then retirement of the legacy ledger
-- Run in a Databricks SQL editor (or a SQL notebook) on the dashboards' warehouse.
--   PART A: before retiring anything (after the initial loads and the first weekly run).
--   PART B: ONLY after PART A looks right AND at least one weekly job run has succeeded on the amended job.
-- =====================================================================================================

-- ---------- PART A.1  New consolidated bronze vs the three legacy bronze tables, per source week ----------
-- Expect: weeks present in the legacy _log/_log_archives tables match the new table week-for-week
-- (the new table is _Log U _Log_Archives at cut-over, then gains one week per run from the master table).
WITH legacy AS (
  SELECT RankingYear, RankingWeek, COUNT(*) AS legacy_rows FROM (
    SELECT RankingYear, RankingWeek FROM wttrankingsbi.bronze.players_events_results_master_log
    UNION ALL
    SELECT RankingYear, RankingWeek FROM wttrankingsbi.bronze.players_events_results_master_log_archives
  ) GROUP BY RankingYear, RankingWeek
),
newt AS (
  SELECT RankingYear, RankingWeek, COUNT(*) AS new_rows
  FROM wttrankingsbi.bronze.players_events_results GROUP BY RankingYear, RankingWeek
)
SELECT COALESCE(l.RankingYear, n.RankingYear) AS ranking_year, COALESCE(l.RankingWeek, n.RankingWeek) AS ranking_week,
       l.legacy_rows, n.new_rows, n.new_rows - l.legacy_rows AS diff
FROM legacy l FULL OUTER JOIN newt n ON l.RankingYear = n.RankingYear AND l.RankingWeek = n.RankingWeek
WHERE l.legacy_rows IS NULL OR n.new_rows IS NULL OR l.legacy_rows <> n.new_rows
ORDER BY 1, 2;
-- Expected: no rows, except possibly the newest week(s) loaded after the legacy tables' last refresh.

-- ---------- PART A.2  Silver: new table vs retired points_ledger ----------
-- The new silver keeps MORE rows: points_ledger keyed on result_position (no event_id / age category) and so
-- collapsed distinct results; and it labelled MDI/WDI/XDI rows as PAIR. Both are fixed in 03-silver/08.
SELECT 'points_ledger (retired)' AS source, entity_type, COUNT(*) AS rows,
       SUM(CASE WHEN identity_resolved THEN 0 ELSE 1 END) AS unresolved
FROM wttrankingsbi.silver.points_ledger GROUP BY entity_type
UNION ALL
SELECT 'player_event_results (new)', entity_type, COUNT(*), SUM(CASE WHEN identity_resolved THEN 0 ELSE 1 END)
FROM wttrankingsbi.silver.player_event_results GROUP BY entity_type
ORDER BY 1, 2;

-- ---------- PART A.3  Entity-type fix proof: MDI/WDI/XDI competitor ids are players, not pairs ----------
SELECT ranking_category_code, entity_type, COUNT(*) AS rows,
       ROUND(100.0 * AVG(CASE WHEN identity_resolved THEN 1 ELSE 0 END), 1) AS pct_resolved
FROM wttrankingsbi.silver.player_event_results
GROUP BY ranking_category_code, entity_type ORDER BY 1;

-- ---------- PART A.4  Events metadata join coverage (results whose event has metadata) ----------
SELECT * FROM wttrankingsbi.gold.v_evt_kpis;

-- ---------- PART A.5  Nothing else still reads the legacy objects ----------
-- In the workspace search box, search for: points_ledger, players_events_results_master
-- Expected hits: only the retired notebooks 02-bronze/03, 04, 05 and 03-silver/04 (deleted in PART B), and markdown history.

-- =====================================================================================================
-- PART B  RETIRE (irreversible for managed tables after the retention window -- take a DEEP CLONE first if
--         you want a safety copy; the source SQL tables remain the system of record either way)
-- =====================================================================================================
-- Optional safety copy (uncomment to keep for 30 days):
-- CREATE TABLE wttrankingsbi.bronze.zz_retired_players_events_results_master_log DEEP CLONE wttrankingsbi.bronze.players_events_results_master_log;

DROP TABLE IF EXISTS wttrankingsbi.bronze.players_events_results_master;
DROP TABLE IF EXISTS wttrankingsbi.bronze.players_events_results_master_log;
DROP TABLE IF EXISTS wttrankingsbi.bronze.players_events_results_master_log_archives;
DROP TABLE IF EXISTS wttrankingsbi.silver.points_ledger;

-- Then delete these workspace notebooks (they are no longer referenced by the amended job):
--   02-bronze/03.Ingest Players Events Results Master
--   02-bronze/04.Ingest Players Events Results Master Log
--   02-bronze/05.Ingest Players Events Results Master Log Archives
--   03-silver/04.Silver Points Ledger
