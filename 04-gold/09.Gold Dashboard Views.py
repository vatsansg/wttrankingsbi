# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Dashboard Views
# MAGIC One SQL view per dashboard concept from the WTT Rankings Blueprint (§03,
# MAGIC dashboards 01-08) -- each is what a Lakeview dashboard tile queries
# MAGIC directly, and each doubles as a potential REST endpoint later (§04 of
# MAGIC the Blueprint: SQL Statement Execution API / Databricks Apps).
# MAGIC
# MAGIC All 8 read only from the gold facts/dims built by notebooks 01-08 in
# MAGIC this same job -- none of them touch `silver.points_ledger` directly,
# MAGIC since `fact_ranking_individual`/`fact_ranking_pair` already carry the
# MAGIC precomputed `current_rank`/`previous_rank`/`ranking_difference` columns
# MAGIC the source system publishes (per the discovery doc: "straight off
# MAGIC `RankingDifference` -- already computed upstream, just needs
# MAGIC surfacing").
# MAGIC
# MAGIC **Distinct-player counts, not row counts.** A player can appear on
# MAGIC multiple rows of `fact_ranking_individual` (one per subevent they're
# MAGIC ranked in), so every "how many players" view uses
# MAGIC `COUNT(DISTINCT ittfid)`, not `COUNT(*)` -- matching how the Blueprint's
# MAGIC own KPI numbers (e.g. "13,944 ranked individuals") were computed.
# MAGIC
# MAGIC **BUGFIX (post-Step-2b, caught by independent review, 2026-09-13):**
# MAGIC every view below was originally written when `fact_ranking_individual`/
# MAGIC `fact_ranking_pair` only ever held ONE ranking week at a time (the
# MAGIC one-off/full-overwrite track's original shape). Step 2b's historical
# MAGIC backfill + the accumulate-write fix means these facts now hold
# MAGIC ~6 years of weekly history (2021 wk1 through the latest week loaded),
# MAGIC but none of these "current state" views were updated to match -- they
# MAGIC silently blended every historical week into one aggregate (e.g.
# MAGIC `v_ranking_leaders`'s window function had no year/week in its
# MAGIC `PARTITION BY`, so "Top 10" pulled from every week's rank-1 competing
# MAGIC together; `v_ranking_movers` returned every player's entire multi-year
# MAGIC history, not this week's movers). This passed `validate_all_gold_tables`
# MAGIC because it's a semantic bug, not an FK/row-count bug. **Every view below
# MAGIC now joins to a `latest_ind`/`latest_pair` CTE** (the max
# MAGIC `ranking_year*100 + ranking_week` per `ranking_run_code`, the same
# MAGIC sortable key `04.Build Dim Ranking Week` already derives) so every
# MAGIC "current state" dashboard/API endpoint reflects only the latest loaded
# MAGIC week, exactly as it did before Step 2b widened the date range. A future
# MAGIC trend/longitudinal dashboard (the whole point of having multi-year
# MAGIC history at all) should read the fact tables directly, unscoped -- that's
# MAGIC a deliberate, separate design, not an oversight to fix here.
# MAGIC
# MAGIC Depends on `01`-`08` (every gold dim and fact) having run first in this
# MAGIC same job -- see `03-jobs/`.
# MAGIC
# MAGIC Views created (`CREATE OR REPLACE VIEW`, so this notebook is safe to
# MAGIC re-run any time the facts/dims change):
# MAGIC 1. `gold.v_continental_pulse` -- dashboard 01
# MAGIC 2. `gold.v_discipline_landscape` -- dashboard 02
# MAGIC 3. `gold.v_federation_scorecard` -- dashboard 03
# MAGIC 4. `gold.v_youth_pipeline` -- dashboard 04
# MAGIC 5. `gold.v_ranking_movers` -- dashboard 05
# MAGIC 6. `gold.v_doubles_partnerships` -- dashboard 06
# MAGIC 7. `gold.v_fresh_faces` -- dashboard 07
# MAGIC 8. `gold.v_ranking_leaders` -- dashboard 08
# MAGIC
# MAGIC Phase 11B (2026-10-07) -- Pairs Performance page:
# MAGIC 9. `gold.v_pair_profile`
# MAGIC 10. `gold.v_pair_ranking_history`
# MAGIC 11. `gold.v_pair_partner_doubles_individual_history`
# MAGIC 12. `gold.v_pair_partner_doubles_individual`
# MAGIC 13. `gold.v_pair_leaderboard`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

fact_individual = f"{catalog_name}.{gold_schema}.fact_ranking_individual"
fact_pair = f"{catalog_name}.{gold_schema}.fact_ranking_pair"
dim_country = f"{catalog_name}.{gold_schema}.dim_country"
dim_subevent = f"{catalog_name}.{gold_schema}.dim_subevent"
dim_age_category = f"{catalog_name}.{gold_schema}.dim_age_category"
dim_player = f"{catalog_name}.{gold_schema}.dim_player"
dim_pair = f"{catalog_name}.{gold_schema}.dim_pair"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 - `v_continental_pulse` (dashboard 01, "Global & Continental Pulse")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_continental_pulse AS
WITH latest_ind AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_individual} GROUP BY ranking_run_code
)
SELECT
    c.continent_name,
    c.continent_code,
    f.ranking_run_code,
    COUNT(DISTINCT f.ittfid) AS ranked_individuals
FROM {fact_individual} f
JOIN latest_ind l ON f.ranking_run_code = l.ranking_run_code
    AND (f.ranking_year * 100 + f.ranking_week) = l.max_week_key
LEFT JOIN {dim_country} c ON f.country_code = c.country_code
GROUP BY c.continent_name, c.continent_code, f.ranking_run_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - `v_discipline_landscape` (dashboard 02, "Discipline Landscape")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_discipline_landscape AS
WITH latest_ind AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_individual} GROUP BY ranking_run_code
),
latest_pair AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_pair} GROUP BY ranking_run_code
)
SELECT
    s.subevent_code,
    s.subevent_label,
    s.entity_type,
    f.ranking_run_code,
    COUNT(DISTINCT f.ittfid) AS ranked_count
FROM {fact_individual} f
JOIN latest_ind l ON f.ranking_run_code = l.ranking_run_code
    AND (f.ranking_year * 100 + f.ranking_week) = l.max_week_key
LEFT JOIN {dim_subevent} s ON f.subevent_code = s.subevent_code
GROUP BY s.subevent_code, s.subevent_label, s.entity_type, f.ranking_run_code

UNION ALL

SELECT
    s.subevent_code,
    s.subevent_label,
    s.entity_type,
    p.ranking_run_code,
    COUNT(DISTINCT p.ittfid) AS ranked_count
FROM {fact_pair} p
JOIN latest_pair l ON p.ranking_run_code = l.ranking_run_code
    AND (p.ranking_year * 100 + p.ranking_week) = l.max_week_key
LEFT JOIN {dim_subevent} s ON p.subevent_code = s.subevent_code
GROUP BY s.subevent_code, s.subevent_label, s.entity_type, p.ranking_run_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 - `v_federation_scorecard` (dashboard 03, "Federation Scorecard")
# MAGIC Volume (ranked-player count) and strength (average/top points) kept as
# MAGIC two separate measures, never one dual-axis chart -- per the Blueprint's
# MAGIC own note that these tell different stories (India: breadth, China:
# MAGIC depth at the top).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_federation_scorecard AS
WITH latest_ind AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_individual} GROUP BY ranking_run_code
)
SELECT
    c.country_code,
    c.country_name,
    c.continent_name,
    f.ranking_run_code,
    f.subevent_code,
    COUNT(DISTINCT f.ittfid) AS ranked_player_count,
    ROUND(AVG(f.ranking_points_ytd), 1) AS avg_ranking_points,
    MAX(f.ranking_points_ytd) AS top_ranking_points
FROM {fact_individual} f
JOIN latest_ind l ON f.ranking_run_code = l.ranking_run_code
    AND (f.ranking_year * 100 + f.ranking_week) = l.max_week_key
LEFT JOIN {dim_country} c ON f.country_code = c.country_code
GROUP BY c.country_code, c.country_name, c.continent_name, f.ranking_run_code, f.subevent_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - `v_youth_pipeline` (dashboard 04, "Youth-to-Senior Pipeline")
# MAGIC Only rows where `is_junior_in_senior_file` -- players in the SEN file
# MAGIC who aren't themselves SEN-aged (the 41%-of-SEN-file-is-juniors finding).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_youth_pipeline AS
WITH latest_ind AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_individual} GROUP BY ranking_run_code
)
SELECT
    a.age_category_code,
    a.age_category_description,
    c.country_code,
    c.country_name,
    c.continent_name,
    COUNT(DISTINCT f.ittfid) AS junior_in_senior_file_count
FROM {fact_individual} f
JOIN latest_ind l ON f.ranking_run_code = l.ranking_run_code
    AND (f.ranking_year * 100 + f.ranking_week) = l.max_week_key
LEFT JOIN {dim_age_category} a ON f.age_category_code = a.age_category_code
LEFT JOIN {dim_country} c ON f.country_code = c.country_code
WHERE f.is_junior_in_senior_file = true
GROUP BY a.age_category_code, a.age_category_description, c.country_code, c.country_name, c.continent_name
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5 - `v_ranking_movers` (dashboard 05, "Ranking Movers")
# MAGIC Not pre-filtered to Men's Singles (the Blueprint mockup's example) --
# MAGIC every subevent/ranking_run_code combination is included, and the
# MAGIC dashboard filters by discipline, so this one view serves every
# MAGIC discipline slice instead of needing 8 near-identical views.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_ranking_movers AS
WITH latest_ind AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_individual} GROUP BY ranking_run_code
)
SELECT
    f.ittfid,
    p.player_name,
    p.country_code,
    p.continent_name,
    f.subevent_code,
    f.ranking_run_code,
    f.current_rank,
    f.previous_rank,
    f.ranking_difference
FROM {fact_individual} f
JOIN latest_ind l ON f.ranking_run_code = l.ranking_run_code
    AND (f.ranking_year * 100 + f.ranking_week) = l.max_week_key
LEFT JOIN {dim_player} p ON f.ittfid = p.ittfid
WHERE f.previous_rank IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6 - `v_doubles_partnerships` (dashboard 06, "Doubles Partnerships")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_doubles_partnerships AS
WITH latest_pair AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_pair} GROUP BY ranking_run_code
)
SELECT
    f.ittfid,
    d.player_name,
    d.partner1_country_code,
    d.partner2_country_code,
    d.is_cross_country,
    f.subevent_code,
    f.ranking_run_code,
    f.current_rank,
    f.points
FROM {fact_pair} f
JOIN latest_pair l ON f.ranking_run_code = l.ranking_run_code
    AND (f.ranking_year * 100 + f.ranking_week) = l.max_week_key
LEFT JOIN {dim_pair} d ON f.ittfid = d.ittfid
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7 - `v_fresh_faces` (dashboard 07, "Fresh Faces")
# MAGIC Unions individuals and pairs -- both carry `is_new_entrant` on their
# MAGIC fact table, so this is the one view in this notebook that reads from
# MAGIC both fact tables together.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_fresh_faces AS
WITH latest_ind AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_individual} GROUP BY ranking_run_code
),
latest_pair AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_pair} GROUP BY ranking_run_code
)
SELECT
    'INDIVIDUAL' AS entity_type,
    fi.ranking_run_code,
    fi.subevent_code,
    COUNT(DISTINCT fi.ittfid) AS total_count,
    COUNT(DISTINCT CASE WHEN fi.is_new_entrant THEN fi.ittfid END) AS new_entrant_count
FROM {fact_individual} fi
JOIN latest_ind l ON fi.ranking_run_code = l.ranking_run_code
    AND (fi.ranking_year * 100 + fi.ranking_week) = l.max_week_key
GROUP BY fi.ranking_run_code, fi.subevent_code

UNION ALL

SELECT
    'PAIR' AS entity_type,
    fp.ranking_run_code,
    fp.subevent_code,
    COUNT(DISTINCT fp.ittfid) AS total_count,
    COUNT(DISTINCT CASE WHEN fp.is_new_entrant THEN fp.ittfid END) AS new_entrant_count
FROM {fact_pair} fp
JOIN latest_pair l ON fp.ranking_run_code = l.ranking_run_code
    AND (fp.ranking_year * 100 + fp.ranking_week) = l.max_week_key
GROUP BY fp.ranking_run_code, fp.subevent_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8 - `v_ranking_leaders` (dashboard 08, "Top-of-the-Table Snapshot")
# MAGIC Already referenced by name in the Blueprint's Databricks capability
# MAGIC table (§04) -- top 10 by `current_rank` within each subevent x
# MAGIC ranking_run_code, via a window function (the one place in this Gold
# MAGIC layer that needs one, since it's a within-group top-N rather than a
# MAGIC straight aggregate).
# MAGIC
# MAGIC **Tiebreaker, deliberately not just `current_rank`.** Real WTT rankings
# MAGIC do produce tied `current_rank` values, and `ROW_NUMBER()` over a
# MAGIC non-unique order is only as deterministic as Spark's task-scheduling
# MAGIC order -- i.e. not deterministic at all, so the "Top 10" could silently
# MAGIC differ between runs at a rank-10/11 tie with no underlying data change.
# MAGIC `ranking_position` is the source system's own tie-broken ordinal for
# MAGIC this exact grain, so it's the natural secondary sort key; `ittfid` is a
# MAGIC final deterministic tiebreak in the (expected to be rare/never) case
# MAGIC `ranking_position` is itself null or tied.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_ranking_leaders AS
WITH latest_ind AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_individual} GROUP BY ranking_run_code
)
SELECT * FROM (
    SELECT
        f.ittfid,
        p.player_name,
        p.country_code,
        p.continent_name,
        f.subevent_code,
        f.ranking_run_code,
        f.current_rank,
        f.ranking_points_ytd,
        ROW_NUMBER() OVER (
            PARTITION BY f.subevent_code, f.ranking_run_code
            ORDER BY f.current_rank ASC, f.ranking_position ASC, f.ittfid ASC
        ) AS leaderboard_position
    FROM {fact_individual} f
    JOIN latest_ind l ON f.ranking_run_code = l.ranking_run_code
        AND (f.ranking_year * 100 + f.ranking_week) = l.max_week_key
    LEFT JOIN {dim_player} p ON f.ittfid = p.ittfid
    WHERE f.current_rank IS NOT NULL
)
WHERE leaderboard_position <= 10
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Phase 11B — Pairs Performance views (added 2026-10-07)
# MAGIC Five views backing the new **Pairs Performance** page of
# MAGIC `WTT Rankings — Executive Overview_V2` (Men's / Women's / Mixed Doubles)
# MAGIC and, from Phase 13, the matching REST endpoints. Same rules as views 1-8:
# MAGIC they read only gold facts/dims, and every "current" figure is scoped to the
# MAGIC latest loaded week per `ranking_run_code` (`latest_pair` / `latest_ind` CTE).
# MAGIC
# MAGIC | # | View | Grain | Feeds |
# MAGIC |---|---|---|---|
# MAGIC | 9 | `v_pair_profile` | pair x subevent x ranking_run_code | pair picker, ID card, header, pair ranking card, counters, longevity table, cross-federation pie |
# MAGIC | 10 | `v_pair_ranking_history` | pair x subevent x run x week | pair ranking-history line, points-history chart |
# MAGIC | 11 | `v_pair_partner_doubles_individual_history` | pair x partner x run x week | partner doubles-individual history (2 lines) |
# MAGIC | 12 | `v_pair_partner_doubles_individual` | pair x partner (1/2) x run | the two partner "doubles individual" ranking cards |
# MAGIC | 13 | `v_pair_leaderboard` | pair x subevent x run, latest week only | leaderboard table, biggest-movers bar |
# MAGIC
# MAGIC **Order matters:** 10-13 reference `v_pair_profile` (9) or each other, so
# MAGIC run these cells top to bottom (the job task already runs the whole notebook).
# MAGIC
# MAGIC **Pair identity.** `fact_ranking_pair.ittfid` is the pair's own ITTF id
# MAGIC (the source `DoublesId`); the two players come from `dim_pair.partner1_ittfid`
# MAGIC / `partner2_ittfid` and their names from `dim_player`. `dim_pair` is
# MAGIC de-duplicated on `ittfid` defensively so a duplicate identity row can never
# MAGIC fan out a pair into two rows. The partner "doubles individual" ranking is the
# MAGIC individual-credit subevent of the pair's subevent: MD -> MDI, WD -> WDI, XD -> XDI.
# MAGIC `dim_player` is de-duplicated the same way before the partner-name joins.
# MAGIC
# MAGIC **Movement sign (review finding, 2026-10-07).** The source's
# MAGIC `ranking_difference` is `current_rank - previous_rank` (positive = FELL —
# MAGIC confirmed on the live Executive Summary: 242 -> 593 shows 351), and silver
# MAGIC back-fills it with a *points* delta when the source value is missing. So
# MAGIC these views never use it for direction: they derive
# MAGIC **`places_gained = previous_rank - current_rank`** (positive = climbed,
# MAGIC NULL for a new entry) and every arrow, counter and "climbers" visual reads
# MAGIC that. `ranking_difference` is still passed through unchanged for lineage.
# MAGIC
# MAGIC **One row per pair per week.** If a week ever carried more than one fact
# MAGIC row for the same pair (category variants), the best-ranked row is kept
# MAGIC whole (`ROW_NUMBER() ... ORDER BY current_rank, ranking_position`) rather
# MAGIC than mixing columns from different rows.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9 - `v_pair_profile` (Pairs Performance — profile, picker, cards)
# MAGIC One row per pair x subevent x `ranking_run_code`, for every pair that has
# MAGIC ever held a non-null `current_rank`. Current-week columns are NULL when the
# MAGIC pair isn't ranked in the latest week (`is_currently_ranked = false`).
# MAGIC `sort_order` (1 = current #1, unranked pairs last by best rank) and
# MAGIC `display_label` (`<pair name> (<pair ittfid>)`) drive the dashboard's pair
# MAGIC drop-down; the dashboard parses the id back out of the trailing `(digits)`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_pair_profile AS
WITH latest_pair AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_pair} GROUP BY ranking_run_code
),
pw AS (
    SELECT
        ittfid, subevent_code, ranking_run_code, ranking_year, ranking_week, week_key,
        current_rank, points, previous_rank, ranking_difference,
        previous_rank - current_rank AS places_gained
    FROM (
        SELECT
            f.*,
            f.ranking_year * 100 + f.ranking_week AS week_key,
            ROW_NUMBER() OVER (
                PARTITION BY f.ittfid, f.subevent_code, f.ranking_run_code, f.ranking_year, f.ranking_week
                ORDER BY f.current_rank ASC, f.ranking_position ASC NULLS LAST, f.points DESC
            ) AS rn
        FROM {fact_pair} f
        WHERE f.subevent_code IN ('MD', 'WD', 'XD') AND f.current_rank IS NOT NULL
    ) t
    WHERE rn = 1
),
agg AS (
    SELECT
        ittfid, subevent_code, ranking_run_code,
        COUNT(*)                                           AS weeks_ranked,
        SUM(CASE WHEN current_rank <= 10 THEN 1 ELSE 0 END) AS weeks_in_top10,
        MIN(week_key)                                      AS first_ranked_key,
        MAX(week_key)                                      AS last_ranked_key,
        MIN(current_rank)                                  AS best_rank,
        MAX(points)                                        AS highest_points
    FROM pw
    GROUP BY ittfid, subevent_code, ranking_run_code
),
best AS (
    SELECT ittfid, subevent_code, ranking_run_code,
           ranking_year AS best_rank_year, ranking_week AS best_rank_week
    FROM (
        SELECT pw.*,
               ROW_NUMBER() OVER (
                   PARTITION BY ittfid, subevent_code, ranking_run_code
                   ORDER BY current_rank ASC, week_key ASC
               ) AS rn
        FROM pw
    ) t
    WHERE rn = 1
),
curr AS (
    SELECT pw.ittfid, pw.subevent_code, pw.ranking_run_code,
           pw.current_rank, pw.points, pw.previous_rank, pw.ranking_difference, pw.places_gained
    FROM pw
    JOIN latest_pair l
      ON pw.ranking_run_code = l.ranking_run_code AND pw.week_key = l.max_week_key
),
dp AS (
    SELECT * FROM (
        SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.ittfid ORDER BY d.subevent_code) AS rn
        FROM {dim_pair} d
    ) t WHERE rn = 1
),
dpl AS (
    SELECT ittfid, player_name FROM (
        SELECT pl.ittfid, pl.player_name,
               ROW_NUMBER() OVER (PARTITION BY pl.ittfid ORDER BY pl.player_name NULLS LAST) AS rn
        FROM {dim_player} pl
    ) t WHERE rn = 1
),
base AS (
    SELECT
        a.ittfid AS pair_ittfid,
        a.subevent_code,
        s.subevent_label AS subevent_name,
        CONCAT(COALESCE(s.subevent_label, a.subevent_code), ' (', a.subevent_code, ')') AS subevent_label,
        a.ranking_run_code,
        CASE
            WHEN p1.player_name IS NOT NULL AND p2.player_name IS NOT NULL
                THEN CONCAT(p1.player_name, ' / ', p2.player_name)
            ELSE COALESCE(dp.player_name, CONCAT('Pair ', a.ittfid))
        END AS pair_name,
        dp.partner1_ittfid,
        p1.player_name AS partner1_name,
        dp.partner1_country_code,
        dp.partner2_ittfid,
        p2.player_name AS partner2_name,
        dp.partner2_country_code,
        dp.is_cross_country,
        CAST(l.max_week_key DIV 100 AS INT) AS latest_year,
        CAST(l.max_week_key % 100 AS INT)   AS latest_week,
        c.current_rank,
        c.points AS current_points,
        c.previous_rank,
        c.ranking_difference,
        c.places_gained,
        c.current_rank IS NOT NULL AS is_currently_ranked,
        a.best_rank,
        b.best_rank_year,
        b.best_rank_week,
        a.highest_points,
        a.weeks_ranked,
        a.weeks_in_top10,
        CAST(a.first_ranked_key DIV 100 AS INT) AS first_ranked_year,
        CAST(a.first_ranked_key % 100 AS INT)   AS first_ranked_week,
        CAST(a.last_ranked_key DIV 100 AS INT)  AS last_ranked_year,
        CAST(a.last_ranked_key % 100 AS INT)    AS last_ranked_week
    FROM agg a
    JOIN latest_pair l ON a.ranking_run_code = l.ranking_run_code
    LEFT JOIN best b
      ON a.ittfid = b.ittfid AND a.subevent_code = b.subevent_code AND a.ranking_run_code = b.ranking_run_code
    LEFT JOIN curr c
      ON a.ittfid = c.ittfid AND a.subevent_code = c.subevent_code AND a.ranking_run_code = c.ranking_run_code
    LEFT JOIN dp ON a.ittfid = dp.ittfid
    LEFT JOIN dpl p1 ON dp.partner1_ittfid = p1.ittfid
    LEFT JOIN dpl p2 ON dp.partner2_ittfid = p2.ittfid
    LEFT JOIN {dim_subevent} s ON a.subevent_code = s.subevent_code
)
SELECT
    base.*,
    CONCAT(pair_name, ' (', pair_ittfid, ')') AS display_label,
    ROW_NUMBER() OVER (
        PARTITION BY subevent_code, ranking_run_code
        ORDER BY current_rank ASC NULLS LAST, best_rank ASC, pair_ittfid ASC
    ) AS sort_order
FROM base
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 10 - `v_pair_ranking_history` (Pairs Performance — ranking & points history)
# MAGIC Every ranked week for every pair, full history (2021 onward). Same
# MAGIC "Current" / "Last Ranked" annotation logic as the Player Performance trend
# MAGIC charts: the pair's last point is flagged `is_current_week` when it is the
# MAGIC latest loaded week, otherwise `is_last_ranked`. `period_date` (week start,
# MAGIC Jan-1 + 7 x (week-1)) gives native Lakeview line charts a temporal axis.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_pair_ranking_history AS
WITH latest_pair AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_pair} GROUP BY ranking_run_code
),
pw AS (
    SELECT
        ittfid, subevent_code, ranking_run_code, ranking_year, ranking_week, period_key,
        current_rank, points, previous_rank, ranking_difference,
        previous_rank - current_rank AS places_gained
    FROM (
        SELECT
            f.*,
            f.ranking_year * 100 + f.ranking_week AS period_key,
            ROW_NUMBER() OVER (
                PARTITION BY f.ittfid, f.subevent_code, f.ranking_run_code, f.ranking_year, f.ranking_week
                ORDER BY f.current_rank ASC, f.ranking_position ASC NULLS LAST, f.points DESC
            ) AS rn
        FROM {fact_pair} f
        WHERE f.subevent_code IN ('MD', 'WD', 'XD') AND f.current_rank IS NOT NULL
    ) t
    WHERE rn = 1
),
flagged AS (
    SELECT
        pw.*,
        l.max_week_key,
        pw.period_key = MAX(pw.period_key) OVER (
            PARTITION BY pw.ittfid, pw.subevent_code, pw.ranking_run_code
        ) AS is_pair_last_point
    FROM pw
    JOIN latest_pair l ON pw.ranking_run_code = l.ranking_run_code
)
SELECT
    ittfid AS pair_ittfid,
    subevent_code,
    ranking_run_code,
    ranking_year,
    ranking_week,
    period_key,
    CONCAT(CAST(ranking_year AS STRING), ' W', LPAD(CAST(ranking_week AS STRING), 2, '0')) AS period_label,
    DATE_ADD(MAKE_DATE(ranking_year, 1, 1), (ranking_week - 1) * 7) AS period_date,
    current_rank,
    points AS ranking_points,
    previous_rank,
    ranking_difference,
    places_gained,
    is_pair_last_point AND period_key = max_week_key  AS is_current_week,
    is_pair_last_point AND period_key <> max_week_key AS is_last_ranked,
    CASE
        WHEN is_pair_last_point AND period_key = max_week_key THEN
            CONCAT('Current — Rank #', CAST(current_rank AS STRING), ' | ',
                   CAST(ranking_year AS STRING), ' W', LPAD(CAST(ranking_week AS STRING), 2, '0'))
        WHEN is_pair_last_point AND period_key <> max_week_key THEN
            CONCAT('Last Ranked — Rank #', CAST(current_rank AS STRING), ' | ',
                   CAST(ranking_year AS STRING), ' W', LPAD(CAST(ranking_week AS STRING), 2, '0'))
    END AS current_annotation
FROM flagged
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 11 - `v_pair_partner_doubles_individual_history` (partner doubles-individual history)
# MAGIC For each pair, both partners' own individual-credit doubles ranking
# MAGIC (MD -> MDI, WD -> WDI, XD -> XDI) for every ranked week, same
# MAGIC `ranking_run_code`. Deliberately the partner's FULL history (not clipped to
# MAGIC the weeks this partnership existed) — that is what the individual-credit
# MAGIC ranking means, and it matches the Player Performance page's
# MAGIC "Doubles Individual" trend. `partner_label` (`P1 · NAME`) is the chart legend.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_pair_partner_doubles_individual_history AS
WITH partners AS (
    SELECT pair_ittfid, subevent_code, ranking_run_code,
           1 AS partner_no, partner1_ittfid AS partner_ittfid, partner1_name AS partner_name,
           partner1_country_code AS partner_country_code
    FROM {catalog_name}.{gold_schema}.v_pair_profile
    WHERE partner1_ittfid IS NOT NULL
    UNION ALL
    SELECT pair_ittfid, subevent_code, ranking_run_code,
           2 AS partner_no, partner2_ittfid, partner2_name, partner2_country_code
    FROM {catalog_name}.{gold_schema}.v_pair_profile
    WHERE partner2_ittfid IS NOT NULL
),
iw AS (
    SELECT
        ittfid, subevent_code, ranking_run_code, ranking_year, ranking_week, period_key,
        current_rank, ranking_points_ytd AS ranking_points
    FROM (
        SELECT
            f.*,
            f.ranking_year * 100 + f.ranking_week AS period_key,
            ROW_NUMBER() OVER (
                PARTITION BY f.ittfid, f.subevent_code, f.ranking_run_code, f.ranking_year, f.ranking_week
                ORDER BY f.current_rank ASC, f.ranking_position ASC NULLS LAST, f.ranking_points_ytd DESC
            ) AS rn
        FROM {fact_individual} f
        WHERE f.subevent_code IN ('MDI', 'WDI', 'XDI') AND f.current_rank IS NOT NULL
    ) t
    WHERE rn = 1
)
SELECT
    p.pair_ittfid,
    p.subevent_code,
    iw.subevent_code AS individual_subevent_code,
    p.ranking_run_code,
    p.partner_no,
    p.partner_ittfid,
    p.partner_name,
    p.partner_country_code,
    CONCAT('P', CAST(p.partner_no AS STRING), ' · ', COALESCE(p.partner_name, p.partner_ittfid)) AS partner_label,
    iw.ranking_year,
    iw.ranking_week,
    iw.period_key,
    CONCAT(CAST(iw.ranking_year AS STRING), ' W', LPAD(CAST(iw.ranking_week AS STRING), 2, '0')) AS period_label,
    DATE_ADD(MAKE_DATE(iw.ranking_year, 1, 1), (iw.ranking_week - 1) * 7) AS period_date,
    iw.current_rank,
    iw.ranking_points
FROM partners p
JOIN iw
  ON iw.ittfid = p.partner_ittfid
 AND iw.subevent_code = CONCAT(p.subevent_code, 'I')
 AND iw.ranking_run_code = p.ranking_run_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 12 - `v_pair_partner_doubles_individual` (partner doubles-individual cards)
# MAGIC One row per pair x partner (1/2) x run: the partner's individual-credit
# MAGIC doubles rank in the latest loaded week (NULL if not ranked that week) and
# MAGIC their best-ever rank with the week it was first reached. Partners with no
# MAGIC individual-credit history at all still get a row (all-NULL ranks) so the
# MAGIC dashboard card shows "NONE" rather than disappearing.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_pair_partner_doubles_individual AS
WITH latest_ind AS (
    SELECT ranking_run_code, MAX(ranking_year * 100 + ranking_week) AS max_week_key
    FROM {fact_individual} GROUP BY ranking_run_code
),
partners AS (
    SELECT pair_ittfid, subevent_code, ranking_run_code,
           1 AS partner_no, partner1_ittfid AS partner_ittfid, partner1_name AS partner_name,
           partner1_country_code AS partner_country_code
    FROM {catalog_name}.{gold_schema}.v_pair_profile
    WHERE partner1_ittfid IS NOT NULL
    UNION ALL
    SELECT pair_ittfid, subevent_code, ranking_run_code,
           2 AS partner_no, partner2_ittfid, partner2_name, partner2_country_code
    FROM {catalog_name}.{gold_schema}.v_pair_profile
    WHERE partner2_ittfid IS NOT NULL
),
h AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY pair_ittfid, subevent_code, ranking_run_code, partner_no
               ORDER BY current_rank ASC, period_key ASC
           ) AS best_rn
    FROM {catalog_name}.{gold_schema}.v_pair_partner_doubles_individual_history
),
best AS (
    SELECT pair_ittfid, subevent_code, ranking_run_code, partner_no,
           current_rank AS best_rank, ranking_year AS best_rank_year, ranking_week AS best_rank_week
    FROM h WHERE best_rn = 1
),
curr AS (
    SELECT h.pair_ittfid, h.subevent_code, h.ranking_run_code, h.partner_no,
           h.current_rank, h.ranking_points AS current_points
    FROM h
    JOIN latest_ind l ON h.ranking_run_code = l.ranking_run_code AND h.period_key = l.max_week_key
)
SELECT
    p.pair_ittfid,
    p.subevent_code,
    CONCAT(p.subevent_code, 'I') AS individual_subevent_code,
    s.subevent_label AS individual_subevent_name,
    p.ranking_run_code,
    p.partner_no,
    p.partner_ittfid,
    p.partner_name,
    p.partner_country_code,
    CAST(l.max_week_key DIV 100 AS INT) AS latest_year,
    CAST(l.max_week_key % 100 AS INT)   AS latest_week,
    c.current_rank,
    c.current_points,
    b.best_rank,
    b.best_rank_year,
    b.best_rank_week
FROM partners p
LEFT JOIN latest_ind l ON p.ranking_run_code = l.ranking_run_code
LEFT JOIN curr c
  ON p.pair_ittfid = c.pair_ittfid AND p.subevent_code = c.subevent_code
 AND p.ranking_run_code = c.ranking_run_code AND p.partner_no = c.partner_no
LEFT JOIN best b
  ON p.pair_ittfid = b.pair_ittfid AND p.subevent_code = b.subevent_code
 AND p.ranking_run_code = b.ranking_run_code AND p.partner_no = b.partner_no
LEFT JOIN {dim_subevent} s ON s.subevent_code = CONCAT(p.subevent_code, 'I')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 13 - `v_pair_leaderboard` (Pairs Performance — leaderboard, movers, federation mix)
# MAGIC Only pairs ranked in the latest loaded week. `movement_label` renders
# MAGIC `places_gained` (positive = climbed) as ▲/▼/—, with `NEW` when there is
# MAGIC no previous rank; `movement_dir` (UP/DOWN/SAME/NEW) is the same thing as a
# MAGIC plain code for table colour rules and API consumers.
# MAGIC `partnership_type` buckets `is_cross_country` for the federation-mix pie.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_pair_leaderboard AS
SELECT
    pair_ittfid,
    subevent_code,
    subevent_label,
    ranking_run_code,
    latest_year,
    latest_week,
    current_rank,
    pair_name,
    partner1_ittfid,
    partner1_name,
    partner1_country_code,
    partner2_ittfid,
    partner2_name,
    partner2_country_code,
    CONCAT_WS(' / ', partner1_country_code, partner2_country_code) AS federations,
    current_points,
    previous_rank,
    ranking_difference,
    places_gained,
    CASE
        WHEN previous_rank IS NULL THEN 'NEW'
        WHEN places_gained > 0 THEN CONCAT('▲ ', CAST(places_gained AS STRING))
        WHEN places_gained < 0 THEN CONCAT('▼ ', CAST(ABS(places_gained) AS STRING))
        ELSE '—'
    END AS movement_label,
    CASE
        WHEN previous_rank IS NULL THEN 'NEW'
        WHEN places_gained > 0 THEN 'UP'
        WHEN places_gained < 0 THEN 'DOWN'
        ELSE 'SAME'
    END AS movement_dir,
    CASE
        WHEN is_cross_country IS NULL THEN 'Unknown'
        WHEN is_cross_country THEN 'Cross-federation'
        ELSE 'Same federation'
    END AS partnership_type,
    weeks_ranked,
    weeks_in_top10,
    best_rank
FROM {catalog_name}.{gold_schema}.v_pair_profile
WHERE is_currently_ranked
""")

# COMMAND ----------

display(spark.sql(f"SHOW VIEWS IN {catalog_name}.{gold_schema}"))