# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Longitudinal Views
# MAGIC The 9 new dashboard concepts proposed by the 2026-09-13 business-analyst
# MAGIC review (see the discovery doc's "Dashboard catalog" section, items
# MAGIC 9-17), built to specifically exploit the ~6 years of weekly history
# MAGIC (2021 wk1 -> the latest loaded week) that Step 2b unlocked in the gold
# MAGIC fact tables.
# MAGIC
# MAGIC **Deliberately the mirror image of `09.Gold Dashboard Views`**: those 8
# MAGIC views each join a `latest_ind`/`latest_pair` CTE to scope down to the
# MAGIC single latest ranking week per `ranking_run_code` (see that notebook's
# MAGIC own bugfix note). Every view below does the opposite on purpose --
# MAGIC it reads `{fact_individual}`/`{fact_pair}` **unscoped**, across every
# MAGIC week/year on record, because a trend/trajectory view is only useful
# MAGIC with the full history in view. Do not add a latest-week CTE to any view
# MAGIC in this notebook; that would silently turn a longitudinal view back
# MAGIC into a snapshot.
# MAGIC
# MAGIC Views created (`CREATE OR REPLACE VIEW`, safe to re-run any time the
# MAGIC facts/dims change), in the recommended build order from the discovery
# MAGIC doc's Next Steps guide (highest business value / lowest complexity
# MAGIC first) -- dashboard numbers match the "Dashboard catalog" section:
# MAGIC 1. `gold.v_federation_strength_trajectory` -- dashboard 10
# MAGIC 2. `gold.v_continental_power_shift` -- dashboard 17
# MAGIC 3. `gold.v_player_career_trajectory` -- dashboard 9
# MAGIC 4. `gold.v_peak_rank_career_longevity` -- dashboard 11
# MAGIC 5. `gold.v_country_discipline_investment_trend` -- dashboard 13
# MAGIC 6. `gold.v_junior_to_senior_transition` -- dashboard 12
# MAGIC 7. `gold.v_new_entrant_retention_curve` -- dashboard 15
# MAGIC 8. `gold.v_doubles_partnership_longevity` -- dashboard 16
# MAGIC 9. `gold.v_volatility_consistency_index` -- dashboard 14
# MAGIC
# MAGIC Depends on `01`-`08` (every gold dim and fact) having run first in this
# MAGIC same job -- see `03-jobs/`. All 9 view names are added to
# MAGIC `ALL_GOLD_VIEWS` in `10.Validate All Gold Tables.py` so each gets the
# MAGIC same row-count sanity check as the original 8.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

fact_individual = f"{catalog_name}.{gold_schema}.fact_ranking_individual"
fact_pair = f"{catalog_name}.{gold_schema}.fact_ranking_pair"
dim_country = f"{catalog_name}.{gold_schema}.dim_country"
dim_player = f"{catalog_name}.{gold_schema}.dim_player"
dim_pair = f"{catalog_name}.{gold_schema}.dim_pair"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 - `v_federation_strength_trajectory` (dashboard 10, "Federation Strength Trajectory")
# MAGIC The multi-year version of `v_federation_scorecard` -- same volume
# MAGIC (ranked-player count) vs. strength (avg/top points) split, kept as
# MAGIC separate measures for the same reason as the original, now broken out
# MAGIC by `ranking_year` instead of collapsed to the latest week.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_federation_strength_trajectory AS
SELECT
    c.country_code,
    c.country_name,
    c.continent_name,
    f.ranking_run_code,
    f.subevent_code,
    f.ranking_year,
    COUNT(DISTINCT f.ittfid) AS ranked_player_count,
    ROUND(AVG(f.ranking_points_ytd), 1) AS avg_ranking_points,
    MAX(f.ranking_points_ytd) AS top_ranking_points
FROM {fact_individual} f
LEFT JOIN {dim_country} c ON f.country_code = c.country_code
GROUP BY c.country_code, c.country_name, c.continent_name, f.ranking_run_code, f.subevent_code, f.ranking_year
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - `v_continental_power_shift` (dashboard 17, "Continental Power Shift")
# MAGIC Long-run share of top-100 ranking slots held by each continent, by
# MAGIC year. `pct_of_top100` is computed with a window function over the
# MAGIC already-aggregated counts (partitioned by the same
# MAGIC subevent/run/year grain), not a second pass over the fact table.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_continental_power_shift AS
SELECT
    continent_name,
    continent_code,
    subevent_code,
    ranking_run_code,
    ranking_year,
    top100_count,
    ROUND(
        top100_count / SUM(top100_count) OVER (PARTITION BY subevent_code, ranking_run_code, ranking_year) * 100,
        1
    ) AS pct_of_top100
FROM (
    SELECT
        c.continent_name,
        c.continent_code,
        f.subevent_code,
        f.ranking_run_code,
        f.ranking_year,
        COUNT(DISTINCT CASE WHEN f.current_rank <= 100 THEN f.ittfid END) AS top100_count
    FROM {fact_individual} f
    LEFT JOIN {dim_country} c ON f.country_code = c.country_code
    WHERE f.current_rank IS NOT NULL
    GROUP BY c.continent_name, c.continent_code, f.subevent_code, f.ranking_run_code, f.ranking_year
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 - `v_player_career_trajectory` (dashboard 9, "Player Career Trajectory")
# MAGIC Row-level, not pre-aggregated -- a dashboard filters this to one
# MAGIC `ittfid` and plots `current_rank`/`ranking_points_ytd` across every
# MAGIC week on record. No new joins beyond `dim_player`, same pattern as
# MAGIC `v_ranking_movers` in `09` minus the latest-week CTE.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_player_career_trajectory AS
SELECT
    f.ittfid,
    p.player_name,
    p.country_code,
    p.continent_name,
    f.subevent_code,
    f.ranking_run_code,
    f.ranking_year,
    f.ranking_week,
    f.current_rank,
    f.ranking_points_ytd,
    f.ranking_difference
FROM {fact_individual} f
LEFT JOIN {dim_player} p ON f.ittfid = p.ittfid
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - `v_peak_rank_career_longevity` (dashboard 11, "Peak Rank & Career Longevity Leaderboard")
# MAGIC One row per player x subevent x ranking_run_code, aggregated across
# MAGIC their entire history: best rank ever reached, and how many weeks they
# MAGIC spent inside the top 10 / top 50 -- durable stars vs. one-week flashes.
# MAGIC `current_rank IS NOT NULL` filter matches the same convention
# MAGIC `v_ranking_leaders` (in `09`) already uses.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_peak_rank_career_longevity AS
SELECT
    f.ittfid,
    p.player_name,
    p.country_code,
    p.continent_name,
    f.subevent_code,
    f.ranking_run_code,
    MIN(f.current_rank) AS best_rank_ever,
    COUNT(CASE WHEN f.current_rank <= 10 THEN 1 END) AS weeks_in_top10,
    COUNT(CASE WHEN f.current_rank <= 50 THEN 1 END) AS weeks_in_top50,
    COUNT(*) AS total_weeks_ranked,
    MIN(f.ranking_year) AS first_year_ranked,
    MAX(f.ranking_year) AS last_year_ranked
FROM {fact_individual} f
LEFT JOIN {dim_player} p ON f.ittfid = p.ittfid
WHERE f.current_rank IS NOT NULL
GROUP BY f.ittfid, p.player_name, p.country_code, p.continent_name, f.subevent_code, f.ranking_run_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5 - `v_country_discipline_investment_trend` (dashboard 13, "Country x Discipline Investment Trend")
# MAGIC Same shape as `v_federation_strength_trajectory` above, one extra
# MAGIC grouping dimension already present on the fact row
# MAGIC (`subevent_code`) -- shows whether a federation is shifting emphasis
# MAGIC toward/away from doubles-credit disciplines over time.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_country_discipline_investment_trend AS
SELECT
    c.country_code,
    c.country_name,
    c.continent_name,
    f.subevent_code,
    f.ranking_run_code,
    f.ranking_year,
    COUNT(DISTINCT f.ittfid) AS ranked_player_count,
    ROUND(AVG(f.ranking_points_ytd), 1) AS avg_ranking_points
FROM {fact_individual} f
LEFT JOIN {dim_country} c ON f.country_code = c.country_code
GROUP BY c.country_code, c.country_name, c.continent_name, f.subevent_code, f.ranking_run_code, f.ranking_year
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6 - `v_junior_to_senior_transition` (dashboard 12, "Junior-to-Senior Transition Cohort Tracker")
# MAGIC Restricts to players who were EVER flagged `is_junior_in_senior_file`
# MAGIC at least once (the cohort), then shows their rank/points by year --
# MAGIC including years after they age out of junior status, which is the
# MAGIC point: the multi-year progression `v_youth_pipeline` (in `09`) can't
# MAGIC show as a single-week snapshot. `was_junior_in_senior_file` is a
# MAGIC per-year flag (true if the player was still a flagged junior in that
# MAGIC particular year), not a one-time cohort label.
# MAGIC
# MAGIC **Grouped by `subevent_code` as well as year** (caught by independent
# MAGIC review, 2026-09-13) -- a player's `current_rank` is only comparable
# MAGIC within the same discipline, so collapsing MS/MDI/XDI rows together into
# MAGIC one `MIN(current_rank)` per year would have blended incomparable
# MAGIC rankings. Matches the grain every other trend view in this notebook
# MAGIC already uses.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_junior_to_senior_transition AS
SELECT
    f.ittfid,
    p.player_name,
    p.country_code,
    p.continent_name,
    f.subevent_code,
    f.ranking_year,
    f.ranking_run_code,
    MAX(CASE WHEN f.is_junior_in_senior_file THEN 1 ELSE 0 END) = 1 AS was_junior_in_senior_file,
    MIN(f.current_rank) AS best_rank_in_year,
    ROUND(AVG(f.ranking_points_ytd), 1) AS avg_points_in_year
FROM {fact_individual} f
LEFT JOIN {dim_player} p ON f.ittfid = p.ittfid
WHERE f.ittfid IN (SELECT DISTINCT ittfid FROM {fact_individual} WHERE is_junior_in_senior_file = true)
GROUP BY f.ittfid, p.player_name, p.country_code, p.continent_name, f.subevent_code, f.ranking_year, f.ranking_run_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7 - `v_new_entrant_retention_curve` (dashboard 15, "New Entrant Cohort Retention / Survival Curve")
# MAGIC The one view in this notebook needing real CTE machinery, per the
# MAGIC discovery doc's build-order note. Cohorts by `cohort_year` = each
# MAGIC entity's own first year on record (`MIN(ranking_year)` per `ittfid`) --
# MAGIC deliberately NOT the `is_new_entrant` flag, since that flags a
# MAGIC player "new" on every row where `previous_rank IS NULL`, which can
# MAGIC recur after a gap and would double-count re-entries as fresh cohort
# MAGIC members. `retention_pct` = what share of a cohort is still active
# MAGIC (appears at least once) in each later year. Individuals and pairs are
# MAGIC computed identically but kept as separate CTE chains (different fact
# MAGIC tables), then unioned only at the final `SELECT`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_new_entrant_retention_curve AS
WITH ind_first_year AS (
    SELECT ittfid, MIN(ranking_year) AS cohort_year FROM {fact_individual} GROUP BY ittfid
),
ind_cohort_size AS (
    SELECT cohort_year, COUNT(DISTINCT ittfid) AS cohort_total FROM ind_first_year GROUP BY cohort_year
),
ind_active_by_year AS (
    SELECT fy.cohort_year, f.ranking_year, COUNT(DISTINCT f.ittfid) AS active_count
    FROM {fact_individual} f
    JOIN ind_first_year fy ON f.ittfid = fy.ittfid
    GROUP BY fy.cohort_year, f.ranking_year
),
pair_first_year AS (
    SELECT ittfid, MIN(ranking_year) AS cohort_year FROM {fact_pair} GROUP BY ittfid
),
pair_cohort_size AS (
    SELECT cohort_year, COUNT(DISTINCT ittfid) AS cohort_total FROM pair_first_year GROUP BY cohort_year
),
pair_active_by_year AS (
    SELECT fy.cohort_year, f.ranking_year, COUNT(DISTINCT f.ittfid) AS active_count
    FROM {fact_pair} f
    JOIN pair_first_year fy ON f.ittfid = fy.ittfid
    GROUP BY fy.cohort_year, f.ranking_year
)
SELECT
    'INDIVIDUAL' AS entity_type,
    a.cohort_year,
    a.ranking_year,
    (a.ranking_year - a.cohort_year) AS years_since_entry,
    a.active_count,
    cs.cohort_total,
    ROUND(a.active_count / cs.cohort_total * 100, 1) AS retention_pct
FROM ind_active_by_year a
JOIN ind_cohort_size cs ON a.cohort_year = cs.cohort_year
WHERE a.ranking_year >= a.cohort_year

UNION ALL

SELECT
    'PAIR' AS entity_type,
    a.cohort_year,
    a.ranking_year,
    (a.ranking_year - a.cohort_year) AS years_since_entry,
    a.active_count,
    cs.cohort_total,
    ROUND(a.active_count / cs.cohort_total * 100, 1) AS retention_pct
FROM pair_active_by_year a
JOIN pair_cohort_size cs ON a.cohort_year = cs.cohort_year
WHERE a.ranking_year >= a.cohort_year
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8 - `v_doubles_partnership_longevity` (dashboard 16, "Doubles Partnership Longevity & Churn")
# MAGIC How long a pair `ittfid` persists, in distinct ranking periods, and
# MAGIC whether that longevity tracks with better results. `partner1`/`partner2`
# MAGIC columns come from `dim_pair` (see that notebook's header -- there is no
# MAGIC single "the pair's country" column, only per-partner ones).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_doubles_partnership_longevity AS
SELECT
    fp.ittfid,
    d.player_name,
    d.partner1_country_code,
    d.partner2_country_code,
    d.is_cross_country,
    fp.subevent_code,
    MIN(fp.ranking_year * 100 + fp.ranking_week) AS first_period_key,
    MAX(fp.ranking_year * 100 + fp.ranking_week) AS last_period_key,
    COUNT(DISTINCT fp.ranking_year * 100 + fp.ranking_week) AS distinct_periods_active,
    MIN(fp.current_rank) AS best_rank_ever,
    ROUND(AVG(fp.points), 1) AS avg_points
FROM {fact_pair} fp
LEFT JOIN {dim_pair} d ON fp.ittfid = d.ittfid
GROUP BY fp.ittfid, d.player_name, d.partner1_country_code, d.partner2_country_code, d.is_cross_country, fp.subevent_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9 - `v_volatility_consistency_index` (dashboard 14, "Volatility & Consistency Index")
# MAGIC `STDDEV()` of `ranking_difference` over full history, per entity --
# MAGIC steady vs. volatile performers. `HAVING COUNT(*) >= 8` on both arms
# MAGIC excludes entities with too few observed weeks for a meaningful
# MAGIC standard deviation (an entity with 1-2 data points would otherwise
# MAGIC show a `NULL`/near-zero stddev that reads as "perfectly consistent"
# MAGIC rather than "not enough data") -- 8 is a deliberately conservative
# MAGIC floor, easy to loosen once real usage shows what's useful.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_volatility_consistency_index AS
SELECT
    'INDIVIDUAL' AS entity_type,
    f.ittfid,
    p.player_name,
    f.subevent_code,
    f.ranking_run_code,
    ROUND(STDDEV(f.ranking_difference), 1) AS ranking_difference_stddev,
    COUNT(*) AS weeks_observed
FROM {fact_individual} f
LEFT JOIN {dim_player} p ON f.ittfid = p.ittfid
WHERE f.ranking_difference IS NOT NULL
GROUP BY f.ittfid, p.player_name, f.subevent_code, f.ranking_run_code
HAVING COUNT(*) >= 8

UNION ALL

SELECT
    'PAIR' AS entity_type,
    fp.ittfid,
    d.player_name,
    fp.subevent_code,
    fp.ranking_run_code,
    ROUND(STDDEV(fp.ranking_difference), 1) AS ranking_difference_stddev,
    COUNT(*) AS weeks_observed
FROM {fact_pair} fp
LEFT JOIN {dim_pair} d ON fp.ittfid = d.ittfid
WHERE fp.ranking_difference IS NOT NULL
GROUP BY fp.ittfid, d.player_name, fp.subevent_code, fp.ranking_run_code
HAVING COUNT(*) >= 8
""")

# COMMAND ----------

display(spark.sql(f"SHOW VIEWS IN {catalog_name}.{gold_schema}"))