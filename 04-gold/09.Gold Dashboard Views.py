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
SELECT
    c.continent_name,
    c.continent_code,
    f.ranking_run_code,
    COUNT(DISTINCT f.ittfid) AS ranked_individuals
FROM {fact_individual} f
LEFT JOIN {dim_country} c ON f.country_code = c.country_code
GROUP BY c.continent_name, c.continent_code, f.ranking_run_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - `v_discipline_landscape` (dashboard 02, "Discipline Landscape")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_discipline_landscape AS
SELECT
    s.subevent_code,
    s.subevent_label,
    s.entity_type,
    f.ranking_run_code,
    COUNT(DISTINCT f.ittfid) AS ranked_count
FROM {fact_individual} f
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
SELECT
    a.age_category_code,
    a.age_category_description,
    c.country_code,
    c.country_name,
    c.continent_name,
    COUNT(DISTINCT f.ittfid) AS junior_in_senior_file_count
FROM {fact_individual} f
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
LEFT JOIN {dim_player} p ON f.ittfid = p.ittfid
WHERE f.previous_rank IS NOT NULL
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6 - `v_doubles_partnerships` (dashboard 06, "Doubles Partnerships")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {catalog_name}.{gold_schema}.v_doubles_partnerships AS
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
SELECT
    'INDIVIDUAL' AS entity_type,
    ranking_run_code,
    subevent_code,
    COUNT(DISTINCT ittfid) AS total_count,
    COUNT(DISTINCT CASE WHEN is_new_entrant THEN ittfid END) AS new_entrant_count
FROM {fact_individual}
GROUP BY ranking_run_code, subevent_code

UNION ALL

SELECT
    'PAIR' AS entity_type,
    ranking_run_code,
    subevent_code,
    COUNT(DISTINCT ittfid) AS total_count,
    COUNT(DISTINCT CASE WHEN is_new_entrant THEN ittfid END) AS new_entrant_count
FROM {fact_pair}
GROUP BY ranking_run_code, subevent_code
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
    LEFT JOIN {dim_player} p ON f.ittfid = p.ittfid
    WHERE f.current_rank IS NOT NULL
)
WHERE leaderboard_position <= 10
""")

# COMMAND ----------

# DBTITLE 1,Show all gold views
spark.sql(f"USE CATALOG {catalog_name}")
display(spark.sql(f"SHOW VIEWS IN {gold_schema}"))