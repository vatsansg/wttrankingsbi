# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Events Views — Phase 12
# MAGIC Gold views for the **Events** domain. Each backs one or more tiles of the V3 dashboard and is also an API endpoint
# MAGIC in Phase 13. Every view reads only:
# MAGIC - **`silver.player_event_results`**, the single consolidated results table (no other results source exists), and
# MAGIC - **`silver.events`** (event level: name, dates, rounds, sanctioned from `dbo.Events`; tier and organisation from
# MAGIC   `dbo.EventsMetadata`) and **`silver.events_metadata`** (row level: draw type, ranking category description, SEN/YOU
# MAGIC   category), for **every** event attribute;
# MAGIC - **`silver.player_identity`**, looked up **at query time** for names and the unmapped buckets, so a competitor added
# MAGIC   to `Competitors` / `Players_Doubles` later leaves its `ITTFID_<category>` bucket on the next refresh, with no
# MAGIC   rebuild of the results table.
# MAGIC
# MAGIC **Event sources: `dbo.Events` and `dbo.EventsMetadata` only.** No event attribute is taken from anywhere else: not
# MAGIC from the results rows (their `OrganizationCode` / `CategoryCode` / expiry year are never used as event attributes),
# MAGIC not from `EventTypeGeneral` or other reference tables, and not from any hand-maintained mapping. A result whose event
# MAGIC is in neither table still counts in player-level views, but shows `Event #<id>` and a NULL event date/year (so it
# MAGIC sits outside year-based tiles); without `EventsMetadata` rows its tier and organisation show `Not in EventsMetadata`.
# MAGIC The KPI `metadata_coverage_pct` is the share of results whose event has `EventsMetadata` rows; it rises to ~100%
# MAGIC after the Ranking team's full reload.
# MAGIC
# MAGIC Competitor names come from `silver.player_identity` (query time). Current ranks (view 13 only) come from the
# MAGIC existing gold rank facts.
# MAGIC
# MAGIC **Three building blocks (views 1–3), which the rest read:**
# MAGIC 1. `v_evt_event_dim`: one row per event (`silver.events`, plus category lists collapsed from `silver.events_metadata`).
# MAGIC 2. `v_evt_result_detail`: one row per **actual result** (`is_first_appearance` in silver), joined to the metadata on
# MAGIC    the exact key `(event_id, ranking_category_code, age_category_code)`. Event-level attributes (name, dates, tier,
# MAGIC    organisation) come from `v_evt_event_dim` on `event_id` alone. It has no window functions, because silver already carries:
# MAGIC    - `ranking_scope` (`SEN`/`YOU`, from the source `CategoryCode`), which says which ranking the points count toward;
# MAGIC    - `is_result_row`, the counting flag: one row per player × event × subevent × ranking category over all history
# MAGIC      (the YOU copy of a youth result, the deepest stage);
# MAGIC    - `entry_type`.
# MAGIC 3. `v_evt_latest_week`: the latest loaded week in the results table.
# MAGIC
# MAGIC **Unmapped competitors.** A competitor whose ID silver could not resolve appears everywhere as one bucket per ranking
# MAGIC category, `ITTFID_<category>` (e.g. `ITTFID_MS`, `ITTFID_XD`), via `competitor_key` / `competitor_name`. View 18 lists
# MAGIC the IDs behind each bucket.
# MAGIC
# MAGIC **Counting rule used throughout:** "how many results / events / players" views filter `is_result_row`, so one
# MAGIC performance counts once. Points views keep both scopes and expose `ranking_scope` as a column, because SEN and youth
# MAGIC points are different numbers for the same result.
# MAGIC
# MAGIC | # | View | Blueprint visual | Grain |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `v_evt_event_dim` | (base; Events API) | event |
# MAGIC | 2 | `v_evt_result_detail` | (base) | result × ranking scope |
# MAGIC | 3 | `v_evt_latest_week` | (base) | 1 row |
# MAGIC | 4 | `v_evt_calendar_density` | 01 Event Calendar Density | quarter × category × tier × organisation |
# MAGIC | 5 | `v_evt_tier_mix_trend` | 02 Event Tier Mix Over Time | year × organisation × category |
# MAGIC | 6 | `v_evt_player_timeline` | 03 Player Career Event Timeline | result × scope (+ live-now flags) |
# MAGIC | 7 | `v_evt_best_results_current` | 04 Best-Results Composition | latest-week result |
# MAGIC | 8 | `v_evt_points_per_tier` | 05 Points-per-Tier Efficiency | tier × scope × ranking category |
# MAGIC | 9 | `v_evt_stage_distribution` | 06 Deepest-Run Distribution | year × tier × stage group |
# MAGIC | 10 | `v_evt_qualifier_performance` | 07 Qualifier vs Direct Entry | year × entry type |
# MAGIC | 11 | `v_evt_organization_share` | 08 WTT vs ITTF vs other share | year × `OrganizationCode` |
# MAGIC | 12 | `v_evt_zpp_tracker` | 09 Zero-Point-Penalty Tracker | year × tier |
# MAGIC | 13 | `v_evt_mandatory_compliance` | 10 Mandatory-Event Compliance (adapted, see below) | latest-week mandatory result |
# MAGIC | 14 | `v_evt_first_senior_event` | 11 First Senior Event Tracker | player |
# MAGIC | 15 | `v_evt_win_rate_leaderboard` | 12 Event Win-Rate Leaderboard | competitor × ranking category |
# MAGIC | 16 | `v_evt_event_summary` | Events list / "this week's events" | event |
# MAGIC | 17 | `v_evt_kpis` | KPI counters | 1 row |
# MAGIC | 18 | `v_evt_unmapped_competitors` | Unmapped-competitor drill-down | ranking category × ITTF ID |
# MAGIC
# MAGIC **Visual 10, adapted.** The blueprint wanted "top players who *skipped* a mandatory event". That needs an event-level
# MAGIC mandatory flag, and `EventsMetadata` doesn't carry one. The results carry a per-result
# MAGIC `MandatoryInclusionforBestResults` flag instead. So this view lists the current top players' **mandatory-inclusion
# MAGIC results this week**, and whether each one is counted, rather than an anti-join of skipped events. If the Ranking
# MAGIC team adds a mandatory flag to `EventsMetadata` later, the anti-join becomes a one-view change.
# MAGIC
# MAGIC **Sparse source columns.** In the sample exports `MatchesPlayed`/`MatchesWon` are filled on about 1% of rows, and
# MAGIC `ZeroPointPenalty` / `MandatoryInclusionforBestResults` are NULL everywhere. The views handle this:
# MAGIC - win rates are NULL, not 0, when no matches are recorded;
# MAGIC - a ZPP is also inferred from a `ZPP` result position;
# MAGIC - the Visual 07 and 12 tiles also show stage-based measures (share reaching the QF, titles, finals), which work
# MAGIC   without match data.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

per = f"{catalog_name}.{silver_schema}.player_event_results"
em = f"{catalog_name}.{silver_schema}.events_metadata"   # row level: event_id + ranking_category_code + age_category_code
ev = f"{catalog_name}.{silver_schema}.events"            # event level: one row per event_id
pid = f"{catalog_name}.{silver_schema}.player_identity"  # identity, looked up at query time (not the flag stamped in silver 08)
fact_individual = f"{catalog_name}.{gold_schema}.fact_ranking_individual"
fact_pair = f"{catalog_name}.{gold_schema}.fact_ranking_pair"
G = f"{catalog_name}.{gold_schema}"
NOT_IN_EM = 'Not in EventsMetadata'   # label for results whose event isn't in EventsMetadata yet

# Unmapped competitors (CompetitorId not found in Competitors / Players_Doubles) appear on every tile as ONE bucket per
# ranking category: ITTFID_MS, ITTFID_WS, ITTFID_MDI, ITTFID_WDI, ITTFID_XDI (individuals), ITTFID_MD, ITTFID_WD,
# ITTFID_XD (pairs). v_evt_unmapped_competitors lists the IDs inside each bucket (dashboard drill-down).
# `a` = results alias, `i` = alias of the LEFT JOIN to silver.player_identity on (ittfid, entity_type).
def competitor_key_sql(a, i):
    return f"CASE WHEN {i}.ittfid IS NOT NULL THEN {a}.ittfid ELSE CONCAT('ITTFID_', {a}.ranking_category_code) END"
def competitor_name_sql(a, i):
    return (f"CASE WHEN {i}.ittfid IS NOT NULL THEN COALESCE({i}.player_name, CONCAT('ITTF ', {a}.ittfid)) "
            f"ELSE CONCAT('ITTFID_', {a}.ranking_category_code) END")
def identity_join_sql(a, i):
    # grouped so a duplicated identity row can never fan out the results
    return (f"LEFT JOIN (SELECT ittfid, entity_type, MAX(player_name) AS player_name, MAX(country_code) AS country_code "
            f"FROM {pid} GROUP BY ittfid, entity_type) {i} ON {i}.ittfid = {a}.ittfid AND {i}.entity_type = {a}.entity_type")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 - `v_evt_event_dim` (one row per event)
# MAGIC Event-level columns from `silver.events`; category lists, `primary_category` and `is_senior_event` from the
# MAGIC row-level `silver.events_metadata`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_event_dim AS
WITH cat AS (   -- row-level EventsMetadata collapsed per event
    SELECT
        event_id,
        CONCAT_WS('/', SORT_ARRAY(COLLECT_SET(event_category_code)))    AS event_categories,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(ranking_category_code))) AS ranking_categories,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(age_category_code)))     AS age_categories,
        MAX(CASE WHEN event_category_code = 'SEN' THEN 1 ELSE 0 END) = 1 AS has_senior_category,
        MAX(CASE WHEN event_category_code = 'YOU' THEN 1 ELSE 0 END) = 1 AS has_youth_category,
        CASE WHEN MAX(CASE WHEN event_category_code = 'YOU' THEN 1 ELSE 0 END) = 0 THEN 'SEN'
             WHEN MAX(CASE WHEN event_category_code = 'SEN' AND age_category_code = 'SEN' THEN 1 ELSE 0 END) = 0 THEN 'YOU'
             ELSE 'SEN+YOU' END                                         AS primary_category,
        -- senior event = every EventsMetadata row of this event is SEN (EventCategoryCodeforRankingCalculations)
        MAX(CASE WHEN event_category_code = 'SEN' THEN 0 ELSE 1 END) = 0 AS is_senior_event
    FROM {em}
    GROUP BY event_id
)
SELECT
    e.event_id,
    e.event_name,
    e.start_date,
    e.end_date,
    e.event_year,
    e.event_quarter_start,
    e.event_quarter_label,
    e.event_ranking_year,
    e.event_ranking_month,
    e.event_ranking_week,
    e.event_type_general_code,
    e.tier,
    e.organization_code,
    e.rounds,
    e.sanctioned,
    e.in_events,
    e.in_events_metadata,
    c.event_categories,
    c.ranking_categories,
    c.age_categories,
    COALESCE(c.has_senior_category, FALSE) AS has_senior_category,
    COALESCE(c.has_youth_category, FALSE)  AS has_youth_category,
    c.primary_category,
    COALESCE(c.is_senior_event, FALSE)     AS is_senior_event
FROM {ev} e
LEFT JOIN cat c ON c.event_id = e.event_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2 - `v_evt_result_detail` (one row per actual result × ranking scope, with event metadata)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_result_detail AS
SELECT
    r.ittfid,
    {competitor_key_sql('r', 'i')}                               AS competitor_key,
    i.ittfid IS NULL                                             AS is_unmapped,
    r.entity_type,
    {competitor_name_sql('r', 'i')}                              AS competitor_name,
    i.country_code                                               AS country_code,
    i.ittfid IS NOT NULL                                         AS identity_resolved,
    r.event_id,
    -- event level (silver.events, joined on event_id)
    COALESCE(d.event_name, CONCAT('Event #', CAST(r.event_id AS STRING))) AS event_name,
    d.start_date,
    d.event_year,
    d.event_quarter_start,
    COALESCE(d.tier, '{NOT_IN_EM}')                              AS tier,
    d.event_type_general_code,
    COALESCE(d.organization_code, '{NOT_IN_EM}')                 AS organization_code,
    -- row level (silver.events_metadata, joined on event_id + ranking_category_code + age_category_code)
    m.event_type_code,
    m.event_type_desc,
    m.ranking_category_desc,
    m.event_category_code                                        AS event_category_code,
    d.primary_category                                           AS event_primary_category,
    COALESCE(d.is_senior_event, FALSE)                           AS event_is_senior,
    r.subevent_code,
    r.ranking_category_code,
    r.age_category_code,
    r.category_code,
    r.ranking_scope,
    r.is_primary_copy,
    r.is_result_row,
    r.result_position,
    r.result_position_base,
    r.stage,
    r.stage_order,
    r.stage_group,
    r.entry_type,
    r.ranking_points,
    r.ranking_year                                               AS first_ranking_year,
    r.ranking_week                                               AS first_ranking_week,
    r.week_key                                                   AS first_week_key,
    r.expiry_year,
    r.expiry_week,
    r.is_qualifier,
    r.is_zpp,
    r.is_mandatory,
    r.matches_played,
    r.matches_won,
    r.matches_lost,
    m.event_id IS NOT NULL                                       AS metadata_category_matched,
    COALESCE(d.in_events, FALSE)                                 AS event_in_events,
    COALESCE(d.in_events_metadata, FALSE)                        AS metadata_event_matched
FROM {per} r
{identity_join_sql('r', 'i')}
LEFT JOIN {em} m
       ON m.event_id = r.event_id
      AND m.ranking_category_code = r.ranking_category_code
      AND m.age_category_code = r.age_category_code
LEFT JOIN {G}.v_evt_event_dim d
       ON d.event_id = r.event_id
WHERE r.is_first_appearance
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 - `v_evt_latest_week`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_latest_week AS
SELECT
    CAST(MAX(week_key) DIV 100 AS INT) AS latest_year,
    CAST(MAX(week_key) % 100 AS INT)   AS latest_week,
    MAX(week_key)                      AS latest_week_key,
    CONCAT(CAST(MAX(week_key) DIV 100 AS STRING), ' W', LPAD(CAST(MAX(week_key) % 100 AS STRING), 2, '0')) AS latest_week_label
FROM {per}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 - `v_evt_calendar_density` (visual 01)
# MAGIC Events per quarter at **event grain**: each event counts once. `primary_category` is `SEN`, `YOU` or `SEN+YOU`
# MAGIC (a youth event that also has a senior draw); `organization_code` is `EventsMetadata.OrganizationCode`. This avoids double-counting youth
# MAGIC events that also carry SEN rows.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_calendar_density AS
SELECT
    event_quarter_start,
    event_quarter_label,
    event_year,
    primary_category,
    organization_code,
    tier,
    COUNT(*) AS event_count
FROM {G}.v_evt_event_dim
WHERE start_date IS NOT NULL
GROUP BY event_quarter_start, event_quarter_label, event_year, primary_category, organization_code, tier
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5 - `v_evt_tier_mix_trend` (visual 02)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_tier_mix_trend AS
WITH ev AS (
    SELECT event_year, organization_code, primary_category, COUNT(*) AS event_count
    FROM {G}.v_evt_event_dim WHERE event_year IS NOT NULL
    GROUP BY event_year, organization_code, primary_category
),
res AS (
    SELECT event_year, organization_code, event_primary_category AS primary_category,
           COUNT(*) AS ranked_results,
           COUNT(DISTINCT competitor_key) AS ranked_competitors
    FROM {G}.v_evt_result_detail
    WHERE is_result_row AND event_year IS NOT NULL
    GROUP BY event_year, organization_code, event_primary_category
)
SELECT
    COALESCE(ev.event_year, res.event_year)             AS event_year,
    COALESCE(ev.organization_code, res.organization_code)         AS organization_code,
    COALESCE(ev.primary_category, res.primary_category) AS primary_category,
    COALESCE(ev.event_count, 0)                         AS event_count,
    COALESCE(res.ranked_results, 0)                     AS ranked_results,
    COALESCE(res.ranked_competitors, 0)                 AS ranked_competitors
FROM ev
FULL OUTER JOIN res
  ON ev.event_year = res.event_year AND ev.organization_code <=> res.organization_code
 AND ev.primary_category <=> res.primary_category
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6 - `v_evt_player_timeline` (visual 03)
# MAGIC Every event a competitor has played, with name, tier and result, and whether it **counts in the latest week**
# MAGIC (`counted_now`). The "now" flags read the latest week only, and the dashboard filters on `ittfid`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_player_timeline AS
WITH lw AS (SELECT latest_week_key FROM {G}.v_evt_latest_week),
now AS (   -- latest week only (one week of rows), not the whole history
    SELECT p.ittfid, p.event_id, p.subevent_code, p.ranking_category_code, p.age_category_code, p.category_code,
           MAX(CASE WHEN p.is_counted THEN 1 ELSE 0 END) = 1 AS counted_now,
           MAX(p.ranking_points)                             AS points_now
    FROM {per} p JOIN lw ON p.week_key = lw.latest_week_key
    GROUP BY p.ittfid, p.event_id, p.subevent_code, p.ranking_category_code, p.age_category_code, p.category_code
)
SELECT
    d.*,
    n.ittfid IS NOT NULL               AS listed_now,
    COALESCE(n.counted_now, FALSE)     AS counted_now,
    n.points_now,
    CONCAT(CAST(d.first_ranking_year AS STRING), ' W', LPAD(CAST(d.first_ranking_week AS STRING), 2, '0')) AS first_week_label,
    CASE WHEN n.counted_now THEN 'Counted' WHEN n.ittfid IS NOT NULL THEN 'Listed, not counted' ELSE 'Expired' END AS status_now
FROM {G}.v_evt_result_detail d
LEFT JOIN now n
  ON n.ittfid = d.ittfid AND n.event_id = d.event_id AND n.subevent_code <=> d.subevent_code
 AND n.ranking_category_code <=> d.ranking_category_code AND n.age_category_code <=> d.age_category_code
 AND n.category_code <=> d.category_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7 - `v_evt_best_results_current` (visual 04)
# MAGIC Latest week only: every result listed for each competitor, with whether it counted (best-N) toward this week's
# MAGIC published points. Order by `best_n_order`: counted results first, in their ranking order. A result with zero-point
# MAGIC penalties appears once per counted slot (`result_type = 'ZPP'`, `result_slot` = its best-N slot), exactly as the
# MAGIC published points are composed. `counted_points` is what each row adds to the published points: 0 for a `ZPP` slot
# MAGIC (the source repeats the event's points on ZPP rows), `ranking_points` otherwise.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_best_results_current AS
SELECT
    p.ittfid,
    p.entity_type,
    {competitor_key_sql('p', 'i')} AS competitor_key,
    {competitor_name_sql('p', 'i')} AS competitor_name,
    p.ranking_category_code,
    p.age_category_code,
    p.ranking_scope,
    p.category_code,
    p.event_id,
    COALESCE(d.event_name, CONCAT('Event #', CAST(p.event_id AS STRING))) AS event_name,
    COALESCE(d.tier, '{NOT_IN_EM}') AS tier,
    COALESCE(d.organization_code, '{NOT_IN_EM}') AS organization_code,
    d.start_date,
    m.event_type_desc,
    p.result_position,
    p.stage,
    p.result_type,
    p.result_slot,
    p.ranking_points,
    -- what the row contributes to the published points: a ZPP slot counts as 0 (it carries the event's points in the
    -- source, up to 140 in the real data, but a zero-point penalty adds nothing)
    CASE WHEN p.result_type = 'ZPP' THEN 0 ELSE p.ranking_points END AS counted_points,
    p.player_best_ranking_result_number,
    p.is_counted,
    CASE WHEN p.is_counted THEN 'Counted (best-N)' ELSE 'Not counted' END AS counted_label,
    CASE WHEN p.is_counted THEN p.player_best_ranking_result_number ELSE 1000 END AS best_n_order,
    p.is_mandatory,
    p.is_zpp,
    p.expiry_year,
    p.expiry_week,
    lw.latest_year,
    lw.latest_week
FROM {per} p
{identity_join_sql('p', 'i')}
JOIN {G}.v_evt_latest_week lw ON p.week_key = lw.latest_week_key
LEFT JOIN {em} m   -- row level
       ON m.event_id = p.event_id AND m.ranking_category_code = p.ranking_category_code
      AND m.age_category_code = p.age_category_code
LEFT JOIN {G}.v_evt_event_dim d ON d.event_id = p.event_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8 - `v_evt_points_per_tier` (visual 05)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_points_per_tier AS
SELECT
    tier,
    ranking_scope,
    ranking_category_code,
    COUNT(*)                                        AS result_count,
    ROUND(AVG(ranking_points), 1)                   AS avg_points,
    PERCENTILE_APPROX(ranking_points, 0.5)          AS median_points,
    MAX(ranking_points)                             AS max_points,
    COUNT(DISTINCT event_id)                        AS event_count
FROM {G}.v_evt_result_detail
WHERE ranking_points IS NOT NULL
GROUP BY tier, ranking_scope, ranking_category_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 9 - `v_evt_stage_distribution` (visual 06)
# MAGIC Stage reached comes from the result position code (`W`, `F`, `SF`, `QF`, `R16`…`R256`, `G*L` groups, `QR*`
# MAGIC qualifying), parsed in silver. This replaces the blueprint's placeholder "position vs rounds" bucketing.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_stage_distribution AS
SELECT
    event_year,
    tier,
    event_category_code,
    stage_group,
    MIN(CASE stage_group WHEN 'Winner' THEN 1 WHEN 'Final' THEN 2 WHEN 'Semifinal' THEN 3 WHEN 'Quarterfinal' THEN 4
                         WHEN 'Main draw (R16 & earlier)' THEN 5 WHEN 'Group stage' THEN 6 WHEN 'Qualifying' THEN 7 ELSE 8 END) AS stage_group_order,
    COUNT(*) AS result_count
FROM {G}.v_evt_result_detail
WHERE is_result_row
GROUP BY event_year, tier, event_category_code, stage_group
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 10 - `v_evt_qualifier_performance` (visual 07)
# MAGIC Three entry types (from silver): **Direct entry**, **Qualifier (main draw)** (came through qualifying; the
# MAGIC source `Qualifier` flag), and **Lost in qualifying** (a QR* position).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_qualifier_performance AS
SELECT
    event_year,
    event_category_code,
    entry_type,
    COUNT(*)                                                                       AS result_count,
    ROUND(100.0 * SUM(CASE WHEN stage_order <= 4 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_reached_qf_or_better,
    ROUND(100.0 * SUM(CASE WHEN stage_order <= 2 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_reached_final,
    SUM(matches_played)                                                            AS matches_played,
    SUM(matches_won)                                                               AS matches_won,
    ROUND(100.0 * SUM(matches_won) / NULLIF(SUM(matches_played), 0), 1)            AS win_rate_pct
FROM {G}.v_evt_result_detail
WHERE is_result_row
GROUP BY event_year, event_category_code, entry_type
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 11 - `v_evt_organization_share` (visual 08)
# MAGIC Share of ranked results by `EventsMetadata.OrganizationCode` (WTT / ITTF / ...), per event year. Results whose event
# MAGIC isn't in `EventsMetadata` yet have no event year, so they are outside this view.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_organization_share AS
WITH x AS (
    SELECT event_year, organization_code, COUNT(*) AS result_count, COUNT(DISTINCT event_id) AS event_count
    FROM {G}.v_evt_result_detail
    WHERE is_result_row AND event_year IS NOT NULL
    GROUP BY event_year, organization_code
)
SELECT
    event_year,
    organization_code,
    result_count,
    event_count,
    ROUND(100.0 * result_count / SUM(result_count) OVER (PARTITION BY event_year), 1) AS result_share_pct
FROM x
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 12 - `v_evt_zpp_tracker` (visual 09)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_zpp_tracker AS
SELECT
    event_year,
    tier,
    event_category_code,
    COUNT(*)                                                       AS result_count,
    SUM(CASE WHEN is_zpp THEN 1 ELSE 0 END)                        AS zpp_results,
    ROUND(100.0 * SUM(CASE WHEN is_zpp THEN 1 ELSE 0 END) / COUNT(*), 2) AS zpp_rate_pct
FROM {G}.v_evt_result_detail
WHERE is_result_row
GROUP BY event_year, tier, event_category_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 13 - `v_evt_mandatory_compliance` (visual 10, adapted; see header)
# MAGIC The latest week's mandatory-inclusion results for every ranked competitor, with their current rank. Current rank
# MAGIC comes from the gold rank facts (SEN run, latest week): individuals use `subevent_code = ranking_category_code`
# MAGIC (MS/WS/MDI/WDI/XDI), pairs use MD/WD/XD. The dashboard filters `current_rank <= 20`.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_mandatory_compliance AS
WITH li AS (SELECT MAX(ranking_year * 100 + ranking_week) AS mk FROM {fact_individual} WHERE ranking_run_code = 'SEN'),
     lp AS (SELECT MAX(ranking_year * 100 + ranking_week) AS mk FROM {fact_pair} WHERE ranking_run_code = 'SEN'),
ranks AS (
    SELECT f.ittfid, f.subevent_code AS ranking_category_code, MIN(f.current_rank) AS current_rank
    FROM {fact_individual} f JOIN li ON f.ranking_year * 100 + f.ranking_week = li.mk
    WHERE f.ranking_run_code = 'SEN' AND f.current_rank IS NOT NULL
    GROUP BY f.ittfid, f.subevent_code
    UNION ALL
    SELECT f.ittfid, f.subevent_code, MIN(f.current_rank)
    FROM {fact_pair} f JOIN lp ON f.ranking_year * 100 + f.ranking_week = lp.mk
    WHERE f.ranking_run_code = 'SEN' AND f.current_rank IS NOT NULL
    GROUP BY f.ittfid, f.subevent_code
)
SELECT
    b.ranking_category_code,
    r.current_rank,
    b.ittfid,
    b.competitor_name,
    b.event_id,
    b.event_name,
    b.tier,
    b.start_date,
    b.result_position,
    b.ranking_points,
    b.is_counted,
    CASE WHEN b.is_counted THEN 'Counted' ELSE 'Not counted' END AS counted_label,
    b.latest_year,
    b.latest_week
FROM {G}.v_evt_best_results_current b
JOIN ranks r ON r.ittfid = b.ittfid AND r.ranking_category_code = b.ranking_category_code
WHERE b.is_mandatory AND b.ranking_scope = 'SEN'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 14 - `v_evt_first_senior_event` (visual 11)
# MAGIC Each individual's first result at a **senior event**, by event start date, decided **only from `EventsMetadata`**:
# MAGIC an event is senior when every one of its `EventsMetadata` rows has `EventCategoryCodeforRankingCalculations = 'SEN'`
# MAGIC (`v_evt_event_dim.is_senior_event`). A youth event that also carries SEN-category rows (e.g. a Youth Contender with a
# MAGIC senior draw) is `SEN+YOU`, not senior. The result's own metadata row must also be SEN. A first SEN result inside the
# MAGIC 2021 backfill window may be the start of the data rather than a genuine debut, so `debut_is_at_data_start` flags
# MAGIC debuts in the earliest year on record.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_first_senior_event AS
WITH s AS (
    SELECT d.*,
           ROW_NUMBER() OVER (PARTITION BY d.competitor_key ORDER BY d.start_date, d.event_id, d.ranking_category_code) AS rn
    FROM {G}.v_evt_result_detail d
    WHERE d.entity_type = 'INDIVIDUAL' AND d.event_category_code = 'SEN' AND d.event_is_senior
      AND d.start_date IS NOT NULL
),
first_year AS (SELECT MIN(event_year) AS y0 FROM {G}.v_evt_result_detail)
SELECT
    s.competitor_key,
    s.is_unmapped,
    s.competitor_name,
    s.country_code,
    s.event_id          AS debut_event_id,
    s.event_name        AS debut_event_name,
    s.tier              AS debut_tier,
    s.start_date        AS debut_date,
    YEAR(s.start_date)  AS debut_year,
    s.ranking_category_code AS debut_ranking_category_code,
    s.stage             AS debut_stage,
    YEAR(s.start_date) = fy.y0 AS debut_is_at_data_start
FROM s CROSS JOIN first_year fy
WHERE s.rn = 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 15 - `v_evt_win_rate_leaderboard` (visual 12)
# MAGIC Per competitor and ranking category, across their whole recorded career:
# MAGIC - events played, titles, finals, QF-or-better rate;
# MAGIC - match win rate where matches are recorded (`win_rate_pct` is NULL when `matches_played = 0`).
# MAGIC
# MAGIC The dashboard applies a minimum-sample guard (`matches_played >= N` or `events_played >= N`).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_win_rate_leaderboard AS
WITH per_event AS (
    SELECT competitor_key, is_unmapped, entity_type, competitor_name, country_code, ranking_category_code, event_id,
           MIN(stage_order) AS best_stage_order,
           MAX(matches_played) AS matches_played, MAX(matches_won) AS matches_won
    FROM {G}.v_evt_result_detail
    WHERE is_result_row
    GROUP BY competitor_key, is_unmapped, entity_type, competitor_name, country_code, ranking_category_code, event_id
)
SELECT
    competitor_key,
    is_unmapped,
    entity_type,
    competitor_name,
    country_code,
    ranking_category_code,
    COUNT(*)                                                         AS events_played,
    SUM(CASE WHEN best_stage_order = 1 THEN 1 ELSE 0 END)            AS titles,
    SUM(CASE WHEN best_stage_order <= 2 THEN 1 ELSE 0 END)           AS finals,
    ROUND(100.0 * SUM(CASE WHEN best_stage_order <= 4 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_qf_or_better,
    COALESCE(SUM(matches_played), 0)                                 AS matches_played,
    COALESCE(SUM(matches_won), 0)                                    AS matches_won,
    ROUND(100.0 * SUM(matches_won) / NULLIF(SUM(matches_played), 0), 1) AS win_rate_pct
FROM per_event
GROUP BY competitor_key, is_unmapped, entity_type, competitor_name, country_code, ranking_category_code
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 16 - `v_evt_event_summary` (events list; "this week's events")
# MAGIC One row per event in the metadata. `is_latest_week_event` marks events whose ranking week is the latest loaded week.
# MAGIC `winners` lists the winner per ranking category.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_event_summary AS
WITH res AS (
    SELECT event_id,
           COUNT(*)                AS result_count,
           COUNT(DISTINCT competitor_key) AS competitor_count,
           MAX(ranking_points)     AS max_points,
           CONCAT_WS(' | ', SORT_ARRAY(COLLECT_SET(
               CASE WHEN stage_order = 1 THEN CONCAT(ranking_category_code, ' ', age_category_code, ': ', competitor_name) END))) AS winners
    FROM {G}.v_evt_result_detail
    WHERE is_result_row
    GROUP BY event_id
)
SELECT
    d.*,
    COALESCE(r.result_count, 0)     AS result_count,
    COALESCE(r.competitor_count, 0) AS competitor_count,
    r.max_points,
    NULLIF(r.winners, '')           AS winners,
    (d.event_ranking_year * 100 + d.event_ranking_week) = lw.latest_week_key AS is_latest_week_event,
    lw.latest_week_label
FROM {G}.v_evt_event_dim d
CROSS JOIN {G}.v_evt_latest_week lw
LEFT JOIN res r ON r.event_id = d.event_id
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 17 - `v_evt_kpis` (dashboard counters)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_kpis AS
WITH r AS (   -- one pass over the result detail
    SELECT
        SUM(CASE WHEN is_result_row THEN 1 ELSE 0 END)                                         AS results_tracked,
        COUNT(DISTINCT competitor_key)                                                           AS competitors_tracked,
        COUNT(DISTINCT CASE WHEN is_unmapped THEN ittfid END)                                    AS unmapped_competitors,
        SUM(CASE WHEN is_result_row AND metadata_event_matched AND organization_code = 'WTT' THEN 1 ELSE 0 END) AS wtt_results,
        SUM(CASE WHEN is_result_row AND metadata_event_matched THEN 1 ELSE 0 END)              AS matched_results
    FROM {G}.v_evt_result_detail
),
e AS (
    SELECT COUNT(*) AS events_tracked,
           SUM(CASE WHEN is_latest_week_event THEN 1 ELSE 0 END) AS events_latest_week
    FROM {G}.v_evt_event_summary
)
SELECT
    e.events_tracked,
    e.events_latest_week,
    r.results_tracked,
    r.competitors_tracked,
    r.unmapped_competitors,
    ROUND(100.0 * r.wtt_results / NULLIF(r.matched_results, 0), 1)     AS wtt_share_pct,   -- of results with metadata
    ROUND(100.0 * r.matched_results / NULLIF(r.results_tracked, 0), 1) AS metadata_coverage_pct,
    lw.latest_week_label
FROM r CROSS JOIN e CROSS JOIN {G}.v_evt_latest_week lw
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 18 - `v_evt_unmapped_competitors` (drill-down behind the ITTFID_<category> buckets)
# MAGIC One row per unmapped competitor ID: its `CompetitorId` is not in `Competitors` (individuals: MS, WS, MDI, WDI, XDI)
# MAGIC or `Players_Doubles` (pairs: MD, WD, XD), so silver could not resolve it. `unmapped_bucket` is the label the
# MAGIC competitor carries on every other tile. `missing_from` says which source table should hold it.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {G}.v_evt_unmapped_competitors AS
WITH lw AS (SELECT latest_week_key FROM {G}.v_evt_latest_week)
SELECT
    CONCAT('ITTFID_', p.ranking_category_code)                  AS unmapped_bucket,
    p.ranking_category_code,
    p.entity_type,
    CASE p.entity_type WHEN 'INDIVIDUAL' THEN 'Competitors' WHEN 'PAIR' THEN 'Players_Doubles'
                       ELSE 'no identity table' END             AS missing_from,
    p.ittfid,
    SUM(CASE WHEN p.is_result_row THEN 1 ELSE 0 END)            AS results,
    COUNT(DISTINCT p.event_id)                                  AS events,
    MIN(p.week_key)                                             AS first_week_key,
    MAX(p.week_key)                                             AS last_week_key,
    MAX(CASE WHEN p.week_key = lw.latest_week_key THEN 1 ELSE 0 END) = 1 AS listed_in_latest_week,
    1                                                           AS competitor_count
FROM {per} p
CROSS JOIN lw
{identity_join_sql('p', 'i')}
WHERE i.ittfid IS NULL
GROUP BY p.ranking_category_code, p.entity_type, p.ittfid
""")

# COMMAND ----------

display(spark.sql(f"SHOW VIEWS IN {catalog_name}.{gold_schema} LIKE 'v_evt_*'"))