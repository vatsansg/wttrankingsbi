# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Player Event Results — Phase 12
# MAGIC Source: `bronze.players_events_results`, the single consolidated results table. Target: **`silver.player_event_results`**.
# MAGIC This **replaces `silver.points_ledger`**, which is retired in Phase 12. Every events Gold view, dashboard dataset and
# MAGIC API reads this table, joined to `silver.events` (event level) and `silver.events_metadata` (row level). The
# MAGIC `identity_*` columns stamped here are as at load time; the gold views look identities up at query time.
# MAGIC
# MAGIC **Weekly grain** (unique, validated):
# MAGIC `(ittfid, event_id, subevent_code, ranking_category_code, age_category_code, category_code, result_position, ranking_year,
# MAGIC ranking_week, result_type, result_slot)`. The last two separate the counted zero-point-penalty slots (`ZPP` rows) of a
# MAGIC result and resolve copies the source wrote more than once; see Step 3.
# MAGIC
# MAGIC The same result appears in **every ranking week it is live for** (until it expires). Within one week the source
# MAGIC also lists:
# MAGIC - a youth result under both the **YOU** and the **SEN** ranking `category_code`, with different points (and U21 is a
# MAGIC   SEN-category age band). So `category_code`, not the age band, decides which ranking a row counts toward
# MAGIC   (`ranking_scope`).
# MAGIC - occasionally a qualifying loss **and** a main-draw result for the same player in the same event (lucky loser),
# MAGIC   hence `result_position` in the weekly key.
# MAGIC
# MAGIC **Result key** (one actual performance per ranking scope):
# MAGIC `(ittfid, event_id, subevent_code, ranking_category_code, age_category_code, category_code)`.
# MAGIC
# MAGIC **Three fixes compared with the retired `points_ledger`** (each confirmed on the real sample exports):
# MAGIC 1. **`entity_type` comes from `ranking_category_code`, not `subevent_code`.** An `XD` subevent row with ranking
# MAGIC    category `XDI` is the *individual* player's credit: its CompetitorId is a PlayerID, found in 0% of
# MAGIC    `Players_Doubles.DoublesId`. Only `MD`/`WD`/`XD` *ranking categories* are pairs. `points_ledger` labelled every
# MAGIC    MDI/WDI/XDI row as PAIR, which inflated its "pair identity gap".
# MAGIC 2. **The business key includes `event_id` and `age_category_code`.** `points_ledger` keyed on `result_position`
# MAGIC    instead, so two different events with the same finishing round in the same week, or the SEN and youth copies
# MAGIC    of one result, collapsed into one row.
# MAGIC 3. **Columns are named explicitly**, not via the regex snake_case helper, so the classic
# MAGIC    `MandatoryInclusionforBestResults → mandatory_inclusionfor_best_results` artefact can't happen.
# MAGIC
# MAGIC **Derived columns:**
# MAGIC - `week_key`;
# MAGIC - `result_position_base` (`'R16-7%'` → `'R16'`, `'G2L-33%'` → `'G2L'`);
# MAGIC - `stage`, `stage_order`, `stage_group` (Winner / Final / Semifinal / Quarterfinal / Main draw / Group stage / Qualifying / Other);
# MAGIC - `ranking_scope` (`SEN`/`YOU`, from `category_code`);
# MAGIC - `entry_type`:
# MAGIC   - 'Lost in qualifying' for a QR* position;
# MAGIC   - 'Qualifier (main draw)' when the source `Qualifier` flag is set;
# MAGIC   - otherwise 'Direct entry';
# MAGIC - flags `is_counted` (`PlayerBestRankingResultNumber <> 0`, the result counts toward that week's published points),
# MAGIC   `is_qualifier` (the source flag), `is_zpp`, `is_mandatory`;
# MAGIC - **`is_primary_copy`**: within a week, one row per player × event × ranking category. It prefers the YOU-category
# MAGIC   copy of a youth result over its SEN copy, then the deepest stage. Count-style Gold views use it so one
# MAGIC   performance counts once.
# MAGIC - **`is_first_appearance`**: the first ranking week this result (result key) was listed, taking the deepest stage
# MAGIC   if two positions share that week. Filtering on it gives **one row per actual result**, which is how the
# MAGIC   event-level Gold views avoid counting a result once per live week. Values are as at first ranking; later
# MAGIC   revisions (about 3% of multi-week results in the sample) show in the current-week views.
# MAGIC - **`is_result_row`**: the **counting flag** — exactly one row per player × event × subevent × ranking category over
# MAGIC   the whole history: the earliest week, the YOU copy first, then the deepest stage. Unlike combining
# MAGIC   `is_first_appearance` with `is_primary_copy`, it stays one row when a result's copies first appear in different
# MAGIC   weeks (e.g. a player ageing out of a youth band relabels the age category). Gold "how many" views filter on it.
# MAGIC
# MAGIC **Modes.**
# MAGIC - **Full rebuild:** `p_week_keys` is empty, or `initial` was used in bronze.
# MAGIC - **Weekly:** `p_week_keys` is set (for example `"202640,202641"`, from bronze's `loaded_week_keys` taskValue). Only
# MAGIC   those weeks are rebuilt, with a partition-pruned `replaceWhere`. `is_first_appearance` is computed against the
# MAGIC   first-appearance rows of all earlier weeks (one row per result, so the anti-join stays small).
# MAGIC - **Guard:** if a loaded week is **older** than a week already in silver (an out-of-order back-fill), later weeks'
# MAGIC   flags could go stale, so the notebook switches to a full rebuild automatically.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/06.silver-helpers

# COMMAND ----------

dbutils.widgets.text("p_week_keys", "")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window


v_week_keys = [int(k) for k in dbutils.widgets.get("p_week_keys").replace(" ", "").split(",") if k]

source_table = f"{catalog_name}.{bronze_schema}.players_events_results"
identity_table = f"{catalog_name}.{silver_schema}.player_identity"
target_table = f"{catalog_name}.{silver_schema}.player_event_results"

PAIR_RANKING_CATEGORIES = ["MD", "WD", "XD"]
INDIVIDUAL_RANKING_CATEGORIES = ["MS", "WS", "MDI", "WDI", "XDI"]
TEAM_RANKING_CATEGORIES = ["MT", "WT", "XT"]
RESULT_KEY = ["ittfid", "event_id", "subevent_code", "ranking_category_code", "age_category_code", "category_code"]
WEEK_KEY = RESULT_KEY + ["result_position", "ranking_year", "ranking_week"]
COPY_KEY = ["ittfid", "event_id", "subevent_code", "ranking_category_code", "ranking_year", "ranking_week"]
COUNT_KEY = ["ittfid", "event_id", "subevent_code", "ranking_category_code"]
# Silver grain (2026-10-09, from the real _Log/_Log_Archives data): the source writes some results more than once under
# the same WEEK_KEY. A row is identified by WEEK_KEY + result_type + result_slot, where result_slot is the best-N slot of a
# counted row (BestResultNoSENYOU) and 0 for an uncounted row. See Step 3.
GRAIN_KEY = WEEK_KEY + ["result_type", "result_slot"]

FULL_REBUILD = (not v_week_keys) or (not spark.catalog.tableExists(target_table))
if not FULL_REBUILD:
    existing_max = spark.table(target_table).agg(F.max("week_key")).first()[0]
    if existing_max is not None and existing_max > max(v_week_keys):
        print(f"WARN  loaded week(s) {v_week_keys} are older than silver's latest week {existing_max} "
              f"-- switching to a FULL REBUILD so later weeks' first-appearance flags stay correct.")
        FULL_REBUILD = True
print(f"INFO  mode={'FULL REBUILD' if FULL_REBUILD else 'weeks ' + str(v_week_keys)}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read bronze (all weeks, or only the weeks just loaded) and name every column explicitly

# COMMAND ----------

def as_bool(c):
    """Source bit / boolean / int / 'True' -> boolean. NULL stays NULL (unknown), never silently False."""
    return (F.when(F.col(c).isNull(), F.lit(None).cast("boolean"))
             .otherwise(F.lower(F.trim(F.col(c).cast("string"))).isin("1", "true", "t", "y", "yes")))

bronze_df = spark.table(source_table)
if not FULL_REBUILD:
    bronze_df = bronze_df.where((F.col("RankingYear") * 100 + F.col("RankingWeek")).isin(v_week_keys))

named_df = bronze_df.select(
    F.col("source_result_id").cast("long").alias("source_result_id"),
    F.col("CompetitorId").cast("long").alias("competitor_id"),
    F.col("EventId").cast("int").alias("event_id"),
    F.trim("SubEventCode").alias("subevent_code"),
    F.trim("RankingCategoryCode").alias("ranking_category_code"),
    F.trim("ResultPosition").alias("result_position"),
    F.col("RankingPoints").cast("double").alias("ranking_points"),
    F.col("RankingYear").cast("int").alias("ranking_year"),
    F.col("RankingMonth").cast("int").alias("ranking_month"),
    F.col("RankingWeek").cast("int").alias("ranking_week"),
    F.col("ExpiryYear").cast("int").alias("expiry_year"),
    F.col("ExpiryMonth").cast("int").alias("expiry_month"),
    F.col("ExpiryWeek").cast("int").alias("expiry_week"),
    F.col("PlayerBestRankingResultNumber").cast("int").alias("player_best_ranking_result_number"),
    as_bool("Active").alias("is_active"),
    F.col("MatchesPlayed").cast("int").alias("matches_played"),
    F.col("MatchesWon").cast("int").alias("matches_won"),
    F.col("MatchesLost").cast("int").alias("matches_lost"),
    as_bool("Qualifier").alias("qualifier"),
    F.trim("ResultType").alias("result_type"),
    as_bool("ZeroPointPenalty").alias("zero_point_penalty"),
    F.col("LastPhaseWin").cast("string").alias("last_phase_win"),
    F.col("LastPhaseWinWithoutBye").cast("string").alias("last_phase_win_without_bye"),
    as_bool("MandatoryInclusionforBestResults").alias("mandatory_inclusion_for_best_results"),
    as_bool("ExcludedDuetoZeroPointPenalty").alias("excluded_due_to_zero_point_penalty"),
    F.trim("AgeCategoryCode").alias("age_category_code"),
    F.trim("CategoryCode").alias("category_code"),
    F.trim("OrganizationCode").alias("result_ranking_organization_code"),   # ranking organisation on the result row; NOT the event's organisation (that is events_metadata.organization_code)
    F.col("BestResultNoSENYOU").cast("int").alias("best_result_no_sen_you"),
    as_bool("IsImported").alias("is_imported"),
    F.col("ResultCategory").cast("string").alias("result_category"),
    F.col("_source_table").alias("ledger_source"),
    F.col("_ingestion_timestamp").alias("_bronze_ingestion_timestamp"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Identity: `ittfid` + `entity_type` (from ranking category), resolved against `silver.player_identity`

# COMMAND ----------

with_id_df = (
    named_df
        .withColumn("ittfid", F.col("competitor_id").cast("string"))
        .withColumn("entity_type",
                    F.when(F.col("ranking_category_code").isin(PAIR_RANKING_CATEGORIES), F.lit("PAIR"))
                     .when(F.col("ranking_category_code").isin(INDIVIDUAL_RANKING_CATEGORIES), F.lit("INDIVIDUAL"))
                     .when(F.col("ranking_category_code").isin(TEAM_RANKING_CATEGORIES), F.lit("TEAM"))
                     .otherwise(F.lit(None).cast("string")))
)
resolved_df = resolve_identity(with_id_df, "ittfid", "entity_type", spark.table(identity_table))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Resolve source copies on the weekly grain, then derive stage and flags
# MAGIC The source sometimes writes a result more than once under the same `WEEK_KEY` (found in the real `_Log` /
# MAGIC `_Log_Archives`: 834 groups at cut-over). Patterns and the rule applied:
# MAGIC
# MAGIC | Pattern in one `WEEK_KEY` group | What it is | Kept in silver |
# MAGIC |---|---|---|
# MAGIC | identical rows (only the row ID differs) | the same row written twice | **one** |
# MAGIC | one `FINAL_RESULT` + several `ZPP` rows, each **counted** with its own `BestResultNoSENYOU` (1, 2, 3 …) | zero-point-penalty slots that count in the published best-N | **every counted row** (one per slot) |
# MAGIC | one counted copy + one uncounted copy of the same `ResultType` | duplicate; the counted one is what the published ranking used | **the counted copy** |
# MAGIC | uncounted copies with different points (e.g. `MANAULY_MODIFIED`, or a recalculated `FINAL_RESULT`) | older and newer versions | **the most recently written** (highest row ID) |
# MAGIC
# MAGIC So the silver grain is `WEEK_KEY` + `result_type` + `result_slot` (`result_slot` = `BestResultNoSENYOU` for a counted
# MAGIC row, 0 otherwise). Within a `WEEK_KEY` + `result_type`, uncounted copies are dropped when a counted row exists; the
# MAGIC remaining rows are one per grain, keeping the most recently written. `source_copy_count` is the number of source rows
# MAGIC with exactly this grain (identical copies and older versions collapsed into it); an uncounted copy dropped because a
# MAGIC counted row of the same `ResultType` exists is not attributed to a slot. Bronze keeps every source row unchanged; validator 09 reports the copies as WARNs.

# COMMAND ----------

slot_df = (resolved_df
    .withColumn("result_type", F.coalesce(F.col("result_type"), F.lit("")))
    .withColumn("_counted", F.coalesce(F.col("player_best_ranking_result_number"), F.lit(0)) != 0)
    .withColumn("result_slot", F.when(F.col("_counted"),
                                      F.coalesce(F.when(F.col("best_result_no_sen_you") != 0, F.col("best_result_no_sen_you")),
                                                 F.col("player_best_ranking_result_number")))
                                .otherwise(F.lit(0)).cast("int")))
_type_w = Window.partitionBy(*WEEK_KEY, "result_type")
_grain_w = Window.partitionBy(*GRAIN_KEY)
deduped_df = (slot_df
    .withColumn("_type_has_counted", F.max(F.col("_counted").cast("int")).over(_type_w) == 1)
    .withColumn("_type_rows", F.count(F.lit(1)).over(_type_w))
    .where(F.col("_counted") | ~F.col("_type_has_counted"))            # drop uncounted copies of a counted type
    .withColumn("_rn", F.row_number().over(_grain_w.orderBy(F.col("_bronze_ingestion_timestamp").desc_nulls_last(),
                                                            F.col("source_result_id").desc_nulls_last())))
    .withColumn("source_copy_count", F.when(F.col("_counted"), F.count(F.lit(1)).over(_grain_w))
                                      .otherwise(F.col("_type_rows")).cast("int"))
    .where("_rn = 1")
    .drop("_rn", "_counted", "_type_has_counted", "_type_rows"))

base = F.upper(F.trim(F.regexp_extract(F.col("result_position"), r"^([^-]+)", 1)))   # strip the '-x%' suffix
derived_df = (
    deduped_df
        .withColumn("week_key", F.col("ranking_year") * 100 + F.col("ranking_week"))
        .withColumn("result_position_base", F.nullif(base, F.lit("")))
        .withColumn("stage_order",
            F.when(base == "W", 1).when(base == "F", 2).when(base == "SF", 3).when(base == "QF", 4)
             .when(base == "R16", 5).when(base == "R32", 6).when(base == "R64", 7).when(base == "R128", 8)
             .when(base == "R256", 9).when(base.rlike(r"^G[0-9]*L$"), 10).when(base.startswith("QR"), 11).otherwise(12))
        .withColumn("stage",
            F.when(base == "W", "Winner").when(base == "F", "Final").when(base == "SF", "Semifinal")
             .when(base == "QF", "Quarterfinal").when(base.isin("R16", "R32", "R64", "R128", "R256"), F.concat(F.lit("Round "), base))
             .when(base.rlike(r"^G[0-9]*L$"), "Group stage").when(base.startswith("QR"), "Qualifying")
             .otherwise(F.coalesce(F.col("result_position"), F.lit("Unknown"))))
        .withColumn("stage_group",
            F.when(base == "W", "Winner").when(base == "F", "Final").when(base == "SF", "Semifinal")
             .when(base == "QF", "Quarterfinal").when(base.isin("R16", "R32", "R64", "R128", "R256"), "Main draw (R16 & earlier)")
             .when(base.rlike(r"^G[0-9]*L$"), "Group stage").when(base.startswith("QR"), "Qualifying").otherwise("Other"))
        .withColumn("is_counted", F.coalesce(F.col("player_best_ranking_result_number"), F.lit(0)) != 0)
        .withColumn("is_qualifier", F.coalesce(F.col("qualifier"), F.lit(False)))
        .withColumn("entry_type",
            F.when(base.startswith("QR"), "Lost in qualifying")
             .when(F.coalesce(F.col("qualifier"), F.lit(False)), "Qualifier (main draw)")
             .otherwise("Direct entry"))
        .withColumn("ranking_scope",
            F.when(F.col("category_code").isin("SEN", "YOU"), F.col("category_code"))
             .when(F.col("age_category_code") == "SEN", "SEN").otherwise("YOU"))
        .withColumn("is_zpp", F.coalesce(F.col("zero_point_penalty"), F.lit(False))
                               | F.coalesce(F.col("excluded_due_to_zero_point_penalty"), F.lit(False))
                               | (base == "ZPP")
                               # a ZPP row anywhere in the same WEEK_KEY marks the whole result (FINAL_RESULT row too)
                               | (F.max((F.col("result_type") == "ZPP").cast("int")).over(Window.partitionBy(*WEEK_KEY)) == 1))
        .withColumn("is_mandatory", F.coalesce(F.col("mandatory_inclusion_for_best_results"), F.lit(False)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - `is_first_appearance`, then write (full overwrite, or `replaceWhere` on the loaded weeks)

# COMMAND ----------

# Tie-breakers end with result_type and result_slot so every pick is deterministic. When a result's FINAL_RESULT and ZPP
# rows tie on the earlier keys (same stage and points, as in the real data), 'FINAL_RESULT' sorts before 'ZPP', so the
# result's own row is chosen.
copy_w = Window.partitionBy(*COPY_KEY).orderBy(
    F.when(F.col("ranking_scope") == "YOU", 0).otherwise(1), F.col("stage_order"), F.col("ranking_points").desc_nulls_last(),
    F.col("age_category_code"), F.col("category_code"), F.col("result_position"), F.col("result_type"), F.col("result_slot"))
derived_df = derived_df.withColumn("is_primary_copy", F.row_number().over(copy_w) == 1)

first_w = Window.partitionBy(*RESULT_KEY).orderBy(F.col("week_key"), F.col("stage_order"), F.col("ranking_points").desc_nulls_last(),
                                                F.col("result_position"), F.col("result_type"), F.col("result_slot"))
# Counting flag: one row per COUNT_KEY over all history. Same tie-breakers as first_w after the scope preference, so the
# chosen row is always also a first-appearance row.
result_w = Window.partitionBy(*COUNT_KEY).orderBy(
    F.col("week_key"), F.when(F.col("ranking_scope") == "YOU", 0).otherwise(1), F.col("stage_order"),
    F.col("ranking_points").desc_nulls_last(), F.col("age_category_code"), F.col("category_code"), F.col("result_position"),
    F.col("result_type"), F.col("result_slot"))

# Column order: keys and filter columns first, so Delta collects data-skipping stats on them (first 32 columns).
LEAD_COLS = ["week_key", "ittfid", "event_id", "is_first_appearance", "is_primary_copy", "is_result_row", "ranking_scope", "entity_type",
             "subevent_code", "ranking_category_code", "age_category_code", "category_code", "result_position",
             "ranking_week", "is_counted", "result_type", "result_slot"]

def ordered(df):
    # ranking_year is the partition column: keep it last (partition columns are not part of file stats anyway).
    rest = [c for c in df.columns if c not in LEAD_COLS and c != "ranking_year"]
    return df.select(*LEAD_COLS, *rest, "ranking_year")

if FULL_REBUILD:
    final_df = add_silver_metadata(derived_df.withColumn("is_first_appearance", F.row_number().over(first_w) == 1)
                                             .withColumn("is_result_row", F.row_number().over(result_w) == 1))
    (ordered(final_df).write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        .partitionBy("ranking_year").saveAsTable(target_table))
    optimize_where = ""
else:
    min_loaded = min(v_week_keys)
    # Result keys already seen in any earlier week. Only first-appearance rows are read (about one row per result,
    # not one per live week), so this stays small relative to the table.
    earlier_keys = (spark.table(target_table)
                        .where((F.col("week_key") < min_loaded) & F.col("is_first_appearance"))
                        .select(*[F.col(c).alias(f"_e_{c}") for c in RESULT_KEY]).distinct()
                        .withColumn("_seen_before", F.lit(True)))
    # NULL-safe key match, so a NULL subevent/age/category key can't make a result "first" again every week.
    _cond = [derived_df[c].eqNullSafe(earlier_keys[f"_e_{c}"]) for c in RESULT_KEY]
    final_df = (derived_df.join(earlier_keys, _cond, "left").drop(*[f"_e_{c}" for c in RESULT_KEY])
                    .withColumn("is_first_appearance",
                                F.col("_seen_before").isNull() & (F.row_number().over(first_w) == 1))
                    .drop("_seen_before"))
    earlier_counted = (spark.table(target_table)
                           .where((F.col("week_key") < min_loaded) & F.col("is_result_row"))
                           .select(*[F.col(c).alias(f"_c_{c}") for c in COUNT_KEY]).distinct()
                           .withColumn("_counted_before", F.lit(True)))
    _cond = [final_df[c].eqNullSafe(earlier_counted[f"_c_{c}"]) for c in COUNT_KEY]
    final_df = (final_df.join(earlier_counted, _cond, "left").drop(*[f"_c_{c}" for c in COUNT_KEY])
                    .withColumn("is_result_row",
                                F.col("_counted_before").isNull() & (F.row_number().over(result_w) == 1))
                    .drop("_counted_before"))
    final_df = add_silver_metadata(final_df)
    years = sorted({k // 100 for k in v_week_keys})
    predicate = (f"ranking_year IN ({', '.join(str(y) for y in years)}) "
                 f"AND week_key IN ({', '.join(str(k) for k in v_week_keys)})")
    # mergeSchema: a table built before a column was added (e.g. result_slot) still accepts the weekly write.
    (ordered(final_df).write.format("delta").mode("overwrite").option("replaceWhere", predicate)
        .option("mergeSchema", "true").saveAsTable(target_table))
    optimize_where = f" WHERE ranking_year IN ({', '.join(str(y) for y in years)})"

# Co-locate each competitor's rows so the dashboard's player filter skips files (Databricks; harmless if unsupported).
try:
    spark.sql(f"OPTIMIZE {target_table}{optimize_where} ZORDER BY (ittfid, event_id)")
except Exception as e:
    print(f"INFO  OPTIMIZE skipped: {str(e)[:120]}")

# COMMAND ----------

t = spark.table(target_table)
print(f"OK    {target_table}: {t.count():,} rows; {t.where('is_result_row').count():,} counted results")
display(t.groupBy("entity_type", "identity_resolved").count().orderBy("entity_type", "identity_resolved"))