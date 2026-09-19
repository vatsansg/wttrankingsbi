# Databricks notebook source
# MAGIC %md
# MAGIC ## 07. Silver reference table registry
# MAGIC Drives `03-silver/06.Silver Reference Table (Generic)` the same way
# MAGIC `00-common/05.reference-table-registry` drives bronze notebook 07 --
# MAGIC one entry per reference table, run once per key by the silver job's
# MAGIC for-each task. Covers the 12 reference tables only -- `competitors`
# MAGIC and `players_doubles` (the other 2 of bronze's 14 generic tables) feed
# MAGIC `03-silver/01.Build Player Identity Dimension` instead of a 1:1 silver
# MAGIC copy, since their whole purpose in Silver is to become the unified
# MAGIC `ittfid` dimension, not a standalone table.
# MAGIC
# MAGIC `has_is_deleted`: whether the bronze table carries an `IsDeleted` flag
# MAGIC worth filtering on for silver (only 1 of the 12 do now -- most of these
# MAGIC reference tables have no soft-delete column at all, so there's nothing
# MAGIC to filter).
# MAGIC
# MAGIC **BUGFIX (2026-09-13):** `ref_countries` flipped from `True` to `False`.
# MAGIC Its source table changed from `Countries_TTU` to `Countries` (DBA
# MAGIC consolidation -- see `00-common/05.reference-table-registry`), and the
# MAGIC new `Countries` table has no `IsDeleted` column at all. Leaving this on
# MAGIC `True` would make `06.Silver Reference Table (Generic)` fail with a
# MAGIC column-not-found error the moment it tries to filter on `is_deleted`.

# COMMAND ----------

SILVER_REFERENCE_TABLES = {
    "ref_countries":                     {"has_is_deleted": False},
    "ref_continents":                    {"has_is_deleted": False},
    "ref_age_categories":                {"has_is_deleted": False},
    "ref_categories":                    {"has_is_deleted": False},
    "ref_ranking_categories":            {"has_is_deleted": False},
    "ref_subevent_types":                {"has_is_deleted": True},
    "ref_subevents_codes":               {"has_is_deleted": False},
    "ref_subevents_codes_description":   {"has_is_deleted": False},
    "ref_subevent_dependent_categories": {"has_is_deleted": False},
    "ref_organization":                  {"has_is_deleted": False},
    "ref_event_type_general":            {"has_is_deleted": False},
    "ref_result_position":               {"has_is_deleted": False},
}
