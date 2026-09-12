# Databricks notebook source
# MAGIC %md
# MAGIC ## 08. Gold helpers
# MAGIC Shared by every Step 4 gold notebook. Unlike Silver's `resolve_identity`
# MAGIC (which had to reconcile two different id spaces), every foreign key a
# MAGIC gold fact carries -- `country_code`, `subevent_code`, `age_category_code`,
# MAGIC `ittfid` -- already exists as a plain column on the silver source row.
# MAGIC Building a fact here is carry-the-key-forward, not look-it-up: the actual
# MAGIC join to a dimension happens later, at query/dashboard time, not while
# MAGIC building the fact. So there is no gold equivalent of `resolve_identity` --
# MAGIC just a metadata stamp, kept here for the same reason silver has one.

# COMMAND ----------

from pyspark.sql import functions as F


def add_gold_metadata(df):
    """Stamp every gold row with when this gold pass produced it."""
    return df.withColumn('_gold_updated_timestamp', F.current_timestamp())