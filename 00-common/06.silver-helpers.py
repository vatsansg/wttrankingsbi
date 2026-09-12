# Databricks notebook source
# MAGIC %md
# MAGIC ## 06. Silver helpers
# MAGIC Shared by every Step 3 silver notebook: column-name standardization,
# MAGIC the identity-resolution join used by every table that carries an
# MAGIC `ittfid`, and a business-key dedup utility. Nothing here writes a
# MAGIC table itself.

# COMMAND ----------

import re
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def to_snake_case(name):
    """CamelCase / PascalCase -> snake_case. Leaves already-lower names and
    the bronze metadata columns (_source_file, _ingestion_timestamp, batch_id)
    alone."""
    if name.startswith('_'):
        return name
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()


def rename_to_snake_case(df):
    """Rename every column on df to snake_case. Safe to call on a df that's
    already partly snake_case (e.g. the `ittfid` column some source tables
    already use lowercase) -- to_snake_case is idempotent on those."""
    for c in df.columns:
        new_c = to_snake_case(c)
        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df


def add_silver_metadata(df):
    """Stamp every silver row with when this silver pass produced it."""
    return df.withColumn('_silver_updated_timestamp', F.current_timestamp())


def resolve_identity(df, id_col, entity_type_col, player_identity_df):
    """Left-join df to the player_identity dimension on (id_col, entity_type_col)
    == (ittfid, entity_type), pulling in player_name/country_code/etc. and an
    `identity_resolved` flag. Never drops a row -- an unresolved id is a data
    quality signal (surfaced in 07.Validate All Silver Tables), not a reason
    to lose the row.
    """
    identity_slim = (
        player_identity_df
            .select(
                F.col('ittfid').alias('_pi_ittfid'),
                F.col('entity_type').alias('_pi_entity_type'),
                F.col('player_name').alias('identity_player_name'),
                F.col('country_code').alias('identity_country_code'),
                F.col('continent_id').alias('identity_continent_id'),
            )
    )
    joined = (
        df.join(
            identity_slim,
            (df[id_col] == identity_slim['_pi_ittfid'])
            & (df[entity_type_col] == identity_slim['_pi_entity_type']),
            'left',
        )
        .withColumn('identity_resolved', F.col('_pi_ittfid').isNotNull())
        .drop('_pi_ittfid', '_pi_entity_type')
    )
    return joined


def dedup_on_business_key(df, key_cols, order_by_desc_cols):
    """Keep exactly one row per key_cols, preferring the row that sorts first
    by order_by_desc_cols (descending) -- a safety net for silver, not an
    expected occurrence: bronze's 3 ledger tables are mutually exclusive by
    ranking week (confirmed by WTT) and the 2 landing tables are one row per
    natural key already, so this should normally drop zero rows. Kept as an
    explicit, visible step rather than assuming the upstream guarantee always
    holds.
    """
    w = Window.partitionBy(*key_cols).orderBy(*[F.col(c).desc_nulls_last() for c in order_by_desc_cols])
    return (
        df.withColumn('_rn', F.row_number().over(w))
          .filter(F.col('_rn') == 1)
          .drop('_rn')
    )