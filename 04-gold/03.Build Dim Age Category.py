# Databricks notebook source
# MAGIC %md
# MAGIC # Build Dim Age Category
# MAGIC Source: `silver.ref_age_categories`
# MAGIC
# MAGIC One row per `age_category_code` (SEN, U19 ... U11), used by every
# MAGIC dashboard that splits by age band (§03 dashboard 04, "Youth-to-Senior
# MAGIC Pipeline").
# MAGIC
# MAGIC **Column-name note:** the bronze source column is `MinAge_Inclusive` /
# MAGIC `MaxAge_Inclusive` -- already containing an underscore *and* a capital
# MAGIC letter boundary. `to_snake_case`'s two-pass regex produces a double
# MAGIC underscore here (`min_age__inclusive` / `max_age__inclusive`), same
# MAGIC class of cosmetic artifact as `Individuals_EventPenaltyId` ->
# MAGIC `individuals__event_penalty_id` noted (and left as-is, unreferenced) in
# MAGIC the Step 3 independent review. Aliased to the clean single-underscore
# MAGIC name here since this dimension does reference the column.
# MAGIC 1. Read `silver.ref_age_categories`
# MAGIC 2. Write to `gold.dim_age_category`

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/08.gold-helpers

# COMMAND ----------

source_table = f"{catalog_name}.{silver_schema}.ref_age_categories"
target_table = f"{catalog_name}.{gold_schema}.dim_age_category"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read, clean up the double-underscore column names

# COMMAND ----------

ref_df = spark.table(source_table)

dim_age_category_df = ref_df.select(
    'age_category_code',
    'age_category_description',
    F.col('min_age__inclusive').alias('min_age_inclusive'),
    F.col('max_age__inclusive').alias('max_age_inclusive'),
    'category_code',
    'organization_code',
)

display(dim_age_category_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Write to gold delta table

# COMMAND ----------

(
    add_gold_metadata(dim_age_category_df)
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table).count())