# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingest Ref Countries (worked example, no parameters)
# MAGIC Source: `dbo.Countries` (Azure SQL Server) via JDBC -- the single source
# MAGIC of truth for country codes.
# MAGIC
# MAGIC **BUGFIX (2026-09-13):** this used to read `dbo.Countries_TTU`, but the
# MAGIC source DBA removed that table and consolidated it into the plain
# MAGIC `Countries` table. `Countries.ContinentId` is a `varchar` holding the
# MAGIC real 5-value continent code (`AFR`/`AME`/`ASI`/`EUR`/`OCE`) directly --
# MAGIC unlike `Countries_TTU.ContinentId`, an unrelated integer id-space that
# MAGIC never resolved against `ref_continents`. The column list below is also
# MAGIC pinned to `Countries`'s actual (smaller) schema, confirmed via
# MAGIC `INFORMATION_SCHEMA.COLUMNS` -- `CreatedBy`, `LastModifiedBy`,
# MAGIC `IsDeleted`, `TelCode`, and `IndividualNamingConvention` no longer exist
# MAGIC on this table. See `00-common/05.reference-table-registry` for the
# MAGIC matching fix in the generic/parameterized notebook this one mirrors, and
# MAGIC `04-gold/01.Build Dim Country` for the corresponding gold-side fix.
# MAGIC
# MAGIC This notebook is functionally identical to running
# MAGIC `02-bronze/07.Ingest Reference Table (Generic)` with
# MAGIC `p_table_key = 'ref_countries'` -- it exists purely so the generic/
# MAGIC parameterized pattern has one fully spelled-out reading companion. Every
# MAGIC other reference/player table follows this exact shape; only the source
# MAGIC table name and column list change (see `00-common/05.reference-table-registry`
# MAGIC for all 19).
# MAGIC
# MAGIC Note: `CountryCode` here is the 3-letter WTT/ITTF federation code, not
# MAGIC ISO-3166 (e.g. TPE = Chinese Taipei) -- do not conflate with ISO country
# MAGIC codes in Silver/Gold.
# MAGIC
# MAGIC 1. Read the table via JDBC, pinned to an explicit column list
# MAGIC 2. Add Metadata Columns
# MAGIC 3. Write to bronze delta table (full overwrite)
# MAGIC 4. Validate row count
# MAGIC
# MAGIC **BUGFIX (2026-09-13, caught by expert pre-run review):** the write
# MAGIC below now needs `.option('overwriteSchema', 'true')` -- this notebook's
# MAGIC column list just shrank from 13 columns to 8 and `ContinentId` changed
# MAGIC type (int -> string), and Delta rejects an `overwrite` write that
# MAGIC changes the target table's schema unless this option is set explicitly.
# MAGIC See `00-common/05.reference-table-registry` and
# MAGIC `02-bronze/07.Ingest Reference Table (Generic)` for the matching fix.

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.jdbc-helpers

# COMMAND ----------

source_table = 'Countries'
table_name = f"{catalog_name}.{bronze_schema}.ref_countries"

columns = [
    "CountryId",
    "CountryCode",
    "CountryName",
    "ContinentId",
    "IsActive",
    "Flag",
    "CreatedDateTime",
    "LastUpdatedDateTime",
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read from Azure SQL Server via JDBC

# COMMAND ----------

ref_countries_df = read_from_sqlserver(source_table, columns)
display(ref_countries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Add Metadata Columns

# COMMAND ----------

ref_countries_final_df = add_jdbc_ingestion_metadata(ref_countries_df, source_table)
display(ref_countries_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to bronze delta table

# COMMAND ----------

(
    ref_countries_final_df
        .write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Validate

# COMMAND ----------

validate_bronze_table(table_name)