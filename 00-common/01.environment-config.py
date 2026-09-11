# Databricks notebook source
# MAGIC %md
# MAGIC ## 01. Environment Config
# MAGIC Central place for every Unity Catalog name, the weekly landing path, and
# MAGIC the Azure SQL Server connection *coordinates* (not credentials) used
# MAGIC across every WTT Rankings bronze notebook.
# MAGIC
# MAGIC Import this into any notebook with `%run ../00-common/01.environment-config`
# MAGIC (same pattern as the Formula 1 reference project).

# COMMAND ----------

# Unity Catalog object names -- confirmed against the environment the user has
# already built (catalog `wttrankingsbi`, schemas `bronze`/`silver`/`gold`,
# external volume `wttrankingsbi.landing.files`) -- see the discovery doc.
catalog_name = 'wttrankingsbi'
bronze_schema = 'bronze'
silver_schema = 'silver'
gold_schema = 'gold'

# COMMAND ----------

# Weekly landing files -- ADF drops RankingIndividuals/RankingPairs CSVs here
# every week, under a "<year> - <week>" subfolder, same as the F1 project reads
# a Volume of per-season files.
landing_folder_path = '/Volumes/wttrankingsbi/landing/files'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Azure SQL Server source (ledger, reference & player master data)
# MAGIC 18 of the 20 bronze tables come from `dbTableTennisUniverse_Ranking_RW37`,
# MAGIC not from files -- new to this project vs. the Formula 1 reference, which
# MAGIC only ever reads files. These are non-secret connection *coordinates* only;
# MAGIC the SQL login itself lives in a Databricks secret scope, wired up in
# MAGIC `03.jdbc-config` (see that notebook for why credentials never appear here
# MAGIC or in any notebook cell).

# COMMAND ----------

sql_server_host = 'dbsvwtt-simulation.database.windows.net'
sql_server_port = 1433
sql_server_database = 'dbTableTennisUniverse_Ranking_RW37'
sql_server_source_schema = 'dbo'

# Confirmed working setup (tested against dbsvwtt-simulation.database.windows.net):
# only the PASSWORD is stored as a Databricks secret -- the SQL login name
# itself isn't sensitive and is set directly below. Created ONCE, out-of-band,
# via the Databricks CLI -- dbutils.secrets has no write API, so this cannot
# be done from inside a notebook:
#
#   databricks secrets create-scope wtt-ranking-db
#   databricks secrets put-secret wtt-ranking-db wttdbadmintest --string-value "<the real password>"
#
# (the secret KEY happens to be the same string as the login name below --
#  that's just the naming choice that was used when this was set up; rename
#  either one independently any time, they don't have to match)
sql_server_secret_scope = 'wtt-ranking-db'
sql_user = 'wttdbadmintest'
sql_server_secret_password_key = 'wttdbadmintest'