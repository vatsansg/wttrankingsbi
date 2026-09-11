# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ## 03. JDBC config -- Azure SQL Server
# MAGIC Builds the JDBC URL and connection properties every SQL-Server-sourced
# MAGIC bronze notebook reads through. Pulling credentials from
# MAGIC `dbutils.secrets.get()` (rather than typing them into a widget or a cell)
# MAGIC means the password is never visible in notebook source, job run output,
# MAGIC or git history -- Databricks redacts secret values in all of these
# MAGIC automatically as long as they only ever pass through `dbutils.secrets`.
# MAGIC
# MAGIC **Cluster requirement:** the Microsoft JDBC driver for SQL Server must be
# MAGIC attached to the cluster as a Maven library --
# MAGIC `com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11` (or newer). Do not assume
# MAGIC the runtime bundles it; pin it explicitly on the job cluster / policy so
# MAGIC every run gets the same driver version regardless of DBR upgrades.

# COMMAND ----------

# MAGIC %run ./01.environment-config

# COMMAND ----------

jdbc_url = (
    f"jdbc:sqlserver://{sql_server_host}:{sql_server_port};"
    # NOTE: the connection-string property is `databaseName`, not `database`
    # -- the Microsoft JDBC driver silently ignores an unrecognized property
    # instead of erroring, so `database=` would connect but land on the
    # server's default DB rather than this one. Confirmed via a live test
    # connection that `databaseName` is the one that actually works.
    f"databaseName={sql_server_database};"
    "encrypt=true;trustServerCertificate=false;loginTimeout=30;"
)

connection_properties = {
    # Login name is not a secret -- set directly in 01.environment-config.
    "user": sql_user,
    "password": dbutils.secrets.get(scope=sql_server_secret_scope, key=sql_server_secret_password_key),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    # MS SQL JDBC's default fetch size is tiny (10 rows) and will silently slow
    # every read regardless of table size or partitioning -- always set this.
    "fetchsize": "10000",
}