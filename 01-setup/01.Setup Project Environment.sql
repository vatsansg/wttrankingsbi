-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Set-up the project environment for WTT RANKINGS BI Project
-- MAGIC 1. Create External Location rankings-bi_ext-wttrankings
-- MAGIC 1. Create Catalog wttrankingsbi
-- MAGIC 1. Create Schemas landing, bronze, silver and gold
-- MAGIC 1. Create Volume Files in the landing schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Access Cloud Storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/landing/2026 - 31/'

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/bronze'
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `rankings-bi_ext-wttrankings`
URL 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `rankings-bi-sc`)
COMMENT 'External location for the wttrankings container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Catalog wttrankingsbi

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS wttrankingsbi
   MANAGED LOCATION 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/' 
   COMMENT 'This is the main catalog for the wtt rankings BI project' ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Schemas landing, bronze, silver, gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS wttrankingsbi.landing;
CREATE SCHEMA IF NOT EXISTS wttrankingsbi.bronze
    MANAGED LOCATION 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS wttrankingsbi.silver
    MANAGED LOCATION 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS wttrankingsbi.gold
    MANAGED LOCATION 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/gold';         

-- COMMAND ----------

-- DBTITLE 1,Verify storage write access
-- Smoke test: verify the storage credential can write to the managed locations
CREATE TABLE IF NOT EXISTS wttrankingsbi.bronze._setup_test (id INT);
INSERT INTO wttrankingsbi.bronze._setup_test VALUES (1);
SELECT * FROM wttrankingsbi.bronze._setup_test;
-- DROP TABLE IF EXISTS wttrankingsbi.bronze._setup_test;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG wttrankingsbi;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Volume Files

-- COMMAND ----------

CREATE EXTERNAL VOLUME wttrankingsbi.landing.files
LOCATION 'abfss://wttrankings@sarankingsbiext.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/wttrankingsbi/landing/files