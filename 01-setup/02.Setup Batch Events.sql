-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Setup Batch Events
-- MAGIC 1. Create control schema
-- MAGIC 1. Create batch_events table
-- MAGIC 1. Insert an event record

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create control schema

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.control
    MANAGED LOCATION 'abfss://formula1@databrickscourseextdl1.dfs.core.windows.net/control';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Create batch_events table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS formula1.control.batch_events
(
    batch_id INT,
    event_timestamp TIMESTAMP
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Insert an event record

-- COMMAND ----------

INSERT INTO formula1.control.batch_events
VALUES (1, current_timestamp());

-- COMMAND ----------

INSERT INTO formula1.control.batch_events
VALUES (2, current_timestamp());

-- COMMAND ----------

SELECT * FROM formula1.control.batch_events;