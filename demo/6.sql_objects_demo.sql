-- Databricks notebook source
create database demo;

-- COMMAND ----------

create database if not exists demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database extended demo

-- COMMAND ----------

use demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables in default

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

use demo;
show tables

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

DESC race_results_python;

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2020;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

describe EXTENDED demo.race_results_sql;

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")
-- MAGIC
-- MAGIC # race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT
)
USING parquet
LOCATION "/mnt/formula1dlrobin/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

describe demo.race_results_ext_py

-- COMMAND ----------

describe demo.race_results_ext_sql

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

 
CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

SELECT * FROM demo.pv_race_results;

-- COMMAND ----------

