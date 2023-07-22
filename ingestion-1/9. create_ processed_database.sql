-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dlrobin/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

show tables in f1_processed

-- COMMAND ----------

desc database f1_raw;

-- COMMAND ----------

