-- Databricks notebook source
SELECT driver_name,
       COUNT(*) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(1) >= 50
ORDER BY avg_points DESC

-- COMMAND ----------

