# Databricks notebook source
# MAGIC %md
# MAGIC # DQ Analysis — Standalone Exploration Notebook
# MAGIC
# MAGIC Interactive notebook for exploring data quality trends, match distributions,
# MAGIC and pipeline health. Not part of the automated pipeline.

# COMMAND ----------

CATALOG = "serverless_stable_swv01_catalog"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Run History

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM analytics.pipeline_run_log ORDER BY run_ts DESC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ Metrics Trend

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   batch_id,
# MAGIC   total_docs,
# MAGIC   pct_unreadable,
# MAGIC   pct_missing_dob,
# MAGIC   pct_missing_ssn4,
# MAGIC   pct_match,
# MAGIC   pct_possible_match,
# MAGIC   pct_non_match
# MAGIC FROM analytics.dq_run_results
# MAGIC ORDER BY run_ts DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Match Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   match_classification AS match_class,
# MAGIC   COUNT(*) AS cnt,
# MAGIC   ROUND(AVG(total_weight), 2) AS avg_weight,
# MAGIC   ROUND(MIN(total_weight), 2) AS min_weight,
# MAGIC   ROUND(MAX(total_weight), 2) AS max_weight
# MAGIC FROM analytics.doc_member_match_candidates
# MAGIC GROUP BY match_classification
# MAGIC ORDER BY avg_weight DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documents Needing Manual Review

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   doc_risk_tier,
# MAGIC   COUNT(*) AS cnt
# MAGIC FROM dashboard.v_doc_match_status
# MAGIC GROUP BY doc_risk_tier
# MAGIC ORDER BY cnt DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fellegi-Sunter Score Histogram

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(total_weight, 0) AS weight_bucket,
# MAGIC   match_classification,
# MAGIC   COUNT(*) AS pair_count
# MAGIC FROM analytics.doc_member_match_candidates
# MAGIC GROUP BY ROUND(total_weight, 0), match_classification
# MAGIC ORDER BY weight_bucket

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline KPIs

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dashboard.v_pipeline_kpis
