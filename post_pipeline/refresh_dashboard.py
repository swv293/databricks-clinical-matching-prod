# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Dashboard Views (Post-Pipeline Task)
# MAGIC
# MAGIC Runs as a serverless job task after DQ metrics pass.
# MAGIC Refreshes all views in the `dashboard_prd` schema and logs the run.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "serverless_stable_swv01_catalog"
PIPELINE_SCHEMA = f"{CATALOG}.pipeline_prd"
DASHBOARD_SCHEMA = f"{CATALOG}.dashboard_prd"

# ── Refresh all dashboard_prd views ───────────────────────────────────────────

dashboard_views = [
    "v_intake_summary",
    "v_dq_metrics",
    "v_match_summary",
    "v_doc_match_status",
    "v_score_distribution",
    "v_auth_match_summary",
    "v_pipeline_kpis",
]

for view in dashboard_views:
    fqn = f"{DASHBOARD_SCHEMA}.{view}"
    try:
        spark.sql(f"REFRESH TABLE {fqn}")
        print(f"  Refreshed: {fqn}")
    except Exception as e:
        # Non-materialized views don't need REFRESH — skip gracefully
        print(f"  Skipped {fqn}: {e}")

# ── Log successful run ────────────────────────────────────────────────────────

spark.sql(f"""
    INSERT INTO {PIPELINE_SCHEMA}.pipeline_run_log
    VALUES (CURRENT_TIMESTAMP(), 'SUCCESS', CURRENT_USER())
""")

print("\nDashboard refresh complete.")
