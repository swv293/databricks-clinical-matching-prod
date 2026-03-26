# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh Dashboard Views (Post-Pipeline Task)
# MAGIC
# MAGIC Runs as a serverless job task after DQ metrics pass.
# MAGIC Refreshes all materialized views in the dashboard schema and logs the run.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "serverless_stable_swv01_catalog"

# ── Refresh all dashboard views ───────────────────────────────────────────────

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
    fqn = f"{CATALOG}.dashboard.{view}"
    try:
        spark.sql(f"REFRESH TABLE {fqn}")
        print(f"  Refreshed: {fqn}")
    except Exception as e:
        # Views (non-materialized) don't need REFRESH — skip gracefully
        print(f"  Skipped {fqn}: {e}")

# ── Refresh materialized masking views ────────────────────────────────────────

materialized_views = [
    f"{CATALOG}.curated.member_masked",
    f"{CATALOG}.curated.clinical_doc_parsed_masked",
    f"{CATALOG}.analytics.match_candidates_safe",
]

for mv in materialized_views:
    try:
        spark.sql(f"REFRESH MATERIALIZED VIEW {mv}")
        print(f"  Refreshed MV: {mv}")
    except Exception as e:
        print(f"  Skipped MV {mv}: {e}")

# ── Log successful run ────────────────────────────────────────────────────────

spark.sql(f"""
    INSERT INTO {CATALOG}.analytics.pipeline_run_log
    VALUES (CURRENT_TIMESTAMP(), 'SUCCESS', CURRENT_USER())
""")

print("\nDashboard refresh complete.")
