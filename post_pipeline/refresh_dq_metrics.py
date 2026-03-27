# Databricks notebook source
# MAGIC %md
# MAGIC # Refresh DQ Metrics (Post-Pipeline Task)
# MAGIC
# MAGIC Runs as a serverless job task after the SDP pipeline completes.
# MAGIC Computes data quality metrics for the latest pipeline run and writes to
# MAGIC the `dq_run_results` table. Raises an exception if any DQ threshold is breached.

# COMMAND ----------

# DBTITLE 1,Cell 2
from pyspark.sql import functions as F

CATALOG = "serverless_stable_swv01_catalog"

# ── Ensure the DQ results table exists ────────────────────────────────────────

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.analytics.dq_run_results (
        batch_id          TIMESTAMP   COMMENT 'Pipeline run timestamp',
        run_ts            TIMESTAMP   COMMENT 'When this DQ check ran',
        total_docs        BIGINT      COMMENT 'Total documents in this batch',
        pct_unreadable    DOUBLE      COMMENT 'Percent unreadable documents',
        pct_missing_dob   DOUBLE      COMMENT 'Percent missing DOB',
        pct_missing_ssn4  DOUBLE      COMMENT 'Percent missing SSN4',
        parse_errors      BIGINT      COMMENT 'Count of parse errors',
        pct_match         DOUBLE      COMMENT 'Percent classified as match',
        pct_possible_match DOUBLE     COMMENT 'Percent classified as possible_match',
        pct_non_match     DOUBLE      COMMENT 'Percent classified as non_match'
    ) USING DELTA
    COMMENT 'Data quality metrics per pipeline run'
""")

# ── Ensure the pipeline run log table exists ──────────────────────────────────

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.analytics.pipeline_run_log (
        run_ts     TIMESTAMP   COMMENT 'When this run completed',
        status     STRING      COMMENT 'SUCCESS or FAILED',
        run_by     STRING      COMMENT 'User or service principal'
    ) USING DELTA
    COMMENT 'Pipeline execution audit log'
""")

# ── Compute DQ metrics for the latest pipeline run ────────────────────────────

spark.sql(f"""
    INSERT INTO {CATALOG}.analytics.dq_run_results
    WITH parsed AS (
        -- Source: clinical_doc_parsed (parse-level metadata)
        SELECT
            doc_id,
            unreadable_flag,
            parse_error_status
        FROM {CATALOG}.curated.clinical_doc_parsed
    ),
    structured AS (
        -- Source: clinical_doc_structured (LLM-extracted fields with pre-computed DQ flags)
        SELECT
            doc_id,
            missing_dob,
            missing_ssn4
        FROM {CATALOG}.curated.clinical_doc_structured
    ),
    matches AS (
        SELECT doc_id, match_classification AS match_class
        FROM {CATALOG}.analytics.doc_member_match_candidates
    )
    SELECT
        CURRENT_TIMESTAMP()                                                           AS batch_id,
        CURRENT_TIMESTAMP()                                                           AS run_ts,
        COUNT(DISTINCT p.doc_id)                                                      AS total_docs,
        ROUND(100.0 * SUM(CASE WHEN p.unreadable_flag THEN 1 ELSE 0 END)
              / NULLIF(COUNT(DISTINCT p.doc_id), 0), 2)                               AS pct_unreadable,
        ROUND(100.0 * SUM(CASE WHEN s.missing_dob THEN 1 ELSE 0 END)
              / NULLIF(COUNT(DISTINCT p.doc_id), 0), 2)                               AS pct_missing_dob,
        ROUND(100.0 * SUM(CASE WHEN s.missing_ssn4 THEN 1 ELSE 0 END)
              / NULLIF(COUNT(DISTINCT p.doc_id), 0), 2)                               AS pct_missing_ssn4,
        SUM(CASE WHEN p.parse_error_status IS NOT NULL THEN 1 ELSE 0 END)             AS parse_errors,
        ROUND(100.0 * SUM(CASE WHEN mc.match_class = 'match' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(mc.match_class), 0), 2)                                  AS pct_match,
        ROUND(100.0 * SUM(CASE WHEN mc.match_class = 'possible_match' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(mc.match_class), 0), 2)                                  AS pct_possible_match,
        ROUND(100.0 * SUM(CASE WHEN mc.match_class = 'non_match' THEN 1 ELSE 0 END)
              / NULLIF(COUNT(mc.match_class), 0), 2)                                  AS pct_non_match
    FROM parsed p
    LEFT JOIN structured s ON p.doc_id = s.doc_id
    LEFT JOIN matches mc ON p.doc_id = mc.doc_id
""")

# ── Check DQ thresholds ──────────────────────────────────────────────────────

DQ_MAX_UNREADABLE_PCT  = 5.0
DQ_MAX_MISSING_DOB_PCT = 20.0
DQ_MAX_MISSING_SSN4_PCT = 30.0

latest = spark.sql(f"""
    SELECT pct_unreadable, pct_missing_dob, pct_missing_ssn4
    FROM   {CATALOG}.analytics.dq_run_results
    ORDER  BY run_ts DESC
    LIMIT  1
""").first()

breaches = {
    f"pct_unreadable={latest.pct_unreadable}%": latest.pct_unreadable > DQ_MAX_UNREADABLE_PCT,
    f"pct_missing_dob={latest.pct_missing_dob}%": latest.pct_missing_dob > DQ_MAX_MISSING_DOB_PCT,
    f"pct_missing_ssn4={latest.pct_missing_ssn4}%": latest.pct_missing_ssn4 > DQ_MAX_MISSING_SSN4_PCT,
}
failed = [msg for msg, breached in breaches.items() if breached]

if failed:
    # Log failure
    spark.sql(f"""
        INSERT INTO {CATALOG}.analytics.pipeline_run_log
        VALUES (CURRENT_TIMESTAMP(), 'DQ_BREACH', CURRENT_USER())
    """)
    raise Exception(f"DQ BREACH — pipeline halted: {', '.join(failed)}")
else:
    print("All DQ checks passed.")
    print(f"  Unreadable: {latest.pct_unreadable}% (threshold: {DQ_MAX_UNREADABLE_PCT}%)")
    print(f"  Missing DOB: {latest.pct_missing_dob}% (threshold: {DQ_MAX_MISSING_DOB_PCT}%)")
    print(f"  Missing SSN4: {latest.pct_missing_ssn4}% (threshold: {DQ_MAX_MISSING_SSN4_PCT}%)")