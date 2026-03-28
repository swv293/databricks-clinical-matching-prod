# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Pipeline (Post-Pipeline Task)
# MAGIC
# MAGIC Runs after the SDP pipeline completes, before DQ metrics.
# MAGIC Performs cross-table assertions to verify pipeline integrity.
# MAGIC Raises an exception on any failure to halt downstream tasks.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "serverless_stable_swv01_catalog"
P = f"{CATALOG}.pipeline_prd"

failures = []

def assert_check(name: str, condition: bool, detail: str = ""):
    """Record a check result. Failures are collected and raised at the end."""
    if condition:
        print(f"  [PASS] {name}")
    else:
        msg = f"{name}: {detail}" if detail else name
        print(f"  [FAIL] {msg}")
        failures.append(msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table Existence and Row Counts

# COMMAND ----------

print("=== Table Existence & Row Counts ===")

parsed_count = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.clinical_doc_parsed").first().c
assert_check("clinical_doc_parsed has rows", parsed_count > 0, f"count={parsed_count}")

structured_count = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.clinical_doc_structured").first().c
readable_count = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.clinical_doc_parsed WHERE unreadable_flag = false").first().c
if readable_count > 0:
    assert_check("clinical_doc_structured has rows (readable docs exist)", structured_count > 0, f"structured={structured_count}, readable={readable_count}")
else:
    print(f"  [SKIP] No readable docs — structured table empty as expected")

member_pairs = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.doc_member_pairs_features").first().c
member_matches = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.doc_member_match_candidates").first().c
auth_pairs = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.doc_auth_pairs_features").first().c
auth_matches = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.doc_auth_match_candidates").first().c
events_count = spark.sql(f"SELECT COUNT(*) AS c FROM {P}.match_events").first().c

print(f"  Row counts: parsed={parsed_count}, structured={structured_count}, "
      f"member_pairs={member_pairs}, member_matches={member_matches}, "
      f"auth_pairs={auth_pairs}, auth_matches={auth_matches}, events={events_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. No Duplicate doc_ids

# COMMAND ----------

print("\n=== Duplicate Detection ===")

parsed_dupes = spark.sql(f"""
    SELECT doc_id, COUNT(*) AS cnt FROM {P}.clinical_doc_parsed
    GROUP BY doc_id HAVING COUNT(*) > 1
""").count()
assert_check("No duplicate doc_ids in clinical_doc_parsed", parsed_dupes == 0, f"dupes={parsed_dupes}")

structured_dupes = spark.sql(f"""
    SELECT doc_id, COUNT(*) AS cnt FROM {P}.clinical_doc_structured
    GROUP BY doc_id HAVING COUNT(*) > 1
""").count()
assert_check("No duplicate doc_ids in clinical_doc_structured", structured_dupes == 0, f"dupes={structured_dupes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Referential Integrity

# COMMAND ----------

print("\n=== Referential Integrity ===")

# All structured doc_ids must exist in parsed
orphan_structured = spark.sql(f"""
    SELECT COUNT(*) AS c FROM {P}.clinical_doc_structured s
    WHERE NOT EXISTS (SELECT 1 FROM {P}.clinical_doc_parsed p WHERE p.doc_id = s.doc_id)
""").first().c
assert_check("All structured doc_ids exist in parsed", orphan_structured == 0, f"orphans={orphan_structured}")

# If member_pairs has rows, member_matches must too
if member_pairs > 0:
    assert_check("member_matches populated when member_pairs exist",
                 member_matches > 0, f"pairs={member_pairs}, matches={member_matches}")

# If auth_pairs has rows, auth_matches must too
if auth_pairs > 0:
    assert_check("auth_matches populated when auth_pairs exist",
                 auth_matches > 0, f"pairs={auth_pairs}, matches={auth_matches}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Value Validation

# COMMAND ----------

print("\n=== Value Validation ===")

# match_class values must be in allowed set
if member_matches > 0:
    bad_classes = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {P}.doc_member_match_candidates
        WHERE match_class NOT IN ('match', 'possible_match', 'non_match')
    """).first().c
    assert_check("All match_class values are valid (member)", bad_classes == 0, f"invalid={bad_classes}")

if auth_matches > 0:
    bad_auth_classes = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {P}.doc_auth_match_candidates
        WHERE match_class NOT IN ('match', 'possible_match', 'non_match')
    """).first().c
    assert_check("All match_class values are valid (auth)", bad_auth_classes == 0, f"invalid={bad_auth_classes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Batch Completeness

# COMMAND ----------

print("\n=== Batch Completeness ===")

# Every non-unreadable parsed doc should have a structured record
if readable_count > 0 and structured_count > 0:
    missing_structured = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {P}.clinical_doc_parsed p
        WHERE p.unreadable_flag = false
        AND NOT EXISTS (SELECT 1 FROM {P}.clinical_doc_structured s WHERE s.doc_id = p.doc_id)
    """).first().c
    extraction_rate = round(100.0 * (readable_count - missing_structured) / readable_count, 1)
    assert_check(f"Extraction completeness >= 80% (actual: {extraction_rate}%)",
                 extraction_rate >= 80.0, f"missing={missing_structured}/{readable_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Result

# COMMAND ----------

print(f"\n{'='*60}")
if failures:
    print(f"VALIDATION FAILED — {len(failures)} check(s) failed:")
    for f_msg in failures:
        print(f"  - {f_msg}")
    raise Exception(f"Pipeline validation failed: {len(failures)} check(s) — {'; '.join(failures)}")
else:
    print("ALL VALIDATION CHECKS PASSED")
print(f"{'='*60}")
