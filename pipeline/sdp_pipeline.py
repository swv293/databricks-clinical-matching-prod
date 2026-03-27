# Databricks notebook source
# MAGIC %md
# MAGIC # Clinical Document Matching — Lakeflow SDP Pipeline
# MAGIC
# MAGIC Full streaming DAG for clinical document intake, AI parsing, structured extraction,
# MAGIC Fellegi-Sunter probabilistic matching, and event fan-in.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC Auto Loader (UC Volume)
# MAGIC       ↓
# MAGIC clinical_doc_parsed (ai_parse_document)
# MAGIC       ↓
# MAGIC clinical_doc_structured (ai_query extraction)
# MAGIC     ↓            ↓
# MAGIC member_pairs   auth_pairs     ← PARALLEL branches
# MAGIC     ↓            ↓
# MAGIC member_matches auth_matches   ← PARALLEL branches
# MAGIC     ↓            ↓
# MAGIC match_events (fan-in via append_flow)
# MAGIC ```
# MAGIC
# MAGIC **API:** `from pyspark import pipelines as dp` (Lakeflow SDP)
# MAGIC **Compute:** Serverless, DataFrame-only, no RDDs, no external pip packages

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── Configuration ─────────────────────────────────────────────────────────────

CATALOG     = "serverless_stable_swv01_catalog"
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/raw_docs/clinical_docs"
SCHEMA_PATH = f"/Volumes/{CATALOG}/raw/raw_docs/_checkpoints/schema"
REF_SCHEMA  = f"{CATALOG}.ref"
RAW_SCHEMA  = f"{CATALOG}.raw"

# Fellegi-Sunter thresholds
FS_MATCH = 8.0
FS_POSS  = 4.0

# Fellegi-Sunter log-likelihood weights
W_SSN4_AGREE    =  4.595   # ln(0.99/0.01)
W_SSN4_DISAGREE = -4.595
W_DOB_AGREE     =  3.892   # ln(0.98/0.02)
W_DOB_DISAGREE  = -3.892
NAME_SCALE      =  2.0     # Jaro-Winkler scaled to [-1, +1] × 2

# Auto Loader batch sizing — scale up under SLA pressure
MAX_FILES_PER_TRIGGER = "100"
MAX_BYTES_PER_TRIGGER = "10g"

# ── Pure Python Jaro-Winkler UDF (zero external dependencies) ─────────────────
# Registered at module level outside decorated functions per SDP constraints.

def _jaro(s1: str, s2: str) -> float:
    """Standard Jaro similarity between two strings."""
    if s1 == s2:
        return 1.0
    l1, l2 = len(s1), len(s2)
    if not l1 or not l2:
        return 0.0
    match_distance = max(max(l1, l2) // 2 - 1, 0)
    s1_matches = [False] * l1
    s2_matches = [False] * l2
    matches = 0
    transpositions = 0
    for i in range(l1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, l2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i in range(l1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    return (
        matches / l1 + matches / l2 + (matches - transpositions / 2) / matches
    ) / 3.0


def _jaro_winkler(a: str, b: str, prefix_weight: float = 0.1) -> float:
    """Jaro-Winkler similarity with prefix boost."""
    if not a or not b:
        return 0.0
    a = a.lower().strip()
    b = b.lower().strip()
    if a == b:
        return 1.0
    jaro_sim = _jaro(a, b)
    # Compute common prefix length (max 4)
    prefix_len = 0
    for c1, c2 in zip(a[:4], b[:4]):
        if c1 == c2:
            prefix_len += 1
        else:
            break
    return min(jaro_sim + prefix_len * prefix_weight * (1.0 - jaro_sim), 1.0)


def jw_udf(a, b):
    """PySpark UDF wrapper for Jaro-Winkler similarity."""
    return float(_jaro_winkler(a or "", b or ""))


# Register as SQL-callable UDF for use in expressions
spark.udf.register("jw_sim", jw_udf, DoubleType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLE 1: clinical_doc_parsed
# MAGIC
# MAGIC Auto Loader reads new PDF/TIFF files incrementally from the UC Volume.
# MAGIC `ai_parse_document` performs OCR and layout analysis. Documents are processed
# MAGIC in configurable batches via `maxFilesPerTrigger` for SLA control.

# COMMAND ----------

@dp.table(
    name="clinical_doc_parsed",
    comment="Incrementally parsed clinical documents from MRM UC volume via Auto Loader + ai_parse_document",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dp.expect("doc_id_present", "doc_id IS NOT NULL")
@dp.expect("path_present", "path IS NOT NULL")
def clinical_doc_parsed():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        # Batch sizing for throughput control — scale these up if SLAs are at risk
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .option("cloudFiles.maxBytesPerTrigger", MAX_BYTES_PER_TRIGGER)
        .option("pathGlobFilter", "*.{pdf,PDF,tiff,TIFF,tif,TIF}")
        .load(VOLUME_PATH)
        .withColumn("doc_id", F.md5(F.col("path")))
        .withColumn("ingest_ts", F.current_timestamp())
        .withColumn("pipeline_run_ts", F.date_trunc("hour", F.current_timestamp()))
        # AI document parsing — OCR + layout extraction
        .withColumn(
            "parsed",
            F.expr("ai_parse_document(content, map('version','2.0'))")
        )
        # Concatenate all text elements into a single raw_text field
        .withColumn(
            "raw_text",
            F.expr("""
                concat_ws('\n\n',
                    transform(
                        try_cast(parsed:document:elements AS ARRAY<VARIANT>),
                        e -> try_cast(e:content AS STRING)
                    )
                )
            """)
        )
        .withColumn("parse_error_status", F.expr("try_cast(parsed:error_status AS STRING)"))
        .withColumn("page_count", F.expr("try_cast(parsed:document:page_count AS INT)"))
        .withColumn(
            "unreadable_flag",
            F.col("parse_error_status").isNotNull()
            | F.col("raw_text").isNull()
            | (F.length(F.col("raw_text")) < 50)
        )
        .drop("content", "parsed", "modificationTime", "length")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLE 2: clinical_doc_structured
# MAGIC
# MAGIC `ai_query` extracts typed clinical fields (name, DOB, SSN4, auth ID, provider)
# MAGIC from the raw text. Only processes readable documents.

# COMMAND ----------

# DBTITLE 1,Cell 6
@dp.table(
    name="clinical_doc_structured",
    comment="LLM-extracted structured identifiers from parsed clinical documents",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dp.expect("doc_id_present", "doc_id IS NOT NULL")
@dp.expect(
    "has_extracted_field",
    "first_name IS NOT NULL OR last_name IS NOT NULL OR dob IS NOT NULL",
)
def clinical_doc_structured():
    return (
        dp.read_stream("clinical_doc_parsed")
        .filter(F.col("unreadable_flag") == False)
        .withColumn(
            "s",
            F.expr("""
                ai_query(
                    'databricks-claude-sonnet-4',
                    concat(
                        'Extract patient and authorization identifiers from this clinical document. ',
                        'Return valid JSON only with these keys: first_name, last_name, ',
                        'dob (YYYY-MM-DD), ssn4 (last 4 digits only), member_id_on_form, ',
                        'auth_id, provider_name. Use null for any field not found.\n\n',
                        'Document text:\n"""', raw_text, '"""'
                    ),
                    responseFormat => 'STRUCT<result: STRUCT<
                        first_name:STRING, last_name:STRING, dob:STRING,
                        ssn4:STRING, member_id_on_form:STRING,
                        auth_id:STRING, provider_name:STRING>>'
                )
            """),
        )
        # Parse the JSON string returned by ai_query into a STRUCT
        .withColumn(
            "s",
            F.from_json(
                F.col("s"),
                "result STRUCT<first_name:STRING, last_name:STRING, dob:STRING, ssn4:STRING, member_id_on_form:STRING, auth_id:STRING, provider_name:STRING>"
            )
        )
        # Unwrap the single top-level field required by ai_query responseFormat
        .withColumn("s", F.col("s.result"))
        .select(
            F.col("doc_id"),
            F.col("path"),
            F.col("ingest_ts"),
            F.col("pipeline_run_ts"),
            F.col("page_count"),
            F.col("s.first_name"),
            F.col("s.last_name"),
            F.to_date(F.col("s.dob"), "yyyy-MM-dd").alias("dob"),
            F.col("s.ssn4"),
            F.col("s.member_id_on_form"),
            F.col("s.auth_id"),
            F.col("s.provider_name"),
            F.col("s.dob").isNull().alias("missing_dob"),
            (F.col("s.ssn4").isNull() | (F.col("s.ssn4") == "")).alias("missing_ssn4"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLE 3a: doc_member_pairs_features (Parallel Branch 1)
# MAGIC
# MAGIC Stream-static join: streaming extracted docs × static member reference table.
# MAGIC Blocking on SSN4 or DOB to reduce the comparison space.

# COMMAND ----------

@dp.table(
    name="doc_member_pairs_features",
    comment="Blocked candidate pairs: documents × members with Jaro-Winkler similarity features",
)
@dp.expect_or_drop("valid_pair", "doc_id IS NOT NULL AND member_id IS NOT NULL")
def doc_member_pairs_features():
    docs = dp.read_stream("clinical_doc_structured")
    members = spark.table(f"{REF_SCHEMA}.member")
    return (
        docs.alias("d")
        .join(
            members.alias("m"),
            # Blocking: match on SSN4 OR DOB to reduce pair space
            (F.col("d.ssn4").isNotNull() & (F.col("d.ssn4") == F.col("m.ssn4")))
            | (F.col("d.dob").isNotNull() & (F.col("d.dob") == F.col("m.dob"))),
            "inner",
        )
        .select(
            F.col("d.doc_id"),
            F.col("m.member_id"),
            F.col("d.pipeline_run_ts"),
            F.expr("jw_sim(d.first_name, m.first_name)").alias("first_name_sim"),
            F.expr("jw_sim(d.last_name,  m.last_name)").alias("last_name_sim"),
            (F.col("d.ssn4") == F.col("m.ssn4")).cast("double").alias("ssn4_exact"),
            (F.col("d.dob") == F.col("m.dob")).cast("double").alias("dob_exact"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLE 3b: doc_auth_pairs_features (Parallel Branch 2)
# MAGIC
# MAGIC Runs in parallel with 3a — independent streaming branch off clinical_doc_structured.
# MAGIC Joins on auth_id or member_id_on_form.

# COMMAND ----------

# DBTITLE 1,Cell 10
@dp.table(
    name="doc_auth_pairs_features",
    comment="Blocked candidate pairs: documents × authorizations with similarity features",
)
@dp.expect_or_drop("valid_pair", "doc_id IS NOT NULL AND auth_id IS NOT NULL")
def doc_auth_pairs_features():
    docs = dp.read_stream("clinical_doc_structured")
    auths = spark.table(f"{RAW_SCHEMA}.authorization").alias("a")
    members = spark.table(f"{REF_SCHEMA}.member").alias("m")
    # Enrich auths with member demographics for name comparison
    # Use string join key to auto-deduplicate the shared member_id column
    auths_enriched = auths.join(members, "member_id", "inner")
    return (
        docs.alias("d")
        .join(
            auths_enriched.alias("am"),
            # Blocking: match on auth_id OR member_id_on_form
            (
                F.col("d.auth_id").isNotNull()
                & (F.col("d.auth_id") == F.col("am.auth_number"))
            )
            | (
                F.col("d.member_id_on_form").isNotNull()
                & (F.col("d.member_id_on_form") == F.col("am.member_id"))
            ),
            "inner",
        )
        .select(
            F.col("d.doc_id"),
            F.col("am.auth_id"),
            F.col("d.pipeline_run_ts"),
            F.expr("jw_sim(d.first_name, am.first_name)").alias("first_name_sim"),
            F.expr("jw_sim(d.last_name,  am.last_name)").alias("last_name_sim"),
            (F.col("d.ssn4") == F.col("am.ssn4")).cast("double").alias("ssn4_exact"),
            (F.col("d.dob") == F.col("am.dob")).cast("double").alias("dob_exact"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fellegi-Sunter Weight Helper
# MAGIC
# MAGIC Applies log-likelihood ratio weights to similarity features and classifies pairs.

# COMMAND ----------

def _apply_fs_weights(df):
    """Apply Fellegi-Sunter weights and classify match pairs."""
    return (
        df
        .withColumn(
            "w_ssn4",
            F.when(F.col("ssn4_exact") == 1.0, F.lit(W_SSN4_AGREE))
            .otherwise(F.lit(W_SSN4_DISAGREE)),
        )
        .withColumn(
            "w_dob",
            F.when(F.col("dob_exact") == 1.0, F.lit(W_DOB_AGREE))
            .otherwise(F.lit(W_DOB_DISAGREE)),
        )
        .withColumn("w_first", F.col("first_name_sim") * NAME_SCALE - 1.0)
        .withColumn("w_last", F.col("last_name_sim") * NAME_SCALE - 1.0)
        .withColumn(
            "total_weight",
            F.col("w_ssn4") + F.col("w_dob") + F.col("w_first") + F.col("w_last"),
        )
        .withColumn(
            "match_class",
            F.when(F.col("total_weight") >= FS_MATCH, "match")
            .when(F.col("total_weight") >= FS_POSS, "possible_match")
            .otherwise("non_match"),
        )
        .withColumn("scored_ts", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLE 4a: doc_member_match_candidates (Parallel Branch 1)

# COMMAND ----------

@dp.table(
    name="doc_member_match_candidates",
    comment="Fellegi-Sunter scored member match candidates with classification",
)
def doc_member_match_candidates():
    return _apply_fs_weights(dp.read_stream("doc_member_pairs_features"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLE 4b: doc_auth_match_candidates (Parallel Branch 2)

# COMMAND ----------

@dp.table(
    name="doc_auth_match_candidates",
    comment="Fellegi-Sunter scored auth match candidates with classification",
)
def doc_auth_match_candidates():
    return _apply_fs_weights(dp.read_stream("doc_auth_pairs_features"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## TABLE 5: match_events (Fan-In via append_flow)
# MAGIC
# MAGIC Uses `dp.create_streaming_table` + two `@dp.append_flow` decorators
# MAGIC to merge member and auth match streams into a single append-only audit log.

# COMMAND ----------

dp.create_streaming_table(
    name="match_events",
    comment="Union of all member and auth match events — append-only audit log",
    table_properties={"delta.enableChangeDataFeed": "true"},
)


@dp.append_flow(target="match_events", name="member_match_events_flow")
def member_match_events_flow():
    """Append member match results to the unified event log."""
    return (
        dp.read_stream("doc_member_match_candidates")
        .withColumn("entity_type", F.lit("member"))
        .withColumnRenamed("member_id", "entity_id")
        .select(
            "doc_id", "entity_id", "entity_type", "pipeline_run_ts",
            "match_class", "total_weight",
            "w_ssn4", "w_dob", "w_first", "w_last", "scored_ts",
        )
    )


@dp.append_flow(target="match_events", name="auth_match_events_flow")
def auth_match_events_flow():
    """Append auth match results to the unified event log."""
    return (
        dp.read_stream("doc_auth_match_candidates")
        .withColumn("entity_type", F.lit("auth"))
        .withColumnRenamed("auth_id", "entity_id")
        .select(
            "doc_id", "entity_id", "entity_type", "pipeline_run_ts",
            "match_class", "total_weight",
            "w_ssn4", "w_dob", "w_first", "w_last", "scored_ts",
        )
    )