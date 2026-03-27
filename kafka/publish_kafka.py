# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Match Events to Kafka (Optional Task)
# MAGIC
# MAGIC Runs in parallel with DQ metrics — does NOT block on DQ.
# MAGIC Publishes non-match events from the latest pipeline run to Kafka.
# MAGIC
# MAGIC Requires Databricks secrets scope `clinical-matching` with:
# MAGIC - `kafka_brokers`: Kafka bootstrap servers
# MAGIC - `kafka_jaas`: SASL JAAS config string

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "serverless_stable_swv01_catalog"

# ── Kafka configuration from secrets ──────────────────────────────────────────

try:
    kafka_options = {
        "kafka.bootstrap.servers": dbutils.secrets.get("clinical-matching", "kafka_brokers"),
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": dbutils.secrets.get("clinical-matching", "kafka_jaas"),
        "topic": "clinical-matching-results",
    }
except Exception as e:
    print(f"Kafka secrets not configured — skipping publish: {e}")
    print("To enable, create a Databricks secrets scope 'clinical-matching' with 'kafka_brokers' and 'kafka_jaas' keys.")
    dbutils.notebook.exit("SKIPPED - no Kafka secrets configured")

# ── Filter events from the last hour's pipeline run ───────────────────────────

events_to_publish = (
    spark.table(f"{CATALOG}.pipeline_prd.match_events")
    .filter(
        (F.col("match_class") != "non_match")
        & (
            F.col("scored_ts")
            >= F.date_trunc("hour", F.current_timestamp() - F.expr("INTERVAL 1 HOUR"))
        )
    )
)

event_count = events_to_publish.count()

if event_count == 0:
    print("No new match events to publish.")
    dbutils.notebook.exit("SKIPPED - no new events")

# ── Publish to Kafka ──────────────────────────────────────────────────────────

(
    events_to_publish
    .selectExpr("CAST(doc_id AS STRING) AS key", "to_json(struct(*)) AS value")
    .write
    .format("kafka")
    .options(**kafka_options)
    .save()
)

print(f"Published {event_count} match events to Kafka topic: clinical-matching-results")
