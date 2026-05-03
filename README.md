# Clinical Document Matching — Production Pipeline (Lakeflow SDP)

End-to-end Lakeflow Streaming Declarative Pipeline (SDP) for clinical document matching: incremental ingest from a Unity Catalog volume, AI-powered OCR + structured extraction, parallel Fellegi-Sunter probabilistic matching for members and authorizations, and a fan-in event log — all serverless, all governed by data-quality expectations.

> **Companion repo:** [`interop-demo`](https://github.com/swv293/interop-demo) is the **learn-and-explore surface** — synthetic data generators, the seven-notebook walkthrough, full DDL, dashboards, a Genie space, and the OpenAPI spec for downstream consumers. Use that repo first to populate `ref.member` and `raw.authorization` (this pipeline reads from those). Use **this** repo to deploy the same logic in streaming, production-shaped form.

Licensed under the [Apache License, Version 2.0](LICENSE).

## Architecture

```
Auto Loader (UC Volume: /Volumes/.../raw/raw_docs/clinical_docs)
       ↓
clinical_doc_parsed         ← ai_parse_document (OCR + layout)
       ↓
clinical_doc_structured     ← ai_query (typed-STRUCT extraction via responseFormat)
     ↓            ↓
member_pairs    auth_pairs              ← parallel branches (blocking + similarity)
     ↓            ↓
member_matches  auth_matches            ← parallel branches (Fellegi-Sunter scoring)
     ↓            ↓
match_events    (fan-in via @dp.append_flow → unified, append-only audit log)
```

All tables live in the `pipeline_prd` schema. Dashboard-ready views live in `dashboard_prd`. Both schemas are deliberately isolated from the `ref / raw / curated / analytics / dashboard` schemas owned by the companion repo's notebook chain — the two pipelines can co-exist in the same catalog without conflict.

## Prerequisites

1. **Databricks workspace** with Unity Catalog and Serverless compute enabled.
2. **Databricks CLI** ≥ v0.220 installed locally and authenticated to the target workspace:
   ```bash
   databricks auth login --profile <your-profile>
   ```
3. **Source tables already populated** in the same catalog:
   - `<catalog>.ref.member` (golden master)
   - `<catalog>.raw.authorization` (open authorizations)

   **If those tables don't exist yet**, run notebook `01_ingest_seed_data.py` from the [interop-demo](https://github.com/swv293/interop-demo) repo first. It seeds 1,000 members and 3,000 authorizations from the included synthetic CSVs.
4. **A UC volume** to drop documents into. Default path: `/Volumes/<catalog>/raw/raw_docs/clinical_docs`. Generate sample PDFs/TIFFs with `data/generation/generate_pdfs.py` from the companion repo.

## Repository Layout

```
databricks-clinical-matching-prod/
├── bundles/
│   └── prod_bundle.yml              # Asset Bundle: pipeline + 5-task job + dashboard
├── pipeline/
│   ├── sdp_pipeline.py              # Lakeflow SDP DAG (Auto Loader → AI → matching → fan-in)
│   └── pipeline_config.py           # Shared constants (schemas, FS thresholds, DQ caps)
├── post_pipeline/
│   ├── validate_pipeline.py         # Cross-table integrity checks; halts the job on failure
│   ├── refresh_dq_metrics.py        # Computes DQ metrics → pipeline_prd.dq_run_results
│   └── refresh_dashboard.py         # Refreshes views in dashboard_prd; logs the run
├── kafka/
│   └── publish_kafka.py             # Optional Kafka publisher (parallel with DQ task)
├── dashboards/
│   ├── clinical_matching_dq_prd.lvdash.json   # Production dashboard (deployed by bundle)
│   └── clinical_matching_dq.lvdash.json       # Earlier dev variant — reference only
├── notebooks/
│   └── DQ_Analysis.py               # Ad-hoc DQ exploration notebook
├── tests/
│   ├── test_jaro_winkler.py         # Pure-Python similarity tests
│   └── test_fs_weights.py           # Fellegi-Sunter weight unit tests
├── LICENSE
└── README.md
```

## Quick Start

### 1. Validate the bundle

```bash
cd bundles
databricks bundle validate
```

### 2. Deploy

```bash
databricks bundle deploy
```

This creates four resources in the workspace:
- **Pipeline**: `clinical-doc-matching-sdp` (serverless, channel: `PREVIEW`, target schema: `pipeline_prd`)
- **Job**: `clinical-doc-matching-prod` (5-task DAG; cron `0 0 * * * ?` — *paused by default*)
- **Dashboard**: `Clinical Document Matching - DQ & Match Dashboard (PRD)` published under `/Shared/Clinical-Matching`

### 3. Trigger the pipeline

Either run the SDP directly:

```bash
databricks pipelines start-update --pipeline-id <pipeline_id_from_deploy_output>
```

Or run the full job end-to-end (pipeline → validate → DQ → dashboard refresh, with Kafka in parallel):

```bash
databricks jobs run-now --job-id <job_id_from_deploy_output>
```

### 4. Drop a document and watch it flow through

Copy a PDF or TIFF into the UC volume — Auto Loader picks it up on the next trigger:

```bash
databricks fs cp ./sample.pdf dbfs:/Volumes/<catalog>/raw/raw_docs/clinical_docs/
```

## Pipeline Run Modes

The pipeline supports two run modes:

| Mode | How to trigger | Behavior |
|------|---------------|----------|
| **Incremental** (default) | `start-update` with default flags | Auto Loader processes only new files since last checkpoint. Cheap and fast. |
| **Full refresh** | `start-update --full-refresh` | Reprocesses every document in the volume. Use when Fellegi-Sunter weights or extraction prompts change. |

The pipeline detects mode via `spark.databricks.dlt.fullRefresh` and prints it in the run log.

## DQ Gates — How the Pipeline Stops Itself

Every table in `sdp_pipeline.py` declares **expectations** alongside its logic, using three escalation levels:

| Decorator | Behavior on violation |
|-----------|----------------------|
| `@dp.expect(name, predicate)` | Row recorded as a violation but kept |
| `@dp.expect_or_drop(name, predicate)` | Row dropped from the table |
| `@dp.expect_or_fail(name, predicate)` | Pipeline run **fails** — bad data never lands |

Examples in this pipeline:
- `clinical_doc_parsed` warns on missing `doc_id` or unreadable docs lacking text.
- `clinical_doc_structured` drops rows with no extracted identifiers (`expect_or_drop`).
- `doc_member_match_candidates` and `doc_auth_match_candidates` **fail the run** if a row is classified as a `match` with `total_weight < 4.0` — a logical impossibility that indicates a configuration regression (`expect_or_fail`).

This is the "DQ gates as code" feature: contract violations halt the pipeline before bad data reaches a downstream consumer.

## The 5-Task Job DAG

```
sdp_pipeline (Lakeflow SDP)
├── validate_pipeline      (depends on sdp_pipeline)
│   └── refresh_dq_metrics (depends on validate_pipeline)
│       └── refresh_dashboard (depends on refresh_dq_metrics)
└── publish_kafka          (depends on sdp_pipeline — runs in parallel with the DQ branch)
```

- **`validate_pipeline`** — cross-table assertions (row counts, no duplicate doc_ids, FK integrity). Raises on any failure to halt downstream tasks.
- **`refresh_dq_metrics`** — computes per-batch metrics into `pipeline_prd.dq_run_results`. Raises if `pct_unreadable > 5%`, `pct_missing_dob > 20%`, or `pct_missing_ssn4 > 30%`. Logs a record in `pipeline_run_log`.
- **`refresh_dashboard`** — `REFRESH TABLE` over every view in `dashboard_prd`, then logs a successful run.
- **`publish_kafka`** — sends non-match events from the latest run to Kafka. Skips gracefully if the secrets scope isn't configured. Runs **in parallel** with the DQ branch by design — it cannot block the dashboard from refreshing.

If any DQ check fails, the dashboard does not refresh — the operations team gets a known signal instead of stale or corrupt downstream data.

## Optional: Kafka Setup

The Kafka publisher requires a Databricks secrets scope named `clinical-matching` with two keys:

```bash
databricks secrets create-scope clinical-matching
databricks secrets put-secret clinical-matching kafka_brokers --string-value "broker1:9092,broker2:9092"
databricks secrets put-secret clinical-matching kafka_jaas --string-value "<your SASL JAAS config>"
```

If the secrets aren't present, the task exits cleanly with `SKIPPED - no Kafka secrets configured` and the rest of the job continues.

## Local Testing

```bash
pip install pytest pyspark
pytest tests/ -v
```

`tests/test_jaro_winkler.py` and `tests/test_fs_weights.py` validate the pure-Python similarity and Fellegi-Sunter weight logic without spinning up a workspace.

## Verifying a Successful Run

After a run, check the four most useful tables:

```sql
-- Latest run summary
SELECT * FROM <catalog>.pipeline_prd.pipeline_run_log ORDER BY run_ts DESC LIMIT 5;

-- DQ metrics for the last run
SELECT * FROM <catalog>.pipeline_prd.dq_run_results ORDER BY run_ts DESC LIMIT 5;

-- Match-event volume by class
SELECT match_class, entity_type, COUNT(*) AS n
FROM <catalog>.pipeline_prd.match_events
GROUP BY match_class, entity_type
ORDER BY entity_type, match_class;

-- Open the dashboard
-- /Shared/Clinical-Matching/Clinical Document Matching - DQ & Match Dashboard (PRD)
```

## Configuration Reference

Edit `pipeline/pipeline_config.py` to retune. Key knobs:

| Constant | Default | What it controls |
|---|---|---|
| `FS_MATCH_THRESHOLD` | `8.0` | Minimum total weight for a `match` classification |
| `FS_POSSIBLE_MATCH_THRESHOLD` | `4.0` | Minimum total weight for a `possible_match` |
| `FS_W_SSN4_AGREE` | `4.595` | Log-likelihood weight when SSN4 matches (= ln(0.99/0.01)) |
| `FS_W_DOB_AGREE` | `3.892` | Log-likelihood weight when DOB matches (= ln(0.98/0.02)) |
| `DQ_MAX_UNREADABLE_PCT` | `5.0` | DQ gate ceiling |
| `DQ_MAX_MISSING_DOB_PCT` | `20.0` | DQ gate ceiling |
| `DQ_MAX_MISSING_SSN4_PCT` | `30.0` | DQ gate ceiling |
| `AUTOLOADER_MAX_FILES_PER_TRIGGER` | `100` | Throughput knob — scale up under SLA pressure |
| `AUTOLOADER_MAX_BYTES_PER_TRIGGER` | `"10g"` | Throughput knob — bytes per trigger |

## Cross-Schema Co-existence with `interop-demo`

Both repos read from the same catalog but **never write to the same schema**:

| Repo | Writes to |
|------|-----------|
| `interop-demo` (notebook chain) | `ref`, `raw`, `curated`, `analytics`, `dashboard` |
| `clinical-matching-prod` (this repo) | `pipeline_prd`, `dashboard_prd` |

Shared inputs (`ref.member`, `raw.authorization`) are read-only from this side. You can run both pipelines in the same workspace simultaneously.

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
