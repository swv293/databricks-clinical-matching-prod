# Databricks notebook source
# Shared configuration constants for the Clinical Document Matching pipeline.
# This file is included as a Lakeflow SDP file dependency.

CATALOG          = "serverless_stable_swv01_catalog"
VOLUME_PATH      = f"/Volumes/{CATALOG}/raw/raw_docs/clinical_docs"
SCHEMA_PATH      = f"/Volumes/{CATALOG}/raw/raw_docs/_checkpoints/schema"
REF_SCHEMA       = f"{CATALOG}.ref"
RAW_SCHEMA       = f"{CATALOG}.raw"
CURATED_SCHEMA   = f"{CATALOG}.curated"
ANALYTICS_SCHEMA = f"{CATALOG}.analytics"
DASHBOARD_SCHEMA = f"{CATALOG}.dashboard"

# Fellegi-Sunter decision thresholds
FS_MATCH_THRESHOLD          = 8.0
FS_POSSIBLE_MATCH_THRESHOLD = 4.0

# Fellegi-Sunter field weights (log-likelihood ratios)
# SSN4: ln(0.99/0.01) ≈ 4.595 — heaviest anchor
# DOB:  ln(0.98/0.02) ≈ 3.892 — second anchor
# Names: scaled Jaro-Winkler → range [-1, +1] × 2
FS_W_SSN4_AGREE    =  4.595
FS_W_SSN4_DISAGREE = -4.595
FS_W_DOB_AGREE     =  3.892
FS_W_DOB_DISAGREE  = -3.892
FS_NAME_SCALE      =  2.0

# DQ thresholds — breach any of these and the pipeline halts
DQ_MAX_UNREADABLE_PCT   = 5.0
DQ_MAX_MISSING_DOB_PCT  = 20.0
DQ_MAX_MISSING_SSN4_PCT = 30.0

# Auto Loader batch sizing for scale-up under SLA pressure
# maxFilesPerTrigger controls throughput vs latency tradeoff
# Start at 100, scale to 1000+ if SLA is at risk
AUTOLOADER_MAX_FILES_PER_TRIGGER = 100
AUTOLOADER_MAX_BYTES_PER_TRIGGER = "10g"
