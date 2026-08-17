# PulseOps — Real-Time Incident Detection & Root-Cause Summarization Pipeline

A streaming data engineering pipeline that replays real production logs through
Kafka, detects incidents in-flight with PySpark Structured Streaming, lands
curated results in Snowflake, validates detection accuracy against ground-truth
labels, and — only as a final, thin layer — uses an LLM to turn already-computed
evidence into a one-shot RCA summary.

**This is a data engineering project first.** The LLM step is intentionally the
smallest, last piece: it explains facts the pipeline already computed. It never
decides what counts as an incident.

## The problem

When a production incident hits, on-call engineers spend the first 30-60 minutes
just figuring out *what broke and where* — scanning logs across many nodes,
correlating error bursts, and forming a hypothesis by hand. That triage lag is
the real-world cost this pipeline targets: turn a raw event stream into "here's
the incident, here's the evidence, here's what changed" in near real time.

## Dataset

[Loghub HDFS_v1](https://github.com/logpai/loghub/tree/master/HDFS) — real logs
from an 11M-line, 32-node HDFS cluster (CUHK), with block-level anomaly labels
already assigned by researchers. Used here for two reasons:
1. It's real production log structure, not synthetic data.
2. The existing labels let us **measure precision/recall of our own streaming
   detector against ground truth** — a concrete, defensible accuracy claim
   instead of "it looks like it works."

Citation: Zhu et al., *"Loghub: A Large Collection of System Log Datasets for
AI-driven Log Analytics,"* ISSRE 2023.

## Architecture (build order = MVP priority order)

```
HDFS_v1 raw logs
      │  (replay_producer.py — original timestamp order, speed multiplier)
      ▼
   Kafka topic: hdfs-log-events              [1] ingestion
      │
      ▼
PySpark Structured Streaming                 [2] THE CORE STORY
  - regex-parses raw log lines
  - tumbling window aggregation per block/node
  - deterministic threshold rule → incident flag
      │  foreachBatch
      ▼
Snowflake                                    [3] curated storage
  RAW_LOG_EVENTS · WINDOWED_METRICS · INCIDENTS · GROUND_TRUTH_LABELS
      │
      ▼
evaluate_detection.py                        [4] validation
  precision / recall / F1 vs GROUND_TRUTH_LABELS
      │
      ▼
summarize_incident.py (Gemini, one-shot CLI) [5] thin explanation layer
  facts in → RCA paragraph + suggested action out
  (no chat loop, no UI, no multi-turn state)
```

GCP: a single Compute Engine VM runs Kafka (Docker, KRaft mode) and the Spark
job. Cloud Storage holds the Structured Streaming checkpoint directory. No GKE,
no Dataproc cluster, no extra moving parts — the trial's real constraint is time,
not compute.

## Warehouse modeling (star schema)

`RAW_LOG_EVENTS` / `WINDOWED_METRICS` / `INCIDENTS` are the **operational**
tables the streaming job writes to directly. On top of those,
`snowflake/schema.sql` also defines a small **dimensional layer** —
`DIM_BLOCK`, `DIM_DATE`, `DIM_COMPONENT`, and `FACT_INCIDENT_EVENTS` — meant to
be populated periodically (a scheduled Snowflake TASK, or a small batch job)
rather than per-microbatch. This is deliberately separate from the streaming
path: dimensional modeling is a data warehousing skill distinct from streaming
ingestion, and keeping the two layers visibly separate in the schema makes
that distinction legible to anyone reviewing the project.

## What's explicitly *not* in the MVP

- No chat interface. No conversational memory. No multi-turn "copilot."
- No Snowflake Cortex dependency (`AI_COMPLETE`, Cortex Search, External Access
  Integration) — lesson carried over from AP Autopilot: trial accounts may
  block these, so all LLM calls happen from the Python backend, and Snowflake
  is used purely as the data store.
- No dashboard by default. `evaluate_detection.py` prints a report; incidents
  and evidence are queryable directly in Snowflake. A read-only incident list
  UI is a clearly optional stretch goal, built last, only if time remains.

## Setup order

1. `data/download_hdfs_dataset.sh` — fetch and unzip HDFS_v1
2. `docker-compose up -d` — start Kafka (KRaft, single broker)
3. Run `snowflake/schema.sql` in a worksheet — creates warehouse/db/schema/tables
4. `python ingestion/replay_producer.py --speed 60` — start replaying logs
5. `spark-submit streaming/spark_streaming_job.py` — start the streaming job
6. `python evaluation/evaluate_detection.py` — precision/recall vs ground truth
7. `python llm/summarize_incident.py --block-id <id>` — one-shot RCA summary

## Challenges & Solutions

*(fill in as you build — same section style as AP Autopilot's README; this is
where trial-account quirks, connector issues, and design trade-offs go)*
