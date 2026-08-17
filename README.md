# PulseOps - Streaming Behavioral Incident Detection and RCA Pipeline

PulseOps is a data engineering and incident-analysis pipeline built around real
HDFS production logs.

It replays historical HDFS events through Kafka, processes them using PySpark
Structured Streaming, lands curated data in Snowflake, derives behavioral
features at block level, evaluates deterministic anomaly rules against
ground-truth labels, groups related alerts into temporal episodes, and uses
Gemini only as a final explanation layer for already-computed evidence.

**PulseOps is intentionally a data-engineering-first project.**

The LLM does not decide whether an event is anomalous. Detection, severity,
evaluation, episode construction, and evidence selection are deterministic.

---

## Problem

Production incident triage often begins with engineers manually searching large
volumes of logs, correlating events across components, and determining which
alerts deserve attention.

PulseOps explores a pipeline that converts raw infrastructure logs into:

1. structured streaming events,
2. behavioral block-level features,
3. deterministic incident candidates,
4. prioritized temporal incident episodes,
5. measurable detector performance,
6. and evidence-grounded RCA summaries.

The goal is not to claim production-ready anomaly detection accuracy.

The goal is to build and evaluate an observable streaming incident-analysis
pipeline whose strengths and limitations can both be measured.

---

## Dataset

PulseOps uses the Loghub HDFS_v1 dataset.

The deployment copy contained:

- approximately 1.5 GB of HDFS logs,
- 11,175,629 raw log lines,
- approximately 18 MB of anomaly labels,
- 575,062 ground-truth label rows.

The dataset contains real HDFS operational logs and block-level anomaly labels,
which makes it possible to evaluate the detector against known ground truth
rather than relying only on qualitative inspection.

Dataset reference:

[Loghub HDFS Dataset](https://github.com/logpai/loghub/tree/master/HDFS)

Reference paper:

Zhu et al., *Loghub: A Large Collection of System Log Datasets for AI-driven
Log Analytics*, ISSRE 2023.

---

# Architecture

```text
HDFS_v1 raw logs
        |
        | replay_producer.py
        v
Kafka topic: hdfs-log-events
        |
        v
PySpark Structured Streaming
        |
        | regex parsing
        | event-time processing
        | block/window aggregation
        v
Snowflake operational layer
        |
        | RAW_LOG_EVENTS
        | WINDOWED_METRICS
        v
Snowflake behavioral analytics
        |
        | BLOCK_BEHAVIOR_FEATURES
        | BLOCK_BEHAVIOR_DERIVED
        v
Deterministic behavioral detector
        |
        | Rule C
        v
INCIDENTS_BEHAVIORAL
        |
        +-----------------------------+
        |                             |
        v                             v
Evaluation                     Window analytics
precision / recall / F1        RANK / DENSE_RANK
vs ground truth                LAG / LEAD
                               NTILE
                               gaps-and-islands
                                      |
                                      v
                               INCIDENT_EPISODES
                                      |
                                      v
                         behavioral evidence views
                                      |
                                      v
                           Gemini RCA summarizer
                     single-block or episode-level
```

---

# Technology Stack

## Streaming and ingestion

- Apache Kafka 3.7.0
- Python
- `kafka-python`
- Apache Spark
- PySpark Structured Streaming 3.5.1

## Analytics and warehouse

- Snowflake
- SQL
- behavioral feature engineering
- SQL window functions
- gaps-and-islands episode detection
- dimensional modeling

## Evaluation

- Python
- Snowflake ground-truth joins
- precision
- recall
- F1 score
- severity-tier precision
- episode purity

## Explanation layer

- Google `google-genai` SDK
- Gemini
- evidence-grounded one-shot RCA generation

## Deployment

- Google Cloud Compute Engine
- Debian 12
- Docker
- Python virtual environment
- GitHub

---

# Pipeline Walkthrough

## 1. Kafka ingestion

`ingestion/replay_producer.py` replays HDFS log lines into the Kafka topic:

```text
hdfs-log-events
```

Each produced message contains the original raw log line together with an
ingestion timestamp.

Example message:

```json
{
  "raw": "081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block ...",
  "ingest_ts": "2026-08-17T13:40:44.159533"
}
```

Kafka runs as a single-broker Docker deployment for the project MVP.

---

## 2. PySpark Structured Streaming

`streaming/spark_streaming_job.py` consumes events from Kafka.

The streaming pipeline:

- reads Kafka messages,
- extracts the original HDFS log line,
- parses date and time,
- extracts process ID,
- extracts log level,
- extracts component,
- extracts message text,
- extracts HDFS block ID,
- reconstructs event timestamps,
- filters invalid records,
- applies event-time processing,
- calculates block/window metrics,
- and writes micro-batches to Snowflake.

The primary streaming outputs used by the final behavioral pipeline are:

```text
RAW_LOG_EVENTS
WINDOWED_METRICS
```

Spark uses Structured Streaming checkpoints so Kafka progress can be recovered
following a Spark process restart.

---

# Original Static Detector

The original project design included a static Spark threshold detector.

A block/window was considered an incident when:

```text
ERROR_COUNT > 0
```

or:

```text
WARN_COUNT >= 3
```

Detected records were written to:

```text
INCIDENTS
```

This design was deliberately deterministic, but evaluation showed an important
problem.

On the evaluated data, the detector flagged no blocks.

```text
Blocks seen:                 7,940
Blocks flagged:                  0
Ground-truth anomalies:        313

True positives:                  0
False positives:                 0
False negatives:               313

Precision:                   0.0000
Recall:                      0.0000
F1 score:                    0.0000
```

Rather than presenting a detector that did not capture the anomaly behavior,
PulseOps evolved toward behavioral feature engineering in Snowflake.

---

# Behavioral Feature Engineering

The final detector operates at HDFS block level rather than relying only on
ERROR and WARN log levels.

Behavioral features include metrics such as:

- event count,
- component count,
- active duration,
- events per second,
- events per component,
- component diversity,
- short-lived activity patterns.

These features are derived from the structured event data already stored in
Snowflake.

The behavioral layer includes views such as:

```text
BLOCK_BEHAVIOR_FEATURES
BLOCK_BEHAVIOR_DERIVED
```

This allows detection logic to reason about how a block behaves across its
observed activity rather than depending solely on explicit error messages.

---

# Rule C Behavioral Detector

Multiple deterministic behavioral rules were evaluated against ground truth.

The selected Rule C configuration produced the strongest measured F1 score
among the evaluated alternatives.

The final behavioral incidents are exposed through:

```text
INCIDENTS_BEHAVIORAL
```

## Rule C Evaluation

```text
Blocks seen:                 7,940
Blocks flagged:                518
Ground-truth anomalies:         313

True positives:                 140
False positives:                378
False negatives:                173

Precision:                   27.03%
Recall:                      44.73%
F1 score:                    0.3369
```

The detector therefore recovered substantially more anomaly signal than the
original static threshold rule.

However, false-positive volume remains high.

PulseOps reports these numbers directly rather than presenting the detector as
more accurate than the evaluation supports.

---

# Severity-Tier Analysis

Behavioral incidents are separated into HIGH and MEDIUM severity tiers.

Measured precision was:

```text
HIGH severity

Flagged blocks: 74
True positives: 37
False positives: 37
Precision: 50.00%
```

and:

```text
MEDIUM severity

Flagged blocks: 444
True positives: 103
False positives: 341
Precision: 23.20%
```

The HIGH tier therefore contains a substantially stronger concentration of
true anomalies than the MEDIUM tier.

This supports a practical alerting strategy:

- HIGH incidents can receive immediate investigation priority.
- MEDIUM incidents can be routed toward secondary or batch investigation.

Severity therefore provides useful operational prioritization even though the
overall detector still requires further tuning.

---

# Window-Function Analytics

PulseOps uses Snowflake window functions directly on detected behavioral
incidents.

The analytics layer includes:

- `RANK()`
- `DENSE_RANK()`
- `LAG()`
- `LEAD()`
- `NTILE()`
- running `SUM()`
- gaps-and-islands grouping

These operations are used for:

- incident prioritization,
- severity ranking,
- temporal ordering,
- neighboring-incident analysis,
- activity segmentation,
- and episode construction.

The SQL window functions are therefore part of the operational analysis rather
than isolated demonstration queries.

---

# Incident Episodes

Individual alerts are useful, but infrastructure incidents can involve multiple
blocks occurring close together.

PulseOps therefore groups temporally related behavioral incidents into episodes
using a gaps-and-islands pattern.

A selected temporal gap threshold produced:

```text
66 episodes
```

from:

```text
518 behavioral incidents
```

The largest cluster was Episode 66.

## Episode 66

```text
Incidents:             390
Distinct blocks:       390
HIGH severity:          38
MEDIUM severity:       352
Duration:               56 seconds
```

At first glance, a 390-block episode could appear to represent a large
correlated infrastructure failure.

PulseOps therefore performed another validation step before making that claim.

---

# Episode Purity

Episode 66 was compared against the available ground-truth labels.

The result was:

```text
Ground-truth labeled blocks: 390
True anomaly blocks:          24
Normal blocks:               366

Episode purity:             6.15%
```

Only 6.15% of the blocks grouped into Episode 66 were labeled anomalous.

Therefore Episode 66 is **not presented as a confirmed 390-block cascading
failure**.

Instead, it demonstrates an important limitation of simple temporal
clustering.

A large amount of synchronized normal activity can occur close to genuine
anomalies and become grouped into the same temporal episode.

Episode size therefore acts as a prioritization and investigation signal, not
proof that every member belongs to one underlying failure.

This result is one of the project's most important evaluation findings because
it demonstrates why apparently strong operational patterns must still be
validated against ground truth.

---

# Evaluation Layer

`evaluation/evaluate_detection.py` evaluates deterministic detector output
against the HDFS ground-truth labels.

The evaluation calculates:

```text
True positives
False positives
False negatives
Precision
Recall
F1
```

Evaluation is restricted to ground-truth blocks that were actually observed in
the ingested event data.

This prevents unobserved blocks from incorrectly affecting detector metrics.

## Evaluate the original detector

```bash
python evaluation/evaluate_detection.py --table INCIDENTS
```

## Evaluate the behavioral detector

```bash
python evaluation/evaluate_detection.py --table INCIDENTS_BEHAVIORAL
```

Observed Rule C result:

```text
Precision: 0.2703
Recall:    0.4473
F1:        0.3369
```

---

# Gemini RCA Explanation Layer

The LLM is deliberately placed at the end of the pipeline.

It does not perform anomaly detection.

By the time Gemini receives a prompt, PulseOps has already determined:

- which incident is being examined,
- its assigned severity,
- why the deterministic detector flagged it,
- its behavioral metrics,
- which evidence rows belong to it,
- and, for episode analysis, aggregate episode statistics.

The LLM's job is therefore:

```text
structured evidence
        |
        v
plain-English incident explanation
```

rather than:

```text
raw logs
        |
        v
LLM decides what happened
```

---

## Behavioral Evidence View

The original evidence view was associated with the abandoned static detector.

After behavioral detection became the selected approach, PulseOps introduced:

```text
INCIDENT_EVIDENCE_BEHAVIORAL
```

This ensures that the RCA layer retrieves evidence corresponding to the
detector actually being evaluated.

---

# Single-Block RCA

Example:

```bash
python llm/summarize_incident.py \
  --block-id "blk_-8143081805648596965"
```

The tested block returned four evidence rows.

The generated RCA successfully produced three sections:

```text
Incident summary

Root-cause hypothesis

Recommended next action
```

The prompt explicitly instructs Gemini to:

- use only supplied evidence,
- not decide whether the incident exists,
- not change severity,
- not invent events,
- not invent failures,
- not invent timestamps,
- distinguish evidence from hypotheses,
- and avoid presenting an unproven root cause as fact.

---

# Episode-Level RCA

The same explanation layer can summarize a pre-aggregated episode.

Example:

```bash
python llm/summarize_incident.py \
  --episode-id 66 \
  --save
```

Rather than sending every log belonging to hundreds of blocks to the LLM,
PulseOps supplies:

- episode metadata,
- component breakdown,
- representative evidence,
- severity statistics,
- and ground-truth purity.

For Episode 66, the generated RCA explicitly incorporated the 6.15% purity
finding and warned against interpreting the entire temporal cluster as a
confirmed cascading failure.

This keeps the LLM aligned with the quantitative evaluation performed earlier
in the pipeline.

---

# GCP Deployment

PulseOps was deployed and validated on a Google Cloud Compute Engine VM.

Deployment environment:

```text
Google Compute Engine
Debian 12
e2-standard-4
Docker
Kafka 3.7.0
PySpark 3.5.1
Python
Snowflake
Gemini API
```

The repository was cloned directly from GitHub onto the VM.

The HDFS_v1 dataset was downloaded directly to the VM rather than committed to
the repository.

This keeps the approximately 1.5 GB dataset outside Git while preserving a
reproducible download process through:

```text
data/download_hdfs_dataset.sh
```

---

# GCP Kafka Validation

Kafka was started on the VM and verified as a running Docker container.

The producer successfully replayed HDFS events into:

```text
hdfs-log-events
```

A Kafka console consumer then retrieved the produced messages from the topic,
validating:

```text
HDFS dataset
    |
    v
Python replay producer
    |
    v
Kafka
```

before Spark was introduced into the deployment test.

---

# GCP Snowflake Validation

Snowflake connectivity was tested directly from the Compute Engine VM.

The VM successfully connected to the configured:

- Snowflake account,
- warehouse,
- database,
- schema,
- and role.

A direct `write_pandas()` test was also performed before running the full Spark
pipeline.

The staged write returned:

```text
True
1 chunk
1 row loaded
```

confirming that the VM could perform Snowflake staged uploads successfully.

---

# End-to-End Deployment Validation

A controlled 200-event batch was replayed through the deployed pipeline.

Observed Spark/Snowflake writes included:

```text
RAW_LOG_EVENTS       200 rows written successfully
WINDOWED_METRICS       5 rows written successfully
```

This validated the deployed path:

```text
HDFS_v1
   |
   v
Kafka
   |
   v
PySpark Structured Streaming
   |
   v
regex parsing
   |
   v
event-time processing
   |
   v
window aggregation
   |
   v
Snowflake
```

Detailed deployment evidence is documented in:

[`gcp/DAY12_VALIDATION.md`](gcp/DAY12_VALIDATION.md)

---

# Checkpoint Recovery

A restart-recovery test was performed on the GCP VM.

The test sequence was:

1. Spark processed an initial controlled batch.
2. Spark was stopped.
3. Kafka remained running.
4. Existing Spark checkpoint state was preserved.
5. 100 additional events were published while Spark was offline.
6. Spark was restarted using the same checkpoint locations.
7. The queued Kafka events were automatically consumed and processed.

Recovery output included:

```text
RAW_LOG_EVENTS       100 rows written successfully
WINDOWED_METRICS       4 rows written successfully
```

This demonstrates recovery of Kafka streaming progress following a Spark
processing-service interruption.

For the current MVP, checkpoints are stored locally at:

```text
/tmp/pulseops-checkpoints
```

This is sufficient for demonstrating Spark process restart recovery on the same
VM.

It does **not** provide checkpoint durability if the Compute Engine VM itself is
deleted or replaced.

A production deployment should move Structured Streaming checkpoints to
durable object storage such as Google Cloud Storage.

---

# Setup

## 1. Clone the repository

```bash
git clone https://github.com/Shanaya31/PulseOps.git
cd PulseOps
```

---

## 2. Create a Python virtual environment

Linux:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Windows PowerShell:

```powershell
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## 3. Configure environment variables

Create:

```text
.env
```

using:

```text
.env.example
```

as the template.

Required configuration includes:

```text
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
SNOWFLAKE_ROLE

KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC

GEMINI_API_KEY
GEMINI_MODEL
```

The real `.env` file contains credentials and must never be committed to Git.

---

## 4. Download the HDFS dataset

```bash
bash data/download_hdfs_dataset.sh
```

Expected files:

```text
data/HDFS_v1/HDFS.log
data/HDFS_v1/anomaly_label.csv
```

The dataset itself is excluded from Git.

---

## 5. Start Kafka

For Docker Compose v2:

```bash
docker compose up -d
```

For systems using the standalone Compose command:

```bash
docker-compose up -d
```

Verify:

```bash
docker ps
```

---

## 6. Configure Snowflake

Run the SQL scripts in the `snowflake/` directory in their documented build
order.

The Snowflake layer contains the project's:

- operational tables,
- dimensional model,
- ground-truth labels,
- behavioral feature views,
- derived behavioral metrics,
- behavioral detector,
- severity logic,
- window analytics,
- incident episodes,
- evaluation queries,
- and behavioral evidence views.

---

## 7. Start PySpark Structured Streaming

Example:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  streaming/spark_streaming_job.py
```

Spark remains running and waits for Kafka events.

---

## 8. Replay HDFS events

For a small validation batch:

```bash
python ingestion/replay_producer.py \
  --speed 100000 \
  --limit 200
```

For larger replays, adjust the limit and replay speed as required.

---

## 9. Evaluate the behavioral detector

```bash
python evaluation/evaluate_detection.py \
  --table INCIDENTS_BEHAVIORAL
```

---

## 10. Generate a single-block RCA

```bash
python llm/summarize_incident.py \
  --block-id "blk_-8143081805648596965"
```

Optionally save it:

```bash
python llm/summarize_incident.py \
  --block-id "blk_-8143081805648596965" \
  --save
```

---

## 11. Generate an episode RCA

```bash
python llm/summarize_incident.py \
  --episode-id 66 \
  --save
```

---

# Challenges and Engineering Decisions

## 1. Static threshold detection was ineffective

The initial Spark detector relied on explicit ERROR/WARN thresholds.

Evaluation produced:

```text
Precision: 0.0000
Recall:    0.0000
F1:        0.0000
```

This demonstrated that explicit log severity alone was not sufficient for the
evaluated HDFS workload.

### Solution

Block-level behavioral features were derived in Snowflake and multiple
deterministic behavioral rules were evaluated.

Rule C increased performance to:

```text
Precision: 27.03%
Recall:    44.73%
F1:        0.3369
```

The original detector remains useful as documentation of the project's
iteration rather than being silently removed from its history.

---

## 2. A large incident episode was mostly detector noise

Episode 66 contained 390 incident candidates and initially looked like a large
correlated infrastructure event.

Ground-truth evaluation found:

```text
24 anomaly blocks
366 normal blocks
6.15% purity
```

### Solution

Episode purity became an explicit evaluation metric.

PulseOps therefore does not equate temporal proximity with shared root cause.

Episode construction is used for prioritization and investigation, while
ground-truth validation determines how confidently a cluster can be
interpreted.

---

## 3. Severity tiers contained different signal quality

Overall Rule C precision was 27.03%.

HIGH-severity incidents achieved:

```text
50.00% precision
```

while MEDIUM incidents achieved:

```text
23.20% precision
```

### Solution

Severity is treated as an operational prioritization mechanism.

HIGH alerts can be investigated first while MEDIUM alerts remain available for
secondary analysis.

---

## 4. The evidence layer initially followed the wrong detector

The original:

```text
INCIDENT_EVIDENCE
```

view was associated with the static incident detector.

After the behavioral detector became the selected path, continuing to use the
old evidence view would have made the explanation layer inconsistent with the
evaluated detector.

### Solution

PulseOps introduced:

```text
INCIDENT_EVIDENCE_BEHAVIORAL
```

so that Gemini receives evidence belonging to the actual behavioral incidents.

---

## 5. Gemini SDK and model availability changed

The original implementation used the older:

```text
google-generativeai
```

SDK and older Gemini model names.

During development, model availability changed and the original model calls
stopped working.

### Solution

The explanation layer was migrated to:

```text
google-genai
```

using the newer Gemini client interface.

Because the LLM is isolated behind a thin explanation layer, the SDK migration
did not require redesigning the streaming or analytics pipeline.

---

## 6. Gemini initially returned incomplete RCA output

An early Gemini request terminated because the response reached its output
token limit before producing all required RCA sections.

### Solution

The prompt and generation configuration were adjusted to ensure that the model
returned:

```text
Incident summary
Root-cause hypothesis
Recommended next action
```

The script also validates that all three sections are present before accepting
the generated RCA.

---

## 7. Snowflake staged writes failed during GCP deployment

Direct Snowflake connectivity from the VM succeeded, but staged
`write_pandas()` operations initially encountered certificate-revocation
validation problems.

Independent TLS connectivity to Google Cloud Storage was verified successfully.

### Solution

The stale Snowflake OCSP response cache was cleared.

A direct `write_pandas()` test then completed successfully before the full Spark
stream was restarted.

This separated network connectivity, Snowflake authentication, and staged-write
behavior instead of treating them as one opaque deployment failure.

---

## 8. Local checkpoint storage is an MVP trade-off

Structured Streaming currently uses:

```text
/tmp/pulseops-checkpoints
```

on the Compute Engine VM.

The restart test demonstrated successful Spark process recovery using this
checkpoint state.

However, local checkpoint storage is tied to the VM.

### Production improvement

Move checkpoints to durable object storage such as Google Cloud Storage so
streaming progress survives VM replacement as well as process restarts.

---

# Repository Structure

```text
PulseOps/
|
|-- data/
|   `-- download_hdfs_dataset.sh
|
|-- evaluation/
|   |-- __init__.py
|   `-- evaluate_detection.py
|
|-- gcp/
|   |-- DEPLOY.md
|   `-- DAY12_VALIDATION.md
|
|-- ingestion/
|   |-- __init__.py
|   `-- replay_producer.py
|
|-- llm/
|   |-- __init__.py
|   `-- summarize_incident.py
|
|-- snowflake/
|   |
|   |-- setup_warehouse.sql
|   |     Snowflake warehouse creation and configuration
|   |
|   |-- setup_schema.sql
|   |     Core operational tables and schema objects
|   |
|   |-- setup_star_schema.sql
|   |     Dimensional/star-schema layer
|   |
|   |-- setup_load_ground_truth.sql
|   |     Ground-truth HDFS anomaly-label loading
|   |
|   |-- 01_validate_raw_log_events.sql
|   |     Validate parsed streaming events
|   |
|   |-- 02_validate_windowed_metrics.sql
|   |     Validate Spark-generated window metrics
|   |
|   |-- 03_validate_ground_truth.sql
|   |     Validate loaded ground-truth data
|   |
|   |-- 04_validate_ground_truth_labels.sql
|   |     Inspect label distribution and duplicate blocks
|   |
|   |-- 05_ground_truth_stream_join_analysis.sql
|   |     Join observed streaming blocks with ground truth
|   |
|   |-- 06_anomalous_block_analysis.sql
|   |     Analyze behavior of known anomalous blocks
|   |
|   |-- 07_windowed_ground_truth_analysis.sql
|   |     Compare streaming windows against ground truth
|   |
|   |-- 08_event_count_anomaly_scoring.sql
|   |     Explore event-count-based anomaly scoring
|   |
|   |-- 09_anomaly_threshold_evaluation.sql
|   |     Evaluate candidate deterministic thresholds
|   |
|   |-- 10_block_behavior_feature_analysis.sql
|   |     Explore block-level behavioral features
|   |
|   |-- 11_normal_vs_anomaly_feature_comparison.sql
|   |     Compare feature distributions by ground-truth class
|   |
|   |-- 12_day5_pre_scale_baseline.sql
|   |     Capture baseline before larger streaming validation
|   |
|   |-- 13_day5_100k_scale_validation.sql
|   |     Validate the 100K-event streaming run
|   |
|   |-- 14_day5_checkpoint_recovery_validation.sql
|   |     Validate post-restart checkpoint recovery
|   |
|   |-- 15_day6_component_window_activity.sql
|   |     Analyze component activity across time windows
|   |
|   |-- 16_day6_component_rolling_zscore_analysis.sql
|   |     Explore rolling component-level Z-scores
|   |
|   |-- 17_day7_block_behavior_features.sql
|   |     Build final block-level behavioral feature layer
|   |
|   |-- 18_day8_window_analytics.sql
|   |     Apply RANK, LAG, LEAD, NTILE and temporal analytics
|   |
|   |-- 19_day9_incident_episodes.sql
|   |     Build temporal episodes using gaps-and-islands
|   |
|   |-- 20_day10_detector_evaluation.sql
|   |     Evaluate Rule C and severity-tier performance
|   |
|   |-- 21_day11_behavioral_evidence.sql
|   |     Prepare deterministic evidence for RCA generation
|   |
|   `-- 22_day11_episode_mapping.sql
|         Map behavioral incidents into episode-level evidence
|
|-- streaming/
|   |-- __init__.py
|   |-- log_parser.py
|   `-- spark_streaming_job.py
|
|-- tests/
|   |-- check_log_parser.py
|   `-- check_timestamp_parsing.py
|
|-- docker-compose.yml
|-- requirements.txt
|-- .env.example
|-- .gitignore
`-- README.md
```

Additional Snowflake worksheets used to build the behavioral analytics and
evaluation layers are maintained as SQL scripts in the `snowflake/` directory.

---

# Current Results

PulseOps currently demonstrates:

- replay of real HDFS infrastructure logs,
- Kafka-based event ingestion,
- PySpark Structured Streaming,
- regex-based HDFS parsing,
- event-time processing,
- tumbling-window aggregation,
- Snowflake operational storage,
- dimensional modeling,
- block-level behavioral feature engineering,
- deterministic behavioral anomaly detection,
- ground-truth detector evaluation,
- precision/recall/F1 measurement,
- severity-based incident prioritization,
- Snowflake window-function analytics,
- gaps-and-islands episode construction,
- episode purity validation,
- evidence-grounded single-incident RCA generation,
- evidence-grounded episode RCA generation,
- Google Compute Engine deployment,
- successful Kafka-to-Snowflake execution on GCP,
- and Structured Streaming checkpoint recovery after Spark interruption.

---

# Key Measured Results

| Metric | Result |
|---|---:|
| HDFS log lines | 11,175,629 |
| Ground-truth label rows | 575,062 |
| Evaluated blocks | 7,940 |
| Evaluable anomaly blocks | 313 |
| Rule C flagged blocks | 518 |
| True positives | 140 |
| False positives | 378 |
| False negatives | 173 |
| Rule C precision | 27.03% |
| Rule C recall | 44.73% |
| Rule C F1 | 0.3369 |
| HIGH severity precision | 50.00% |
| MEDIUM severity precision | 23.20% |
| Incident episodes | 66 |
| Episode 66 blocks | 390 |
| Episode 66 purity | 6.15% |

---

# What PulseOps Demonstrates

PulseOps combines several data-engineering concepts in one end-to-end project:

### Streaming engineering

Kafka and PySpark Structured Streaming provide the ingestion and event
processing layer.

### Data warehousing

Snowflake stores operational events and supports dimensional and analytical
models.

### SQL analytics

Behavioral features, detector logic, ranking, temporal comparisons,
gaps-and-islands analysis, and episode construction are implemented
deterministically in SQL.

### Model evaluation

Detector quality is measured against known ground truth rather than inferred
from attractive-looking alert output.

### Cloud deployment

The pipeline was reproduced on a clean Google Compute Engine environment and
validated through controlled event batches.

### Recovery engineering

Structured Streaming checkpoint behavior was explicitly tested through a Spark
restart while Kafka continued accepting events.

### Responsible LLM integration

Gemini explains evidence selected by the pipeline rather than making the
detection decision itself.

---

# What PulseOps Does Not Claim

PulseOps is an **evaluated engineering prototype**, not a production-ready
anomaly detection platform.

In particular:

- Rule C still produces substantial false positives.
- Overall precision is 27.03%.
- Temporal clustering can produce low-purity episodes.
- Local VM checkpoints are not durable against VM replacement.
- The deployment uses a single Kafka broker.
- The system does not currently provide a production monitoring UI.
- Gemini generates hypotheses, not verified root causes.

These limitations are intentionally documented because evaluating where a
pipeline fails is part of the engineering work.

---

# Future Improvements

Potential next steps include:

- improve Rule C using additional behavioral features,
- evaluate component-specific behavioral baselines,
- evaluate operation-specific baselines,
- introduce adaptive rather than fixed episode thresholds,
- investigate clustering methods beyond temporal gaps,
- calibrate severity thresholds using ground truth,
- move Spark checkpoints to Google Cloud Storage,
- introduce idempotent Snowflake micro-batch writes,
- add automated detector-regression tests,
- monitor Kafka consumer lag,
- add structured logging for failed Snowflake writes,
- automate deployment configuration,
- and expose incidents and episodes through a lightweight read-only dashboard.

The LLM layer should remain downstream of deterministic detection and evidence
selection rather than becoming the anomaly detector itself.

---

# Deployment Evidence

Detailed GCP deployment and restart-recovery validation is available here:

[`gcp/DAY12_VALIDATION.md`](gcp/DAY12_VALIDATION.md)

---

# Project Status

PulseOps is complete through:

- behavioral anomaly detection,
- ground-truth evaluation,
- incident episode analysis,
- Gemini RCA generation,
- GCP deployment,
- checkpoint-recovery validation,
- and full Snowflake SQL version control.

All SQL worksheets used to build the Snowflake analytical layer are preserved
in the `snowflake/` directory as numbered scripts together with the setup SQL.

The repository now contains the complete reproducible implementation trail for
the project.