# PulseOps — GCP Deployment Validation

## Objective

Validate that the PulseOps streaming pipeline can run on a Google Cloud
Compute Engine VM, communicate with Snowflake, process Kafka events using
PySpark Structured Streaming, and recover after a processing interruption.

## Deployment Environment

- Google Cloud Compute Engine
- Debian 12
- e2-standard-4 VM
- Apache Kafka 3.7.0
- Apache Spark / PySpark Structured Streaming
- Python virtual environment
- Snowflake
- Gemini RCA layer

## Deployed Architecture

HDFS_v1 Dataset
→ Kafka
→ PySpark Structured Streaming
→ RAW_LOG_EVENTS
→ WINDOWED_METRICS
→ Snowflake behavioral incident detection
→ Gemini RCA summarization

## 1. Kafka Validation

Apache Kafka was deployed using Docker on the Compute Engine VM.

The HDFS_v1 dataset was downloaded directly to the VM.

Dataset size:

- HDFS.log: approximately 1.5 GB
- Log records: 11,175,629
- anomaly_label.csv: approximately 18 MB
- Ground-truth rows: 575,062

The replay producer successfully published HDFS log events to the
`hdfs-log-events` Kafka topic.

A Kafka console consumer was used to verify that the events were available
from the broker.

## 2. Snowflake Connectivity

The GCP VM successfully connected directly to Snowflake using the
Snowflake Python connector.

The connection returned the expected PulseOps database, schema, warehouse,
and role.

A direct `write_pandas()` test successfully staged and loaded a test row
into `RAW_LOG_EVENTS`.

## 3. Streaming Pipeline Validation

The PySpark Structured Streaming application was started on the GCP VM.

A controlled batch of 200 HDFS events was published to Kafka.

Results:

- RAW_LOG_EVENTS: 200 rows written successfully
- WINDOWED_METRICS: 5 rows written successfully

This validated the following path:

Kafka
→ Spark Structured Streaming
→ parsing
→ event-time window aggregation
→ Snowflake

## 4. Checkpoint Recovery Test

A failure-recovery test was performed to validate Spark Structured
Streaming checkpoint behavior.

Procedure:

1. The streaming application processed an initial Kafka batch.
2. Spark was stopped while Kafka remained available.
3. Existing Spark checkpoint directories were preserved.
4. 100 additional events were published while Spark was offline.
5. The same streaming application was restarted.
6. Spark restored its streaming state from the existing checkpoints.
7. The queued Kafka records were processed after restart.

Recovery results:

- RAW_LOG_EVENTS: 100 rows written successfully
- WINDOWED_METRICS: 4 rows written successfully

This demonstrates that PulseOps can resume processing Kafka data after
a streaming-service interruption without requiring the producer to replay
the interrupted batch manually.

## 5. Behavioral Detection

The Snowflake behavioral incident detector was evaluated against the
ground-truth HDFS anomaly labels.

Evaluation output:

- Blocks seen: 7,940
- Blocks flagged by detector: 518
- Evaluable anomaly blocks: 313
- True positives: 140
- False positives: 378
- False negatives: 173
- Precision: 0.2703
- Recall: 0.4473
- F1 score: 0.3369

These metrics are recorded as evaluation results rather than hidden or
presented as production-grade detection performance.

## 6. Gemini RCA Validation

PulseOps successfully retrieved behavioral incident evidence from
Snowflake on the GCP VM and passed the computed evidence to Gemini.

Gemini generated:

- an incident summary,
- a root-cause hypothesis,
- and a recommended next action.

The LLM is used only as the final explanatory layer. Incident detection
itself remains deterministic and data-driven.

## Validation Status

GCP deployment: PASS

Kafka ingestion: PASS

PySpark Structured Streaming: PASS

Snowflake ingestion: PASS

Checkpoint recovery: PASS

Behavioral detector evaluation: PASS

Gemini RCA generation: PASS

The core PulseOps cloud deployment and recovery workflow was successfully
validated on Google Cloud.