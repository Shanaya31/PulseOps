# PulseOps Days 1–2 execution guide

## Before starting

Use the repository root as your working directory. On Windows, run the shell script in Git Bash or WSL. Docker Desktop must be running.

Create and activate a virtual environment:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Verify imports:

```powershell
python -c "import kafka, pandas, pyarrow, pyspark, snowflake.connector; print('Dependencies OK')"
```

## Day 1A: download and inspect HDFS_v1

From Git Bash or WSL:

```bash
bash data/download_hdfs_dataset.sh
wc -l data/HDFS_v1/HDFS.log
wc -l data/HDFS_v1/anomaly_label.csv
head -n 5 data/HDFS_v1/HDFS.log
head -n 5 data/HDFS_v1/anomaly_label.csv
```

PowerShell equivalents after the download:

```powershell
(Get-Content data\HDFS_v1\HDFS.log | Measure-Object -Line).Lines
(Get-Content data\HDFS_v1\anomaly_label.csv | Measure-Object -Line).Lines
Get-Content data\HDFS_v1\HDFS.log -TotalCount 5
Get-Content data\HDFS_v1\anomaly_label.csv -TotalCount 5
```

Do not commit `HDFS_v1.zip`, `HDFS.log`, or credentials.

## Day 1B: start and test Kafka

```powershell
docker compose up -d
docker compose ps
docker logs pulseops-kafka --tail 50
```

Create the topic explicitly:

```powershell
docker exec pulseops-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic hdfs-log-events --partitions 1 --replication-factor 1
docker exec pulseops-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic hdfs-log-events
```

Terminal 1, consumer:

```powershell
docker exec -it pulseops-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hdfs-log-events --from-beginning
```

Terminal 2, producer:

```powershell
docker exec -it pulseops-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hdfs-log-events
```

Type one line and press Enter. It must appear in Terminal 1. Stop both commands with Ctrl+C.

## Day 1C: configure environment

```powershell
Copy-Item .env.example .env
```

Fill in the Snowflake values. Keep this for local work:

```dotenv
GCS_CHECKPOINT_PATH=/tmp/pulseops-checkpoints
```

Do not commit `.env`.

## Day 1D: create Snowflake objects

Run `snowflake/schema.sql` in a Snowflake worksheet from top to bottom.

Then confirm the context and tables:

```sql
SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
SHOW TABLES IN SCHEMA PULSEOPS.CORE;
```

## Day 1E: load ground-truth labels

In Snowsight, create an internal named stage so the upload is easy:

```sql
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

CREATE STAGE IF NOT EXISTS PULSEOPS_UPLOAD_STAGE;
```

Open **Data → Add Data → Load files into a Stage**, choose `PULSEOPS_UPLOAD_STAGE`, and upload `data/HDFS_v1/anomaly_label.csv`.

Then run:

```sql
TRUNCATE TABLE GROUND_TRUTH_LABELS;

COPY INTO GROUND_TRUTH_LABELS (BLOCK_ID, LABEL)
FROM (
    SELECT $1::STRING, $2::STRING
    FROM @PULSEOPS_UPLOAD_STAGE/anomaly_label.csv
)
FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
)
ON_ERROR = 'ABORT_STATEMENT';
```

Run `snowflake/day1_validation.sql`. The validation query must report zero blank block IDs and zero unexpected labels.

## Day 2: reconcile and validate the star schema

`schema.sql` is now the only DDL source. `star_schema.sql` contains only repeatable MERGE statements.

At this stage, the operational tables are still empty except for `GROUND_TRUTH_LABELS`, so run:

```sql
-- First rerun schema.sql to ensure the final table definitions exist.
-- Then run star_schema.sql.
```

Expected result on Day 2:

- `DIM_BLOCK` is populated from ground truth labels.
- `DIM_DATE`, `DIM_COMPONENT`, and `FACT_INCIDENT_EVENTS` remain empty until streaming data exists.
- Rerunning `star_schema.sql` does not duplicate rows.

Validate:

```sql
SELECT COUNT(*) AS LABEL_ROWS FROM GROUND_TRUTH_LABELS;
SELECT COUNT(*) AS DIM_BLOCK_ROWS FROM DIM_BLOCK;

SELECT COUNT(*) AS DUPLICATE_BLOCK_IDS
FROM (
    SELECT BLOCK_ID
    FROM DIM_BLOCK
    GROUP BY BLOCK_ID
    HAVING COUNT(*) > 1
);

SELECT GROUND_TRUTH_LABEL, COUNT(*)
FROM DIM_BLOCK
GROUP BY GROUND_TRUTH_LABEL
ORDER BY GROUND_TRUTH_LABEL;
```

`DUPLICATE_BLOCK_IDS` must equal `0`. Label counts in `DIM_BLOCK` should match `GROUND_TRUTH_LABELS` after accounting for any duplicate source rows.

## Definition of done

Day 1 is complete when the dataset exists, dependencies import, Kafka passes a manual message test, Snowflake objects exist, and labels validate.

Day 2 is complete when `schema.sql` is the sole DDL file, `star_schema.sql` reruns cleanly, `DIM_BLOCK` contains the labels, and no duplicate block IDs exist.
