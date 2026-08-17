"""
PulseOps core streaming job.

Reads raw HDFS log lines from Kafka, parses them with regex, computes a
tumbling-window event count per block_id, applies a DETERMINISTIC threshold
rule to flag incidents, and writes three curated outputs to Snowflake:

  RAW_LOG_EVENTS   - every parsed line (for evidence lookups later)
  WINDOWED_METRICS - per-block, per-window event/error counts
  INCIDENTS        - blocks whose window crossed the anomaly threshold

No LLM, no external API calls happen anywhere in this file. This is the
piece that has to be rock solid; everything else depends on it.

Run with:
    spark-submit streaming/spark_streaming_job.py
"""
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, regexp_extract, concat, to_timestamp, window, count, sum as spark_sum,
    when, current_timestamp,
)

from log_parser import (
    LINE_PATTERN, GROUP_DATE, GROUP_TIME, GROUP_PID, GROUP_LEVEL,
    GROUP_COMPONENT, GROUP_MESSAGE, BLOCK_ID_PATTERN, TIMESTAMP_FORMAT,
)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "hdfs-log-events")
CHECKPOINT_BASE = os.getenv("GCS_CHECKPOINT_PATH", "/tmp/pulseops-checkpoints")

WINDOW_DURATION = "1 minute"
WATERMARK_DELAY = "2 minutes"

# Deterministic anomaly rule: a block is flagged incident if, within a single
# window, it has ANY error-level line, or WARN_COUNT crosses this threshold.
WARN_COUNT_THRESHOLD = 3


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("pulseops-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )


def parse_raw_lines(kafka_df: DataFrame) -> DataFrame:
    raw = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    raw = raw.selectExpr("get_json_object(json_str, '$.raw') as raw_line")

    parsed = raw.select(
        col("raw_line"),
        regexp_extract("raw_line", LINE_PATTERN, GROUP_DATE).alias("log_date"),
        regexp_extract("raw_line", LINE_PATTERN, GROUP_TIME).alias("log_time"),
        regexp_extract("raw_line", LINE_PATTERN, GROUP_PID).alias("pid"),
        regexp_extract("raw_line", LINE_PATTERN, GROUP_LEVEL).alias("level"),
        regexp_extract("raw_line", LINE_PATTERN, GROUP_COMPONENT).alias("component"),
        regexp_extract("raw_line", LINE_PATTERN, GROUP_MESSAGE).alias("message"),
        regexp_extract("raw_line", BLOCK_ID_PATTERN, 1).alias("block_id"),
    )

    parsed = parsed.withColumn(
        "event_time",
        to_timestamp(concat(col("log_date"), col("log_time")), TIMESTAMP_FORMAT),
    ).filter(col("event_time").isNotNull() & (col("block_id") != ""))

    return parsed


def build_windowed_metrics(parsed: DataFrame) -> DataFrame:
    watermarked = parsed.withWatermark("event_time", WATERMARK_DELAY)

    return (
        watermarked
        .groupBy(window(col("event_time"), WINDOW_DURATION), col("block_id"))
        .agg(
            count("*").alias("event_count"),
            spark_sum(when(col("level") == "ERROR", 1).otherwise(0)).alias("error_count"),
            spark_sum(when(col("level") == "WARN", 1).otherwise(0)).alias("warn_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("block_id"),
            col("event_count"),
            col("error_count"),
            col("warn_count"),
        )
    )


def flag_incidents(windowed: DataFrame) -> DataFrame:
    return (
        windowed
        .withColumn(
            "is_incident",
            (col("error_count") > 0) | (col("warn_count") >= WARN_COUNT_THRESHOLD),
        )
        .filter(col("is_incident"))
        .withColumn("detected_at", current_timestamp())
    )


def write_to_snowflake(pdf, table_name: str):
    """Writes a pandas DataFrame micro-batch to a Snowflake table."""
    if pdf.empty:
        return

    import pandas as pd
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    # Convert pandas datetime columns to strings before Snowflake upload.
    # This avoids nanosecond epoch interpretation issues.
    for column in pdf.columns:
        if pd.api.types.is_datetime64_any_dtype(pdf[column]):
            pdf[column] = (
                pd.to_datetime(pdf[column], errors="coerce")
                .dt.strftime("%Y-%m-%d %H:%M:%S.%f")
            )

    # Snowflake tables use unquoted uppercase column names.
    # Normalize Spark/Pandas column names before write_pandas.
    pdf.columns = [str(col).upper() for col in pdf.columns]

    print(f"[SNOWFLAKE] Writing {len(pdf)} rows to {table_name}")
    print(f"[SNOWFLAKE] Columns: {list(pdf.columns)}")

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )

    try:
        success, nchunks, nrows, output = write_pandas(
            conn,
            pdf,
            table_name.upper()
        )

        print(
            f"[SNOWFLAKE] table={table_name.upper()} "
            f"success={success} rows={nrows}"
        )

    finally:
        conn.close()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_kafka_stream(spark)
    parsed = parse_raw_lines(kafka_df)
    windowed = build_windowed_metrics(parsed)
    incidents = flag_incidents(windowed)

    def sink_raw_events(batch_df, batch_id):
        write_to_snowflake(batch_df.toPandas(), "RAW_LOG_EVENTS")

    def sink_windowed(batch_df, batch_id):
        write_to_snowflake(batch_df.toPandas(), "WINDOWED_METRICS")

    def sink_incidents(batch_df, batch_id):
        write_to_snowflake(batch_df.toPandas(), "INCIDENTS")

    q1 = (
        parsed.writeStream
        .foreachBatch(sink_raw_events)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw_events")
        .trigger(processingTime="30 seconds")
        .start()
    )

    q2 = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(sink_windowed)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/windowed_metrics")
        .trigger(processingTime="30 seconds")
        .start()
    )

    q3 = (
        incidents.writeStream
        .outputMode("update")
        .foreachBatch(sink_incidents)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/incidents")
        .trigger(processingTime="30 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
