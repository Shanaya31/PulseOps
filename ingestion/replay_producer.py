"""
Replays the raw HDFS_v1 log file into a Kafka topic, preserving original
event order and (compressed) real-world timing, so downstream consumers see
something resembling a live production log stream.

Deliberately sends RAW lines, not pre-parsed fields — parsing happens in the
Spark job. That keeps the "real" data engineering work (regex/schema
extraction, windowing) in Spark, where it belongs for this project's story.

Usage:
    python replay_producer.py --speed 60 --limit 200000
"""
import argparse
import os
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer
import json

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "hdfs-log-events")

# HDFS.log lines start with a 6-digit date and 6-digit time, e.g. "081109 203615 ..."
def parse_line_timestamp(line: str) -> datetime | None:
    try:
        date_part, time_part = line.split(" ", 2)[:2]
        return datetime.strptime(date_part + time_part, "%y%m%d%H%M%S")
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", default="data/HDFS_v1/HDFS.log")
    parser.add_argument("--speed", type=float, default=60.0,
                         help="Playback speed multiplier (60 = 1 real hour in 1 minute)")
    parser.add_argument("--limit", type=int, default=None,
                         help="Max number of lines to replay (omit for full file)")
    parser.add_argument("--max-sleep", type=float, default=2.0,
                         help="Cap on sleep between lines, in seconds, to avoid long stalls")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=20,
    )

    sent = 0
    prev_ts = None

    with open(args.file, "r", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            ts = parse_line_timestamp(line)
            if prev_ts is not None and ts is not None:
                delta_seconds = (ts - prev_ts).total_seconds()
                if delta_seconds > 0:
                    sleep_for = min(delta_seconds / args.speed, args.max_sleep)
                    if sleep_for > 0:
                        time.sleep(sleep_for)
            prev_ts = ts if ts is not None else prev_ts

            producer.send(TOPIC, {
                "raw": line,
                "ingest_ts": datetime.utcnow().isoformat(),
            })
            sent += 1

            if sent % 5000 == 0:
                print(f"  replayed {sent} lines...")

            if args.limit and sent >= args.limit:
                break

    producer.flush()
    print(f"Done. Replayed {sent} lines to topic '{TOPIC}'.")


if __name__ == "__main__":
    main()
