"""
Compares blocks flagged by a PulseOps detector table/view against
Loghub's human-labeled ground truth (GROUND_TRUTH_LABELS table) and reports
precision, recall, and F1.

This allows us to evaluate both:
- INCIDENTS              -> original static ERROR/WARN detector
- INCIDENTS_BEHAVIORAL   -> behavioral Rule C detector

Usage:
    python evaluation/evaluate_detection.py --table INCIDENTS
    python evaluation/evaluate_detection.py --table INCIDENTS_BEHAVIORAL
"""

import os
import argparse

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Evaluate a PulseOps anomaly detector against ground truth."
    )

    parser.add_argument(
        "--table",
        default="INCIDENTS_BEHAVIORAL",
        choices=["INCIDENTS", "INCIDENTS_BEHAVIORAL"],
        help="Detector table/view to evaluate.",
    )

    args = parser.parse_args()
    detector_table = args.table

    conn = get_connection()
    cur = conn.cursor()

    # Blocks actually flagged by the selected detector
    cur.execute(f"SELECT DISTINCT BLOCK_ID FROM {detector_table}")
    detected = {row[0] for row in cur.fetchall() if row[0] is not None}

    # IMPORTANT:
    # Only evaluate anomalous blocks that actually appeared in RAW_LOG_EVENTS.
    #
    # The full ground-truth dataset contains many blocks that were never replayed
    # into the current PulseOps run. Counting those as false negatives would make
    # the detector look artificially worse.
    cur.execute("""
        SELECT DISTINCT g.BLOCK_ID
        FROM GROUND_TRUTH_LABELS g
        JOIN (
            SELECT DISTINCT BLOCK_ID
            FROM RAW_LOG_EVENTS
            WHERE BLOCK_ID IS NOT NULL
        ) r
            ON g.BLOCK_ID = r.BLOCK_ID
        WHERE g.LABEL = 'Anomaly'
    """)

    actual_anomalies = {row[0] for row in cur.fetchall()}

    cur.execute("""
        SELECT COUNT(DISTINCT BLOCK_ID)
        FROM RAW_LOG_EVENTS
        WHERE BLOCK_ID IS NOT NULL
    """)
    total_blocks_seen = cur.fetchone()[0]

    cur.close()
    conn.close()

    true_positives = detected & actual_anomalies
    false_positives = detected - actual_anomalies
    false_negatives = actual_anomalies - detected

    precision = (
        len(true_positives) / len(detected)
        if detected
        else 0.0
    )

    recall = (
        len(true_positives) / len(actual_anomalies)
        if actual_anomalies
        else 0.0
    )

    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall)
        else 0.0
    )

    print("=== PulseOps Detection Evaluation ===")
    print(f"Detector table:              {detector_table}")
    print(f"Blocks seen:                 {total_blocks_seen}")
    print(f"Blocks flagged by detector:  {len(detected)}")
    print(f"Evaluable anomaly blocks:    {len(actual_anomalies)}")
    print(f"True positives:              {len(true_positives)}")
    print(f"False positives:             {len(false_positives)}")
    print(f"False negatives:             {len(false_negatives)}")
    print(f"Precision:                   {precision:.4f}")
    print(f"Recall:                      {recall:.4f}")
    print(f"F1:                          {f1:.4f}")
    print()
    print(
        "Evaluation is restricted to ground-truth blocks that were actually "
        "observed in RAW_LOG_EVENTS."
    )


if __name__ == "__main__":
    main()