"""
PulseOps explanation layer for behavioral incidents and incident episodes.

Supports:

1. Single behavioral incident RCA
2. Episode-level RCA for temporally clustered incidents

The LLM makes no detection or clustering decisions. Incident detection,
severity assignment, and episode membership are already determined by the
deterministic PulseOps pipeline.

Examples:

    python llm/summarize_incident.py \
        --block-id "blk_-8143081805648596965"

    python llm/summarize_incident.py \
        --block-id "blk_-8143081805648596965" \
        --save

    python llm/summarize_incident.py \
        --episode-id 66

    python llm/summarize_incident.py \
        --episode-id 66 \
        --save
"""

import argparse
import os

from dotenv import load_dotenv
import snowflake.connector
from google import genai
from google.genai import types


load_dotenv()


# ---------------------------------------------------------
# Gemini configuration
# ---------------------------------------------------------

MODEL_NAME = os.getenv(
    "GEMINI_MODEL",
    "gemini-3.5-flash",
)


def get_gemini_client():
    """
    Create a Gemini client using GEMINI_API_KEY from .env.
    """

    api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        raise ValueError(
            "GEMINI_API_KEY was not found. "
            "Add it to your .env file."
        )

    return genai.Client(api_key=api_key)


# ---------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------

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


# =========================================================
# SINGLE-BLOCK MODE
# =========================================================

# ---------------------------------------------------------
# Fetch behavioral evidence
# ---------------------------------------------------------

def fetch_evidence(block_id: str):

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                SEVERITY,
                DETECTION_REASON,
                EVENTS_PER_SECOND,
                COMPONENT_DIVERSITY,
                LEVEL,
                COMPONENT,
                MESSAGE,
                EVENT_TIME
            FROM INCIDENT_EVIDENCE_BEHAVIORAL
            WHERE BLOCK_ID = %s
            ORDER BY EVENT_TIME
            """,
            (block_id,),
        )

        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return rows


# ---------------------------------------------------------
# Utility formatting
# ---------------------------------------------------------

def format_metric(value, fallback="unavailable"):

    if value is None:
        return fallback

    return str(value)


# ---------------------------------------------------------
# Single-block prompt
# ---------------------------------------------------------

def build_prompt(block_id: str, rows) -> str:

    if not rows:
        raise ValueError(
            f"No behavioral evidence found in Snowflake "
            f"for block_id={block_id}"
        )

    severity = rows[0][0]
    detection_reason = rows[0][1]
    events_per_second = rows[0][2]
    component_diversity = rows[0][3]

    event_times = [
        row[7]
        for row in rows
        if row[7] is not None
    ]

    first_event = (
        min(event_times)
        if event_times
        else "unknown"
    )

    last_event = (
        max(event_times)
        if event_times
        else "unknown"
    )

    log_lines = "\n".join(
        f"[{row[7]}] {row[4]} {row[5]}: {row[6]}"
        for row in rows
    )

    eps_text = format_metric(
        events_per_second,
        "unavailable (zero-duration observation)",
    )

    diversity_text = format_metric(
        component_diversity
    )

    return f"""
You are summarizing an already-detected infrastructure incident
for an on-call engineer.

IMPORTANT CONSTRAINTS:

- All facts below were produced by a deterministic detection pipeline.
- Do not decide whether this is an incident.
- Do not change the assigned severity.
- Do not invent events, failures, causes, components, IP addresses,
  timestamps, or operational conditions.
- Distinguish observed evidence from hypotheses.
- A root cause may only be described as a hypothesis unless the supplied
  evidence explicitly proves it.
- Base the explanation ONLY on the evidence supplied below.
- Do not merely repeat or quote the raw log lines.
- Do not call tools or functions.
- Produce a complete response, not a fragment.

Incident facts
--------------
Block ID: {block_id}
Severity: {severity}
Detection reason: {detection_reason}
First event: {first_event}
Last event: {last_event}
Events per second: {eps_text}
Component diversity: {diversity_text}
Evidence rows: {len(rows)}

Raw log evidence
----------------
{log_lines}

Required output format
----------------------

Incident summary:
Write 2-3 concise sentences describing what was observed.

Root-cause hypothesis:
Write 2-3 concise sentences grounded only in the supplied evidence.
Clearly identify uncertainty where the evidence does not prove a cause.

Recommended next action:
Provide one concrete investigation or operational step.

Return ONLY these three sections as plain text.
Do not repeat the raw evidence verbatim.
"""


# =========================================================
# EPISODE MODE
# =========================================================

# ---------------------------------------------------------
# Fetch episode metadata
# ---------------------------------------------------------

def fetch_episode_metadata(episode_id: int):

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                EPISODE_ID,
                EPISODE_START,
                EPISODE_END,
                EPISODE_DURATION_SECONDS,
                N_INCIDENTS,
                N_DISTINCT_BLOCKS,
                N_HIGH_SEVERITY,
                N_MEDIUM_SEVERITY,
                TOTAL_EVENTS,
                AVG_EVENTS_PER_SECOND,
                MAX_EVENTS_PER_SECOND,
                AVG_COMPONENT_DIVERSITY
            FROM INCIDENT_EPISODES
            WHERE EPISODE_ID = %s
            """,
            (episode_id,),
        )

        row = cur.fetchone()

    finally:
        cur.close()
        conn.close()

    return row


# ---------------------------------------------------------
# Fetch component / log-level breakdown
# ---------------------------------------------------------

def fetch_episode_component_breakdown(episode_id: int):

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                e.COMPONENT,
                e.LEVEL,
                COUNT(*) AS EVENT_COUNT
            FROM INCIDENT_EVIDENCE_BEHAVIORAL e
            JOIN INCIDENT_EPISODE_MEMBERS m
                ON e.BLOCK_ID = m.BLOCK_ID
            WHERE m.EPISODE_ID = %s
            GROUP BY
                e.COMPONENT,
                e.LEVEL
            ORDER BY
                EVENT_COUNT DESC,
                e.COMPONENT,
                e.LEVEL
            """,
            (episode_id,),
        )

        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return rows


# ---------------------------------------------------------
# Fetch deterministic evidence sample
# ---------------------------------------------------------

def fetch_episode_sample(episode_id: int, sample_size: int = 25):

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                e.BLOCK_ID,
                e.EVENT_TIME,
                e.SEVERITY,
                e.LEVEL,
                e.COMPONENT,
                e.MESSAGE
            FROM INCIDENT_EVIDENCE_BEHAVIORAL e
            JOIN INCIDENT_EPISODE_MEMBERS m
                ON e.BLOCK_ID = m.BLOCK_ID
            WHERE m.EPISODE_ID = %s
            ORDER BY HASH(
                e.BLOCK_ID,
                e.EVENT_TIME,
                e.COMPONENT,
                e.MESSAGE
            )
            LIMIT %s
            """,
            (
                episode_id,
                sample_size,
            ),
        )

        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return rows


# ---------------------------------------------------------
# Episode ground-truth purity
# ---------------------------------------------------------

def fetch_episode_purity(episode_id: int):

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT
                COUNT(*) AS LABELED_BLOCKS,

                COUNT_IF(
                    g.LABEL = 'Anomaly'
                ) AS TRUE_ANOMALY_BLOCKS,

                COUNT_IF(
                    g.LABEL = 'Normal'
                ) AS NORMAL_BLOCKS,

                ROUND(
                    COUNT_IF(
                        g.LABEL = 'Anomaly'
                    ) * 100.0
                    / NULLIF(COUNT(*), 0),
                    2
                ) AS PURITY_PCT

            FROM INCIDENT_EPISODE_MEMBERS m

            JOIN GROUND_TRUTH_LABELS g
                ON m.BLOCK_ID = g.BLOCK_ID

            WHERE m.EPISODE_ID = %s
            """,
            (episode_id,),
        )

        row = cur.fetchone()

    finally:
        cur.close()
        conn.close()

    return row


# ---------------------------------------------------------
# Episode prompt
# ---------------------------------------------------------

def build_episode_prompt(
    episode_id: int,
    metadata,
    component_breakdown,
    sample_rows,
    purity,
) -> str:

    if metadata is None:
        raise ValueError(
            f"No episode metadata found for episode_id={episode_id}"
        )

    (
        _episode_id,
        episode_start,
        episode_end,
        episode_duration_seconds,
        n_incidents,
        n_distinct_blocks,
        n_high_severity,
        n_medium_severity,
        total_events,
        avg_events_per_second,
        max_events_per_second,
        avg_component_diversity,
    ) = metadata

    (
        labeled_blocks,
        true_anomaly_blocks,
        normal_blocks,
        purity_pct,
    ) = purity

    component_text = "\n".join(
        (
            f"{component} | "
            f"{level} | "
            f"{event_count} events"
        )
        for component, level, event_count
        in component_breakdown
    )

    if not component_text:
        component_text = "No component breakdown available."

    sample_text = "\n".join(
        (
            f"[{event_time}] "
            f"Block={block_id} | "
            f"Severity={severity} | "
            f"{level} | "
            f"{component}: "
            f"{message}"
        )
        for (
            block_id,
            event_time,
            severity,
            level,
            component,
            message,
        )
        in sample_rows
    )

    if not sample_text:
        sample_text = "No sample evidence available."

    return f"""
You are summarizing a temporally clustered group of already-detected
PulseOps behavioral incidents for an on-call engineer.

The cluster is called an incident episode.

IMPORTANT CONSTRAINTS:

- Detection, severity assignment, and episode clustering were performed by
  deterministic code before this prompt was created.
- Do not decide whether individual blocks are anomalous.
- Do not change the assigned severity values.
- Do not assume that temporal clustering proves a shared root cause.
- Do not describe the episode as a confirmed cascading failure unless the
  supplied evidence proves that.
- Ground-truth purity MUST be considered when interpreting this cluster.
- A low purity value means many behaviorally flagged blocks were actually
  labeled Normal in the ground-truth dataset.
- Distinguish clearly between:
    1. observed facts,
    2. behavioral-detector output,
    3. ground-truth evaluation,
    4. hypotheses.
- Do not invent failures, components, hosts, IP addresses, causes,
  timestamps, or operational conditions.
- Base the narrative ONLY on the supplied evidence.
- The raw evidence below is a SAMPLE, not the complete log population.
- Do not claim that the sample proves something about every block.
- Do not call tools or functions.
- Produce a complete response.

Episode facts
-------------
Episode ID: {episode_id}
Episode start: {episode_start}
Episode end: {episode_end}
Episode duration seconds: {episode_duration_seconds}
Behavioral incidents: {n_incidents}
Distinct blocks: {n_distinct_blocks}
HIGH severity incidents: {n_high_severity}
MEDIUM severity incidents: {n_medium_severity}
Total events represented: {total_events}
Average events per second: {avg_events_per_second}
Maximum events per second: {max_events_per_second}
Average component diversity: {avg_component_diversity}

Ground-truth evaluation
-----------------------
Labeled blocks in episode: {labeled_blocks}
Ground-truth anomaly blocks: {true_anomaly_blocks}
Ground-truth normal blocks: {normal_blocks}
Episode purity: {purity_pct}%

Interpretation constraint:

An episode purity of {purity_pct}% means that only
{true_anomaly_blocks} of the {labeled_blocks} evaluated blocks were
ground-truth anomalies.

Therefore, if purity is low, explicitly state that this temporal cluster
contains substantial detector noise and MUST NOT be presented as a confirmed
390-block cascading failure.

Component and level breakdown
-----------------------------
{component_text}

Representative evidence sample
------------------------------
{sample_text}

Required output format
----------------------

Incident summary:
Write 3-4 concise sentences describing the episode as an observed behavioral
cluster. Include the scale, severity distribution, time span, and the
ground-truth purity result. Do not overstate causality.

Root-cause hypothesis:
Write 2-4 concise sentences describing only hypotheses supported by the
component breakdown and representative evidence. Explicitly state if the
evidence is insufficient to establish one shared root cause.

Recommended next action:
Provide one concrete investigation or operational step that would help
separate genuine correlated anomalies from synchronized normal activity or
behavioral-detector false positives.

Return ONLY these three sections as plain text.
"""


# =========================================================
# GEMINI GENERATION
# =========================================================

def generate_summary(prompt: str) -> str:

    client = get_gemini_client()

    response = client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_level="minimal"
            ),
            temperature=0.2,
            max_output_tokens=3000,
        ),
    )

    if not response.candidates:
        raise RuntimeError(
            "Gemini returned no response candidates."
        )

    candidate = response.candidates[0]

    print(
        f"Gemini finish reason: "
        f"{candidate.finish_reason}"
    )

    if response.text is None or not response.text.strip():
        raise RuntimeError(
            "Gemini returned an empty text response."
        )

    summary = response.text.strip()

    required_sections = [
        "Incident summary",
        "Root-cause hypothesis",
        "Recommended next action",
    ]

    missing_sections = [
        section
        for section in required_sections
        if section.lower() not in summary.lower()
    ]

    if missing_sections:
        raise RuntimeError(
            "Gemini returned an incomplete RCA. "
            f"Missing sections: "
            f"{', '.join(missing_sections)}\n\n"
            f"Raw Gemini output:\n{summary}"
        )

    return summary


# =========================================================
# SAVE HELPERS
# =========================================================

def save_block_summary(
    block_id: str,
    rows,
    summary: str,
):

    safe_block_id = (
        block_id
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
    )

    filename = (
        f"incident_{safe_block_id}.txt"
    )

    with open(
        filename,
        "w",
        encoding="utf-8",
    ) as f:

        f.write(
            "PulseOps Behavioral Incident RCA\n"
        )

        f.write(
            "================================\n\n"
        )

        f.write(
            f"Block ID: {block_id}\n"
        )

        f.write(
            f"Gemini model: {MODEL_NAME}\n"
        )

        f.write(
            f"Evidence rows: {len(rows)}\n\n"
        )

        f.write(summary)

        f.write("\n")

    return filename


def save_episode_summary(
    episode_id: int,
    metadata,
    purity,
    summary: str,
):

    filename = (
        f"episode_{episode_id}_rca.txt"
    )

    (
        _episode_id,
        episode_start,
        episode_end,
        episode_duration_seconds,
        n_incidents,
        n_distinct_blocks,
        n_high_severity,
        n_medium_severity,
        total_events,
        avg_events_per_second,
        max_events_per_second,
        avg_component_diversity,
    ) = metadata

    (
        labeled_blocks,
        true_anomaly_blocks,
        normal_blocks,
        purity_pct,
    ) = purity

    with open(
        filename,
        "w",
        encoding="utf-8",
    ) as f:

        f.write(
            "PulseOps Behavioral Episode RCA\n"
        )

        f.write(
            "===============================\n\n"
        )

        f.write(
            f"Episode ID: {episode_id}\n"
        )

        f.write(
            f"Gemini model: {MODEL_NAME}\n"
        )

        f.write(
            f"Episode start: {episode_start}\n"
        )

        f.write(
            f"Episode end: {episode_end}\n"
        )

        f.write(
            f"Duration seconds: "
            f"{episode_duration_seconds}\n"
        )

        f.write(
            f"Incidents: {n_incidents}\n"
        )

        f.write(
            f"Distinct blocks: "
            f"{n_distinct_blocks}\n"
        )

        f.write(
            f"HIGH severity: "
            f"{n_high_severity}\n"
        )

        f.write(
            f"MEDIUM severity: "
            f"{n_medium_severity}\n"
        )

        f.write(
            f"Total events: "
            f"{total_events}\n"
        )

        f.write(
            f"Ground-truth anomaly blocks: "
            f"{true_anomaly_blocks}\n"
        )

        f.write(
            f"Ground-truth normal blocks: "
            f"{normal_blocks}\n"
        )

        f.write(
            f"Episode purity: "
            f"{purity_pct}%\n\n"
        )

        f.write(summary)

        f.write("\n")

    return filename


# =========================================================
# MAIN CLI
# =========================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Generate RCA-style explanations for PulseOps "
            "behavioral incidents or incident episodes."
        )
    )

    mode = parser.add_mutually_exclusive_group(
        required=True
    )

    mode.add_argument(
        "--block-id",
        help="Behavioral incident block ID",
    )

    mode.add_argument(
        "--episode-id",
        type=int,
        help="Incident episode ID",
    )

    parser.add_argument(
        "--save",
        action="store_true",
        help="Save the generated summary to a text file",
    )

    args = parser.parse_args()

    # =====================================================
    # SINGLE BLOCK
    # =====================================================

    if args.block_id:

        print(
            "Fetching behavioral incident evidence..."
        )

        rows = fetch_evidence(
            args.block_id
        )

        print(
            f"Evidence rows retrieved: {len(rows)}"
        )

        if not rows:

            print(
                f"No evidence found for block ID: "
                f"{args.block_id}"
            )

            return

        prompt = build_prompt(
            args.block_id,
            rows,
        )

        print(
            f"Generating RCA with model: "
            f"{MODEL_NAME}"
        )

        summary = generate_summary(
            prompt
        )

        print(
            "\n=== PulseOps Behavioral Incident Summary ==="
        )

        print(
            f"Block: {args.block_id}"
        )

        print()

        print(summary)

        if args.save:

            filename = save_block_summary(
                args.block_id,
                rows,
                summary,
            )

            print(
                f"\nSaved to {filename}"
            )

    # =====================================================
    # EPISODE
    # =====================================================

    elif args.episode_id is not None:

        episode_id = args.episode_id

        print(
            f"Fetching Episode {episode_id} metadata..."
        )

        metadata = fetch_episode_metadata(
            episode_id
        )

        if metadata is None:

            print(
                f"No episode found with "
                f"EPISODE_ID={episode_id}"
            )

            return

        print(
            "Fetching episode component breakdown..."
        )

        component_breakdown = (
            fetch_episode_component_breakdown(
                episode_id
            )
        )

        print(
            f"Component/level groups retrieved: "
            f"{len(component_breakdown)}"
        )

        print(
            "Fetching representative evidence sample..."
        )

        sample_rows = fetch_episode_sample(
            episode_id,
            sample_size=25,
        )

        print(
            f"Sample evidence rows retrieved: "
            f"{len(sample_rows)}"
        )

        print(
            "Fetching ground-truth episode purity..."
        )

        purity = fetch_episode_purity(
            episode_id
        )

        if purity is None:

            raise RuntimeError(
                "Could not calculate episode purity."
            )

        (
            labeled_blocks,
            true_anomaly_blocks,
            normal_blocks,
            purity_pct,
        ) = purity

        print(
            f"Ground-truth labeled blocks: "
            f"{labeled_blocks}"
        )

        print(
            f"True anomaly blocks: "
            f"{true_anomaly_blocks}"
        )

        print(
            f"Normal blocks: "
            f"{normal_blocks}"
        )

        print(
            f"Episode purity: "
            f"{purity_pct}%"
        )

        prompt = build_episode_prompt(
            episode_id,
            metadata,
            component_breakdown,
            sample_rows,
            purity,
        )

        print(
            f"Generating episode RCA with model: "
            f"{MODEL_NAME}"
        )

        summary = generate_summary(
            prompt
        )

        print(
            "\n=== PulseOps Behavioral Episode Summary ==="
        )

        print(
            f"Episode: {episode_id}"
        )

        print()

        print(summary)

        if args.save:

            filename = save_episode_summary(
                episode_id,
                metadata,
                purity,
                summary,
            )

            print(
                f"\nSaved to {filename}"
            )


if __name__ == "__main__":
    main()