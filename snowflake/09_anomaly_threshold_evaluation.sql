USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- =========================================================
-- PulseOps
-- Evaluate global z-score thresholds
--
-- Baseline experiment only.
-- =========================================================

WITH stats AS (

    SELECT
        AVG(EVENT_COUNT)
            AS MEAN_EVENTS,

        STDDEV(EVENT_COUNT)
            AS STD_EVENTS

    FROM PULSEOPS.CORE.WINDOWED_METRICS
),

scored AS (

    SELECT
        w.BLOCK_ID,
        w.EVENT_COUNT,

        ABS(
            (
                w.EVENT_COUNT - s.MEAN_EVENTS
            )
            /
            NULLIF(
                s.STD_EVENTS,
                0
            )
        ) AS ABS_Z_SCORE

    FROM PULSEOPS.CORE.WINDOWED_METRICS w

    CROSS JOIN stats s
),

block_scores AS (

    SELECT
        BLOCK_ID,
        MAX(ABS_Z_SCORE)
            AS MAX_Z_SCORE

    FROM scored

    GROUP BY BLOCK_ID
),

thresholds AS (

    SELECT 1.5 AS THRESHOLD

    UNION ALL

    SELECT 2.0

    UNION ALL

    SELECT 2.5

    UNION ALL

    SELECT 3.0
)

SELECT
    t.THRESHOLD,

    COUNT_IF(
        b.MAX_Z_SCORE >= t.THRESHOLD
    ) AS FLAGGED_BLOCKS,

    COUNT_IF(
        b.MAX_Z_SCORE >= t.THRESHOLD
        AND g.LABEL = 'Anomaly'
    ) AS TRUE_ANOMALIES,

    COUNT_IF(
        b.MAX_Z_SCORE >= t.THRESHOLD
        AND g.LABEL = 'Normal'
    ) AS FALSE_POSITIVES

FROM block_scores b

JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON b.BLOCK_ID = g.BLOCK_ID

CROSS JOIN thresholds t

GROUP BY t.THRESHOLD

ORDER BY t.THRESHOLD;