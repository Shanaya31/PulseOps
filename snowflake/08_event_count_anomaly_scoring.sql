USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- =========================================================
-- PulseOps
-- Experimental global EVENT_COUNT z-score
--
-- NOTE:
-- This is a baseline experiment, NOT the final detector.
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
        w.WINDOW_START,
        w.WINDOW_END,
        w.EVENT_COUNT,

        (
            w.EVENT_COUNT - s.MEAN_EVENTS
        )
        /
        NULLIF(
            s.STD_EVENTS,
            0
        ) AS Z_SCORE

    FROM PULSEOPS.CORE.WINDOWED_METRICS w

    CROSS JOIN stats s
)

SELECT
    s.BLOCK_ID,
    g.LABEL,
    s.EVENT_COUNT,

    ROUND(
        s.Z_SCORE,
        2
    ) AS Z_SCORE,

    s.WINDOW_START,
    s.WINDOW_END

FROM scored s

LEFT JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON s.BLOCK_ID = g.BLOCK_ID

ORDER BY ABS(s.Z_SCORE) DESC

LIMIT 30;