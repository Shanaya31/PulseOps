USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- ============================================================
-- PulseOps Day 6
-- Rolling Component Z-Score Analysis
--
-- Baseline:
--   Previous 5 observations for the same component
--
-- Current observation is EXCLUDED from its own baseline.
-- ============================================================

-- ============================================================
-- SECTION 1: REBUILD COMPONENT-WINDOW ACTIVITY
-- ============================================================

CREATE OR REPLACE TEMP VIEW COMPONENT_WINDOW_ACTIVITY AS
SELECT
    DATE_TRUNC('MINUTE', EVENT_TIME) AS WINDOW_START,

    DATEADD(
        'MINUTE',
        1,
        DATE_TRUNC('MINUTE', EVENT_TIME)
    ) AS WINDOW_END,

    COMPONENT,

    COUNT(*) AS EVENT_COUNT,

    COUNT_IF(LEVEL = 'ERROR') AS ERROR_COUNT,

    COUNT_IF(LEVEL = 'WARN') AS WARN_COUNT

FROM PULSEOPS.CORE.RAW_LOG_EVENTS

WHERE EVENT_TIME IS NOT NULL
  AND COMPONENT IS NOT NULL

GROUP BY
    DATE_TRUNC('MINUTE', EVENT_TIME),
    COMPONENT;

-- ============================================================
-- SECTION 2: CALCULATE 5-WINDOW ROLLING BASELINE
-- ============================================================

CREATE OR REPLACE TEMP VIEW COMPONENT_ROLLING_STATS AS
SELECT
    WINDOW_START,
    WINDOW_END,
    COMPONENT,
    EVENT_COUNT,
    ERROR_COUNT,
    WARN_COUNT,

    COUNT(*) OVER (
        PARTITION BY COMPONENT
        ORDER BY WINDOW_START
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS HISTORY_COUNT,

    AVG(EVENT_COUNT) OVER (
        PARTITION BY COMPONENT
        ORDER BY WINDOW_START
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS ROLLING_MEAN,

    STDDEV(EVENT_COUNT) OVER (
        PARTITION BY COMPONENT
        ORDER BY WINDOW_START
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS ROLLING_STDDEV

FROM COMPONENT_WINDOW_ACTIVITY;

-- ============================================================
-- SECTION 3: CREATE Z-SCORED OBSERVATIONS
-- ============================================================

CREATE OR REPLACE TEMP VIEW COMPONENT_ZSCORES AS
SELECT
    WINDOW_START,
    WINDOW_END,
    COMPONENT,
    EVENT_COUNT,
    ERROR_COUNT,
    WARN_COUNT,
    HISTORY_COUNT,
    ROUND(ROLLING_MEAN, 2) AS ROLLING_MEAN,
    ROUND(ROLLING_STDDEV, 2) AS ROLLING_STDDEV,

    ROUND(
        (EVENT_COUNT - ROLLING_MEAN)
        / NULLIF(ROLLING_STDDEV, 0),
        3
    ) AS Z_SCORE

FROM COMPONENT_ROLLING_STATS

WHERE HISTORY_COUNT >= 5
  AND ROLLING_STDDEV > 0;

-- ============================================================
-- SECTION 4: How many observations are actually scoreable?
-- ============================================================


SELECT
    COUNT(*) AS SCORED_OBSERVATIONS,
    COUNT(DISTINCT COMPONENT) AS SCORED_COMPONENTS,
    MIN(WINDOW_START) AS FIRST_SCORED_WINDOW,
    MAX(WINDOW_START) AS LAST_SCORED_WINDOW
FROM COMPONENT_ZSCORES;

SELECT
    COMPONENT,
    COUNT(*) AS SCORED_WINDOWS,
    ROUND(AVG(Z_SCORE), 3) AS AVG_Z_SCORE,
    ROUND(MIN(Z_SCORE), 3) AS MIN_Z_SCORE,
    ROUND(MAX(Z_SCORE), 3) AS MAX_Z_SCORE,
    ROUND(MAX(ABS(Z_SCORE)), 3) AS MAX_ABS_Z_SCORE
FROM COMPONENT_ZSCORES
GROUP BY COMPONENT
ORDER BY MAX_ABS_Z_SCORE DESC;

-- ============================================================
-- SECTION 5: inspect the actual outliers
-- ============================================================


SELECT
    WINDOW_START,
    WINDOW_END,
    COMPONENT,
    EVENT_COUNT,
    HISTORY_COUNT,
    ROLLING_MEAN,
    ROLLING_STDDEV,
    Z_SCORE
FROM COMPONENT_ZSCORES
ORDER BY ABS(Z_SCORE) DESC
LIMIT 50;

-- ============================================================
-- SECTION 6: threshold distribution
-- ============================================================


SELECT
    THRESHOLD,
    COUNT_IF(ABS(Z_SCORE) >= THRESHOLD) AS FLAGGED_COMPONENT_WINDOWS,
    ROUND(
        100.0 * COUNT_IF(ABS(Z_SCORE) >= THRESHOLD)
        / NULLIF(COUNT(*), 0),
        2
    ) AS FLAGGED_PERCENT
FROM COMPONENT_ZSCORES

CROSS JOIN (
    SELECT 1.5 AS THRESHOLD
    UNION ALL SELECT 2.0
    UNION ALL SELECT 2.5
    UNION ALL SELECT 3.0
    UNION ALL SELECT 3.5
) t

GROUP BY THRESHOLD
ORDER BY THRESHOLD;

-- ============================================================
-- SECTION 7: map component anomalies back to HDFS blocks
-- ============================================================


CREATE OR REPLACE TEMP VIEW ZSCORE_BLOCK_CANDIDATES AS
SELECT DISTINCT
    z.WINDOW_START,
    z.WINDOW_END,
    z.COMPONENT,
    z.EVENT_COUNT,
    z.Z_SCORE,
    r.BLOCK_ID

FROM COMPONENT_ZSCORES z

JOIN PULSEOPS.CORE.RAW_LOG_EVENTS r
    ON r.COMPONENT = z.COMPONENT
   AND r.EVENT_TIME >= z.WINDOW_START
   AND r.EVENT_TIME < z.WINDOW_END

WHERE r.BLOCK_ID IS NOT NULL
  AND r.BLOCK_ID <> '';

-- ============================================================
-- SECTION 8: evaluate thresholds against ground truth
-- ============================================================
  

SELECT
    t.THRESHOLD,

    COUNT(DISTINCT CASE
        WHEN ABS(z.Z_SCORE) >= t.THRESHOLD
        THEN z.BLOCK_ID
    END) AS FLAGGED_BLOCKS,

    COUNT(DISTINCT CASE
        WHEN ABS(z.Z_SCORE) >= t.THRESHOLD
         AND g.LABEL = 'Anomaly'
        THEN z.BLOCK_ID
    END) AS TRUE_ANOMALY_BLOCKS,

    COUNT(DISTINCT CASE
        WHEN ABS(z.Z_SCORE) >= t.THRESHOLD
         AND g.LABEL = 'Normal'
        THEN z.BLOCK_ID
    END) AS FALSE_POSITIVE_BLOCKS

FROM ZSCORE_BLOCK_CANDIDATES z

JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON z.BLOCK_ID = g.BLOCK_ID

CROSS JOIN (
    SELECT 1.5 AS THRESHOLD
    UNION ALL SELECT 2.0
    UNION ALL SELECT 2.5
    UNION ALL SELECT 3.0
    UNION ALL SELECT 3.5
) t

GROUP BY t.THRESHOLD
ORDER BY t.THRESHOLD;

-- ============================================================
-- SECTION 9: PRECISION / RECALL THRESHOLD EVALUATION
-- ============================================================

WITH TOTALS AS (
    SELECT
        COUNT(DISTINCT CASE
            WHEN LABEL = 'Anomaly' THEN BLOCK_ID
        END) AS TOTAL_ANOMALY_BLOCKS
    FROM PULSEOPS.CORE.GROUND_TRUTH_LABELS
),

RESULTS AS (
    SELECT
        t.THRESHOLD,

        COUNT(DISTINCT CASE
            WHEN ABS(z.Z_SCORE) >= t.THRESHOLD
            THEN z.BLOCK_ID
        END) AS FLAGGED_BLOCKS,

        COUNT(DISTINCT CASE
            WHEN ABS(z.Z_SCORE) >= t.THRESHOLD
             AND g.LABEL = 'Anomaly'
            THEN z.BLOCK_ID
        END) AS TRUE_POSITIVES,

        COUNT(DISTINCT CASE
            WHEN ABS(z.Z_SCORE) >= t.THRESHOLD
             AND g.LABEL = 'Normal'
            THEN z.BLOCK_ID
        END) AS FALSE_POSITIVES

    FROM ZSCORE_BLOCK_CANDIDATES z

    JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
        ON z.BLOCK_ID = g.BLOCK_ID

    CROSS JOIN (
        SELECT 1.5 AS THRESHOLD
        UNION ALL SELECT 2.0
        UNION ALL SELECT 2.5
        UNION ALL SELECT 3.0
        UNION ALL SELECT 3.5
        UNION ALL SELECT 4.0
        UNION ALL SELECT 4.5
        UNION ALL SELECT 5.0
        UNION ALL SELECT 5.5
        UNION ALL SELECT 6.0
        UNION ALL SELECT 7.0
    ) t

    GROUP BY t.THRESHOLD
)

SELECT
    r.THRESHOLD,
    r.FLAGGED_BLOCKS,
    r.TRUE_POSITIVES,
    r.FALSE_POSITIVES,
    t.TOTAL_ANOMALY_BLOCKS,

    ROUND(
        100.0 * r.TRUE_POSITIVES /
        NULLIF(r.FLAGGED_BLOCKS, 0),
        2
    ) AS PRECISION_PCT,

    ROUND(
        100.0 * r.TRUE_POSITIVES /
        NULLIF(t.TOTAL_ANOMALY_BLOCKS, 0),
        2
    ) AS RECALL_PCT

FROM RESULTS r
CROSS JOIN TOTALS t

ORDER BY r.THRESHOLD;

-- ============================================================
-- SECTION 10: POSITIVE VS NEGATIVE Z-SCORE BEHAVIOR
-- ============================================================

SELECT
    CASE
        WHEN z.Z_SCORE >= 3 THEN 'POSITIVE_SPIKE'
        WHEN z.Z_SCORE <= -3 THEN 'NEGATIVE_DROP'
        ELSE 'NORMAL_RANGE'
    END AS Z_DIRECTION,

    COUNT(*) AS COMPONENT_BLOCK_ROWS,

    COUNT(DISTINCT z.BLOCK_ID) AS UNIQUE_BLOCKS,

    COUNT(DISTINCT CASE
        WHEN g.LABEL = 'Anomaly'
        THEN z.BLOCK_ID
    END) AS ANOMALY_BLOCKS,

    COUNT(DISTINCT CASE
        WHEN g.LABEL = 'Normal'
        THEN z.BLOCK_ID
    END) AS NORMAL_BLOCKS,

    ROUND(
        100.0 *
        COUNT(DISTINCT CASE
            WHEN g.LABEL = 'Anomaly'
            THEN z.BLOCK_ID
        END)
        /
        NULLIF(COUNT(DISTINCT z.BLOCK_ID), 0),
        2
    ) AS ANOMALY_PERCENT

FROM ZSCORE_BLOCK_CANDIDATES z

JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON z.BLOCK_ID = g.BLOCK_ID

GROUP BY 1

ORDER BY
    CASE Z_DIRECTION
        WHEN 'POSITIVE_SPIKE' THEN 1
        WHEN 'NEGATIVE_DROP' THEN 2
        ELSE 3
    END;