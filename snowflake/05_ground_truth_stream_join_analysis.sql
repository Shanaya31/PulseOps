USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- =========================================================
-- PulseOps
-- Compare streamed blocks against official ground truth
-- =========================================================

-- Distribution of streamed events by ground-truth label
SELECT
    g.LABEL,
    COUNT(DISTINCT r.BLOCK_ID) AS UNIQUE_BLOCKS,
    COUNT(*) AS LOG_EVENTS
FROM PULSEOPS.CORE.RAW_LOG_EVENTS r
JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON r.BLOCK_ID = g.BLOCK_ID
WHERE r.BLOCK_ID IS NOT NULL
GROUP BY g.LABEL
ORDER BY UNIQUE_BLOCKS DESC;


-- Coverage summary
SELECT
    COUNT(DISTINCT r.BLOCK_ID)
        AS STREAMED_BLOCKS,

    COUNT(
        DISTINCT CASE
            WHEN g.LABEL = 'Normal'
            THEN r.BLOCK_ID
        END
    ) AS NORMAL_BLOCKS,

    COUNT(
        DISTINCT CASE
            WHEN g.LABEL = 'Anomaly'
            THEN r.BLOCK_ID
        END
    ) AS ANOMALY_BLOCKS,

    COUNT(
        DISTINCT CASE
            WHEN g.BLOCK_ID IS NULL
            THEN r.BLOCK_ID
        END
    ) AS UNMATCHED_BLOCKS

FROM PULSEOPS.CORE.RAW_LOG_EVENTS r

LEFT JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON r.BLOCK_ID = g.BLOCK_ID

WHERE r.BLOCK_ID IS NOT NULL;