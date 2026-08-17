USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- =========================================================
-- PulseOps
-- Detailed anomalous-block analysis
-- =========================================================

SELECT
    r.BLOCK_ID,
    g.LABEL,

    COUNT(*) AS EVENT_COUNT,

    MIN(r.EVENT_TIME)
        AS FIRST_EVENT,

    MAX(r.EVENT_TIME)
        AS LAST_EVENT

FROM PULSEOPS.CORE.RAW_LOG_EVENTS r

JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON r.BLOCK_ID = g.BLOCK_ID

WHERE g.LABEL = 'Anomaly'

GROUP BY
    r.BLOCK_ID,
    g.LABEL

ORDER BY EVENT_COUNT DESC

LIMIT 30;


-- Inspect raw event sequence for anomalous blocks
SELECT
    r.BLOCK_ID,
    r.EVENT_TIME,
    r.COMPONENT,
    r.LEVEL,
    r.MESSAGE
FROM PULSEOPS.CORE.RAW_LOG_EVENTS r

JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON r.BLOCK_ID = g.BLOCK_ID

WHERE g.LABEL = 'Anomaly'

ORDER BY
    r.BLOCK_ID,
    r.EVENT_TIME

LIMIT 200;