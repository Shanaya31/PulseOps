USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- =========================================================
-- PulseOps
-- Block-level behavioural feature analysis
-- =========================================================

WITH block_features AS (

    SELECT
        BLOCK_ID,

        COUNT(*) AS TOTAL_EVENTS,

        COUNT(DISTINCT COMPONENT)
            AS UNIQUE_COMPONENTS,

        COUNT(DISTINCT MESSAGE)
            AS UNIQUE_MESSAGES,

        DATEDIFF(
            'second',
            MIN(EVENT_TIME),
            MAX(EVENT_TIME)
        ) AS LIFETIME_SECONDS

    FROM PULSEOPS.CORE.RAW_LOG_EVENTS

    WHERE BLOCK_ID IS NOT NULL
      AND EVENT_TIME IS NOT NULL

    GROUP BY BLOCK_ID
)

SELECT
    f.BLOCK_ID,
    g.LABEL,

    f.TOTAL_EVENTS,
    f.UNIQUE_COMPONENTS,
    f.UNIQUE_MESSAGES,
    f.LIFETIME_SECONDS

FROM block_features f

JOIN PULSEOPS.CORE.GROUND_TRUTH_LABELS g
    ON f.BLOCK_ID = g.BLOCK_ID

ORDER BY
    CASE
        WHEN g.LABEL = 'Anomaly'
        THEN 0
        ELSE 1
    END,
    f.TOTAL_EVENTS DESC

LIMIT 50;