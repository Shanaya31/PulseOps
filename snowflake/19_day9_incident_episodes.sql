USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- ============================================================
-- DAY 9 - TASK 1
-- Examine inter-incident gap distribution
-- ============================================================

WITH INCIDENT_TIMELINE AS (
    SELECT
        i.BLOCK_ID,
        f.FIRST_EVENT_TIME
    FROM INCIDENTS_BEHAVIORAL i
    JOIN BLOCK_BEHAVIOR_FEATURES f
        ON i.BLOCK_ID = f.BLOCK_ID
),

GAPS AS (
    SELECT
        BLOCK_ID,
        FIRST_EVENT_TIME,
        DATEDIFF(
            'second',
            LAG(FIRST_EVENT_TIME) OVER (
                ORDER BY FIRST_EVENT_TIME, BLOCK_ID
            ),
            FIRST_EVENT_TIME
        ) AS GAP_SECONDS
    FROM INCIDENT_TIMELINE
)

SELECT
    GAP_SECONDS,
    COUNT(*) AS OCCURRENCES,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (),
        2
    ) AS PCT_OF_GAPS
FROM GAPS
WHERE GAP_SECONDS IS NOT NULL
GROUP BY GAP_SECONDS
ORDER BY GAP_SECONDS;

-- ============================================================
-- DAY 9 - TASK 2
-- Compare candidate episode thresholds
-- ============================================================

WITH INCIDENT_TIMELINE AS (
    SELECT
        i.BLOCK_ID,
        f.FIRST_EVENT_TIME
    FROM INCIDENTS_BEHAVIORAL i
    JOIN BLOCK_BEHAVIOR_FEATURES f
        ON i.BLOCK_ID = f.BLOCK_ID
),

GAPS AS (
    SELECT
        BLOCK_ID,
        FIRST_EVENT_TIME,
        DATEDIFF(
            'second',
            LAG(FIRST_EVENT_TIME) OVER (
                ORDER BY FIRST_EVENT_TIME, BLOCK_ID
            ),
            FIRST_EVENT_TIME
        ) AS GAP_SECONDS
    FROM INCIDENT_TIMELINE
)

SELECT
    COUNT_IF(GAP_SECONDS > 1)  + 1 AS EPISODES_AT_1_SEC,
    COUNT_IF(GAP_SECONDS > 2)  + 1 AS EPISODES_AT_2_SEC,
    COUNT_IF(GAP_SECONDS > 5)  + 1 AS EPISODES_AT_5_SEC,
    COUNT_IF(GAP_SECONDS > 10) + 1 AS EPISODES_AT_10_SEC,
    COUNT_IF(GAP_SECONDS > 15) + 1 AS EPISODES_AT_15_SEC,
    COUNT_IF(GAP_SECONDS > 20) + 1 AS EPISODES_AT_20_SEC,
    COUNT_IF(GAP_SECONDS > 30) + 1 AS EPISODES_AT_30_SEC
FROM GAPS;

-- ============================================================
-- DAY 9 - TASK 3
-- Gaps-and-islands incident episode detection
--
-- Episode threshold: 2 seconds
--
-- Rationale:
-- Inter-incident gap analysis showed that most incidents occur
-- within 0-1 seconds of one another. A >2 second gap therefore
-- separates dense incident bursts while retaining meaningful
-- temporal episodes.
-- ============================================================

CREATE OR REPLACE VIEW INCIDENT_EPISODES AS

WITH INCIDENT_TIMELINE AS (

    SELECT
        i.BLOCK_ID,
        i.SEVERITY,
        i.EVENT_COUNT,
        i.EVENTS_PER_SECOND,
        i.COMPONENT_DIVERSITY,
        f.FIRST_EVENT_TIME,
        f.LAST_EVENT_TIME

    FROM INCIDENTS_BEHAVIORAL i

    JOIN BLOCK_BEHAVIOR_FEATURES f
        ON i.BLOCK_ID = f.BLOCK_ID
),

ORDERED_INCIDENTS AS (

    SELECT
        *,

        DATEDIFF(
            'second',
            LAG(FIRST_EVENT_TIME) OVER (
                ORDER BY FIRST_EVENT_TIME, BLOCK_ID
            ),
            FIRST_EVENT_TIME
        ) AS GAP_SECONDS

    FROM INCIDENT_TIMELINE
),

EPISODE_FLAGS AS (

    SELECT
        *,

        CASE
            WHEN GAP_SECONDS IS NULL
              OR GAP_SECONDS > 2
            THEN 1
            ELSE 0
        END AS IS_NEW_EPISODE

    FROM ORDERED_INCIDENTS
),

EPISODE_IDS AS (

    SELECT
        *,

        SUM(IS_NEW_EPISODE) OVER (
            ORDER BY FIRST_EVENT_TIME, BLOCK_ID
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS EPISODE_ID

    FROM EPISODE_FLAGS
)

SELECT
    EPISODE_ID,

    MIN(FIRST_EVENT_TIME) AS EPISODE_START,

    MAX(LAST_EVENT_TIME) AS EPISODE_END,

    DATEDIFF(
        'second',
        MIN(FIRST_EVENT_TIME),
        MAX(LAST_EVENT_TIME)
    ) AS EPISODE_DURATION_SECONDS,

    COUNT(*) AS N_INCIDENTS,

    COUNT(DISTINCT BLOCK_ID) AS N_DISTINCT_BLOCKS,

    COUNT_IF(SEVERITY = 'HIGH') AS N_HIGH_SEVERITY,

    COUNT_IF(SEVERITY = 'MEDIUM') AS N_MEDIUM_SEVERITY,

    SUM(EVENT_COUNT) AS TOTAL_EVENTS,

    ROUND(AVG(EVENTS_PER_SECOND), 4)
        AS AVG_EVENTS_PER_SECOND,

    ROUND(MAX(EVENTS_PER_SECOND), 4)
        AS MAX_EVENTS_PER_SECOND,

    ROUND(AVG(COMPONENT_DIVERSITY), 4)
        AS AVG_COMPONENT_DIVERSITY

FROM EPISODE_IDS

GROUP BY EPISODE_ID;