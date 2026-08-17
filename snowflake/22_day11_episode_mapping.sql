USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- =========================================================
-- PulseOps Day 11
-- Persistent mapping from behavioral incidents to episodes
-- Uses the Day 9 selected gap threshold of 2 seconds
-- =========================================================

CREATE OR REPLACE VIEW INCIDENT_EPISODE_MEMBERS AS

WITH ordered_incidents AS (

    SELECT
        i.BLOCK_ID,
        i.SEVERITY,
        i.EVENTS_PER_SECOND,
        i.COMPONENT_DIVERSITY,
        f.FIRST_EVENT_TIME,
        f.LAST_EVENT_TIME,

        LAG(f.FIRST_EVENT_TIME) OVER (
            ORDER BY
                f.FIRST_EVENT_TIME,
                i.BLOCK_ID
        ) AS PREV_INCIDENT_START

    FROM INCIDENTS_BEHAVIORAL i

    JOIN BLOCK_BEHAVIOR_FEATURES f
        ON i.BLOCK_ID = f.BLOCK_ID
),

gaps AS (

    SELECT
        *,

        DATEDIFF(
            'second',
            PREV_INCIDENT_START,
            FIRST_EVENT_TIME
        ) AS GAP_SECONDS

    FROM ordered_incidents
),

episode_flags AS (

    SELECT
        *,

        CASE
            WHEN GAP_SECONDS IS NULL
                 OR GAP_SECONDS > 2
            THEN 1
            ELSE 0
        END AS IS_NEW_EPISODE

    FROM gaps
),

episode_ids AS (

    SELECT
        *,

        SUM(IS_NEW_EPISODE) OVER (
            ORDER BY
                FIRST_EVENT_TIME,
                BLOCK_ID
            ROWS BETWEEN
                UNBOUNDED PRECEDING
                AND CURRENT ROW
        ) AS EPISODE_ID

    FROM episode_flags
)

SELECT
    EPISODE_ID,
    BLOCK_ID,
    SEVERITY,
    EVENTS_PER_SECOND,
    COMPONENT_DIVERSITY,
    FIRST_EVENT_TIME,
    LAST_EVENT_TIME,
    GAP_SECONDS

FROM episode_ids;