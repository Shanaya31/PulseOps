-- ============================================================
-- DAY 8 - TASK 1
-- Incident priority ranking
-- NULL EPS values intentionally ranked last
-- ============================================================

SELECT
    BLOCK_ID,
    SEVERITY,
    EVENTS_PER_SECOND,
    COMPONENT_DIVERSITY,
    ACTIVE_DURATION_SECONDS,

    RANK() OVER (
        ORDER BY EVENTS_PER_SECOND DESC NULLS LAST
    ) AS OVERALL_RANK,

    DENSE_RANK() OVER (
        PARTITION BY SEVERITY
        ORDER BY EVENTS_PER_SECOND DESC NULLS LAST
    ) AS RANK_WITHIN_SEVERITY

FROM INCIDENTS_BEHAVIORAL

ORDER BY OVERALL_RANK
LIMIT 30;

-- ============================================================
-- DAY 8 - TASK 3A
-- NTILE quartiles across the complete block population
-- ============================================================

SELECT
    BLOCK_ID,
    EVENTS_PER_SECOND,

    NTILE(4) OVER (
        ORDER BY EVENTS_PER_SECOND NULLS FIRST
    ) AS EPS_QUARTILE

FROM BLOCK_BEHAVIOR_DERIVED

ORDER BY EPS_QUARTILE DESC,
         EVENTS_PER_SECOND DESC NULLS LAST;

-- ============================================================
-- DAY 8 - TASK 3B
-- Where do behavioral incidents fall in EPS quartiles?
-- ============================================================

WITH BLOCK_QUARTILES AS (
    SELECT
        BLOCK_ID,
        EVENTS_PER_SECOND,

        NTILE(4) OVER (
            ORDER BY EVENTS_PER_SECOND NULLS FIRST
        ) AS EPS_QUARTILE

    FROM BLOCK_BEHAVIOR_DERIVED
)

SELECT
    q.EPS_QUARTILE,

    COUNT(*) AS TOTAL_BLOCKS,

    COUNT_IF(i.BLOCK_ID IS NOT NULL) AS FLAGGED_INCIDENTS,

    ROUND(
        100.0 * COUNT_IF(i.BLOCK_ID IS NOT NULL)
        / NULLIF(COUNT(*), 0),
        2
    ) AS INCIDENT_RATE_PCT

FROM BLOCK_QUARTILES q

LEFT JOIN INCIDENTS_BEHAVIORAL i
    ON q.BLOCK_ID = i.BLOCK_ID

GROUP BY q.EPS_QUARTILE

ORDER BY q.EPS_QUARTILE;

-- ============================================================
-- DAY 8 - TASK 4
-- Distribution of the 518 behavioral incidents
-- across EPS quartiles
-- ============================================================

WITH BLOCK_QUARTILES AS (
    SELECT
        BLOCK_ID,
        EVENTS_PER_SECOND,

        NTILE(4) OVER (
            ORDER BY EVENTS_PER_SECOND NULLS FIRST
        ) AS EPS_QUARTILE

    FROM BLOCK_BEHAVIOR_DERIVED
),

INCIDENT_QUARTILES AS (
    SELECT
        q.EPS_QUARTILE,
        i.BLOCK_ID,
        i.SEVERITY
    FROM BLOCK_QUARTILES q
    JOIN INCIDENTS_BEHAVIORAL i
        ON q.BLOCK_ID = i.BLOCK_ID
)

SELECT
    EPS_QUARTILE,

    COUNT(*) AS INCIDENTS,

    COUNT_IF(SEVERITY = 'HIGH') AS HIGH_SEVERITY,

    COUNT_IF(SEVERITY = 'MEDIUM') AS MEDIUM_SEVERITY,

    ROUND(
        100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (),
        2
    ) AS PCT_OF_ALL_INCIDENTS

FROM INCIDENT_QUARTILES

GROUP BY EPS_QUARTILE

ORDER BY EPS_QUARTILE;

-- ============================================================
-- DAY 8 - TASK 2
-- LAG / LEAD: timing between behavioral incidents
-- FIRST_EVENT_TIME comes from BLOCK_BEHAVIOR_FEATURES
-- ============================================================

WITH INCIDENT_TIMELINE AS (
    SELECT
        i.BLOCK_ID,
        i.SEVERITY,
        i.EVENTS_PER_SECOND,
        i.COMPONENT_DIVERSITY,
        f.FIRST_EVENT_TIME,
        f.LAST_EVENT_TIME
    FROM INCIDENTS_BEHAVIORAL i
    JOIN BLOCK_BEHAVIOR_FEATURES f
        ON i.BLOCK_ID = f.BLOCK_ID
)

SELECT
    BLOCK_ID,
    SEVERITY,
    FIRST_EVENT_TIME,
    LAST_EVENT_TIME,

    LAG(FIRST_EVENT_TIME) OVER (
        ORDER BY FIRST_EVENT_TIME, BLOCK_ID
    ) AS PREV_INCIDENT_START,

    DATEDIFF(
        'second',
        LAG(FIRST_EVENT_TIME) OVER (
            ORDER BY FIRST_EVENT_TIME, BLOCK_ID
        ),
        FIRST_EVENT_TIME
    ) AS SECONDS_SINCE_PREV_INCIDENT,

    LEAD(FIRST_EVENT_TIME) OVER (
        ORDER BY FIRST_EVENT_TIME, BLOCK_ID
    ) AS NEXT_INCIDENT_START

FROM INCIDENT_TIMELINE

ORDER BY FIRST_EVENT_TIME, BLOCK_ID;

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
    COUNT(*) AS INCIDENTS,
    MIN(GAP_SECONDS) AS MIN_GAP_SECONDS,
    ROUND(AVG(GAP_SECONDS), 2) AS AVG_GAP_SECONDS,
    MEDIAN(GAP_SECONDS) AS MEDIAN_GAP_SECONDS,
    MAX(GAP_SECONDS) AS MAX_GAP_SECONDS,
    COUNT_IF(GAP_SECONDS <= 60) AS WITHIN_60_SEC,
    COUNT_IF(GAP_SECONDS > 60) AS OVER_60_SEC
FROM GAPS;