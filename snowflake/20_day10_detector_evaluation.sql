USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- ============================================
-- DAY 10
-- Episode-level ground truth purity analysis
-- ============================================

WITH incident_mapping AS (
    SELECT
        i.BLOCK_ID,
        f.FIRST_EVENT_TIME,
        i.SEVERITY,
        LAG(f.FIRST_EVENT_TIME) OVER (
            ORDER BY f.FIRST_EVENT_TIME, i.BLOCK_ID
        ) AS PREV_EVENT_TIME
    FROM INCIDENTS_BEHAVIORAL i
    JOIN BLOCK_BEHAVIOR_FEATURES f
        ON i.BLOCK_ID = f.BLOCK_ID
),

episode_flags AS (
    SELECT
        *,
        DATEDIFF(
            'second',
            PREV_EVENT_TIME,
            FIRST_EVENT_TIME
        ) AS GAP_SECONDS,

        CASE
            WHEN PREV_EVENT_TIME IS NULL THEN 1
            WHEN DATEDIFF(
                'second',
                PREV_EVENT_TIME,
                FIRST_EVENT_TIME
            ) > 2 THEN 1
            ELSE 0
        END AS NEW_EPISODE
    FROM incident_mapping
),

episode_ids AS (
    SELECT
        *,
        SUM(NEW_EPISODE) OVER (
            ORDER BY FIRST_EVENT_TIME, BLOCK_ID
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS EPISODE_ID
    FROM episode_flags
)

SELECT
    e.EPISODE_ID,
    COUNT(*) AS N_BLOCKS,

    COUNT_IF(g.LABEL = 'Anomaly')
        AS TRUE_POSITIVES,

    COUNT_IF(g.LABEL = 'Normal')
        AS FALSE_POSITIVES,

    ROUND(
        100.0 * COUNT_IF(g.LABEL = 'Anomaly')
        / NULLIF(COUNT(*), 0),
        2
    ) AS PURITY_PCT

FROM episode_ids e

JOIN GROUND_TRUTH_LABELS g
    ON e.BLOCK_ID = g.BLOCK_ID

WHERE e.EPISODE_ID = 66

GROUP BY e.EPISODE_ID;

SELECT
    COUNT_IF(
        i.SEVERITY = 'HIGH'
        AND g.LABEL = 'Anomaly'
    ) AS HIGH_TRUE_POSITIVES,

    COUNT_IF(
        i.SEVERITY = 'HIGH'
        AND g.LABEL = 'Normal'
    ) AS HIGH_FALSE_POSITIVES,

    COUNT_IF(
        i.SEVERITY = 'HIGH'
    ) AS HIGH_FLAGGED,

    ROUND(
        100.0 *
        COUNT_IF(
            i.SEVERITY = 'HIGH'
            AND g.LABEL = 'Anomaly'
        )
        / NULLIF(
            COUNT_IF(i.SEVERITY = 'HIGH'),
            0
        ),
        2
    ) AS HIGH_ONLY_PRECISION_PCT

FROM INCIDENTS_BEHAVIORAL i

JOIN GROUND_TRUTH_LABELS g
    ON i.BLOCK_ID = g.BLOCK_ID;

    SELECT
    i.SEVERITY,

    COUNT(*) AS FLAGGED_BLOCKS,

    COUNT_IF(g.LABEL = 'Anomaly')
        AS TRUE_POSITIVES,

    COUNT_IF(g.LABEL = 'Normal')
        AS FALSE_POSITIVES,

    ROUND(
        100.0 * COUNT_IF(g.LABEL = 'Anomaly')
        / NULLIF(COUNT(*), 0),
        2
    ) AS PRECISION_PCT

FROM INCIDENTS_BEHAVIORAL i

JOIN GROUND_TRUTH_LABELS g
    ON i.BLOCK_ID = g.BLOCK_ID

GROUP BY i.SEVERITY

ORDER BY
    CASE i.SEVERITY
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        ELSE 3
    END;