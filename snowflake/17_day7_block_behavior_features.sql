USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- =====================================================
-- PulseOps Day 7
-- Block-Level Behavioral Feature Engineering
-- =====================================================

-- =====================================================
-- SECTION 1: BLOCK POPULATION BASELINE
-- =====================================================

SELECT
    COUNT(*) AS TOTAL_EVENTS,
    COUNT(DISTINCT BLOCK_ID) AS UNIQUE_BLOCKS,
    COUNT(DISTINCT COMPONENT) AS UNIQUE_COMPONENTS,
    MIN(EVENT_TIME) AS FIRST_EVENT,
    MAX(EVENT_TIME) AS LAST_EVENT
FROM RAW_LOG_EVENTS
WHERE BLOCK_ID IS NOT NULL;

-- =====================================================
-- SECTION 2: BLOCK BEHAVIOR FEATURE VIEW
-- =====================================================

CREATE OR REPLACE VIEW BLOCK_BEHAVIOR_FEATURES AS

SELECT
    BLOCK_ID,

    COUNT(*) AS EVENT_COUNT,

    COUNT(DISTINCT COMPONENT) AS COMPONENT_COUNT,

    COUNT(DISTINCT LEVEL) AS LEVEL_COUNT,

    SUM(
        CASE
            WHEN UPPER(LEVEL) = 'ERROR' THEN 1
            ELSE 0
        END
    ) AS ERROR_COUNT,

    SUM(
        CASE
            WHEN UPPER(LEVEL) IN ('WARN', 'WARNING') THEN 1
            ELSE 0
        END
    ) AS WARN_COUNT,

    MIN(EVENT_TIME) AS FIRST_EVENT_TIME,

    MAX(EVENT_TIME) AS LAST_EVENT_TIME,

    DATEDIFF(
        'second',
        MIN(EVENT_TIME),
        MAX(EVENT_TIME)
    ) AS ACTIVE_DURATION_SECONDS

FROM RAW_LOG_EVENTS

WHERE BLOCK_ID IS NOT NULL

GROUP BY BLOCK_ID;

-- =====================================================
-- SECTION 3: FEATURE VIEW VALIDATION
-- =====================================================

SELECT
    COUNT(*) AS FEATURE_ROWS,
    COUNT(DISTINCT BLOCK_ID) AS UNIQUE_BLOCKS,

    ROUND(AVG(EVENT_COUNT), 2) AS AVG_EVENTS_PER_BLOCK,
    MEDIAN(EVENT_COUNT) AS MEDIAN_EVENTS_PER_BLOCK,
    MAX(EVENT_COUNT) AS MAX_EVENTS_PER_BLOCK,

    ROUND(AVG(COMPONENT_COUNT), 2) AS AVG_COMPONENTS_PER_BLOCK,
    MAX(COMPONENT_COUNT) AS MAX_COMPONENTS_PER_BLOCK,

    SUM(ERROR_COUNT) AS TOTAL_ERRORS,
    SUM(WARN_COUNT) AS TOTAL_WARNINGS

FROM BLOCK_BEHAVIOR_FEATURES;

-- =====================================================
-- SECTION 4: MOST ACTIVE BLOCKS
-- =====================================================

SELECT
    BLOCK_ID,
    EVENT_COUNT,
    COMPONENT_COUNT,
    LEVEL_COUNT,
    ERROR_COUNT,
    WARN_COUNT,
    ACTIVE_DURATION_SECONDS
FROM BLOCK_BEHAVIOR_FEATURES
ORDER BY EVENT_COUNT DESC
LIMIT 30;

-- =====================================================
-- SECTION 5: NORMAL VS ANOMALOUS BLOCK FEATURES
-- =====================================================

SELECT
    g.LABEL,

    COUNT(DISTINCT b.BLOCK_ID) AS BLOCKS,

    ROUND(AVG(b.EVENT_COUNT), 2)
        AS AVG_EVENT_COUNT,

    MEDIAN(b.EVENT_COUNT)
        AS MEDIAN_EVENT_COUNT,

    ROUND(AVG(b.COMPONENT_COUNT), 2)
        AS AVG_COMPONENT_COUNT,

    ROUND(AVG(b.ERROR_COUNT), 2)
        AS AVG_ERROR_COUNT,

    ROUND(AVG(b.WARN_COUNT), 2)
        AS AVG_WARN_COUNT,

    ROUND(AVG(b.ACTIVE_DURATION_SECONDS), 2)
        AS AVG_ACTIVE_DURATION_SECONDS

FROM BLOCK_BEHAVIOR_FEATURES b

JOIN GROUND_TRUTH_LABELS g
    ON b.BLOCK_ID = g.BLOCK_ID

GROUP BY g.LABEL
ORDER BY g.LABEL;

-- =====================================================
-- SECTION 6: FEATURE SEPARATION ANALYSIS
-- =====================================================

WITH LABEL_STATS AS (

    SELECT
        g.LABEL,

        AVG(b.EVENT_COUNT) AS AVG_EVENT_COUNT,
        AVG(b.COMPONENT_COUNT) AS AVG_COMPONENT_COUNT,
        AVG(b.ERROR_COUNT) AS AVG_ERROR_COUNT,
        AVG(b.WARN_COUNT) AS AVG_WARN_COUNT,
        AVG(b.ACTIVE_DURATION_SECONDS)
            AS AVG_ACTIVE_DURATION_SECONDS

    FROM BLOCK_BEHAVIOR_FEATURES b

    JOIN GROUND_TRUTH_LABELS g
        ON b.BLOCK_ID = g.BLOCK_ID

    GROUP BY g.LABEL
)

SELECT *
FROM LABEL_STATS
ORDER BY LABEL;

-- =====================================================
-- SECTION 7: DERIVED BEHAVIORAL FEATURES
-- =====================================================

CREATE OR REPLACE VIEW BLOCK_BEHAVIOR_DERIVED AS

SELECT
    BLOCK_ID,
    EVENT_COUNT,
    COMPONENT_COUNT,
    ACTIVE_DURATION_SECONDS,

    -- Events generated per active second
    ROUND(
        EVENT_COUNT /
        NULLIF(ACTIVE_DURATION_SECONDS, 0),
        4
    ) AS EVENTS_PER_SECOND,

    -- Average number of events contributed per component
    ROUND(
        EVENT_COUNT /
        NULLIF(COMPONENT_COUNT, 0),
        4
    ) AS EVENTS_PER_COMPONENT,

    -- Component diversity relative to the 8 components
    ROUND(
        COMPONENT_COUNT / 8.0,
        4
    ) AS COMPONENT_DIVERSITY,

    -- Extremely short-lived blocks can be interesting
    CASE
        WHEN ACTIVE_DURATION_SECONDS <= 30 THEN 1
        ELSE 0
    END AS SHORT_LIVED_FLAG

FROM BLOCK_BEHAVIOR_FEATURES;

-- =====================================================
-- SECTION 8: DERIVED FEATURE VALIDATION
-- =====================================================

SELECT
    COUNT(*) AS BLOCKS,

    ROUND(AVG(EVENTS_PER_SECOND), 4)
        AS AVG_EVENTS_PER_SECOND,

    ROUND(AVG(EVENTS_PER_COMPONENT), 4)
        AS AVG_EVENTS_PER_COMPONENT,

    ROUND(AVG(COMPONENT_DIVERSITY), 4)
        AS AVG_COMPONENT_DIVERSITY,

    SUM(SHORT_LIVED_FLAG)
        AS SHORT_LIVED_BLOCKS,

    ROUND(
        100.0 * SUM(SHORT_LIVED_FLAG) / COUNT(*),
        2
    ) AS SHORT_LIVED_PERCENT

FROM BLOCK_BEHAVIOR_DERIVED;

-- =====================================================
-- SECTION 9: DERIVED FEATURE SEPARATION
-- =====================================================

SELECT
    g.LABEL,

    COUNT(*) AS BLOCKS,

    ROUND(AVG(b.EVENTS_PER_SECOND), 4)
        AS AVG_EVENTS_PER_SECOND,

    ROUND(AVG(b.EVENTS_PER_COMPONENT), 4)
        AS AVG_EVENTS_PER_COMPONENT,

    ROUND(AVG(b.COMPONENT_DIVERSITY), 4)
        AS AVG_COMPONENT_DIVERSITY,

    ROUND(AVG(b.ACTIVE_DURATION_SECONDS), 2)
        AS AVG_DURATION_SECONDS,

    SUM(b.SHORT_LIVED_FLAG)
        AS SHORT_LIVED_BLOCKS,

    ROUND(
        100.0 * SUM(b.SHORT_LIVED_FLAG) / COUNT(*),
        2
    ) AS SHORT_LIVED_PERCENT

FROM BLOCK_BEHAVIOR_DERIVED b

JOIN GROUND_TRUTH_LABELS g
    ON b.BLOCK_ID = g.BLOCK_ID

GROUP BY g.LABEL

ORDER BY g.LABEL;

-- =====================================================
-- SECTION 10: FEATURE DISTRIBUTION BY LABEL
-- =====================================================

SELECT
    g.LABEL,

    -- Events per second
    ROUND(PERCENTILE_CONT(0.25)
        WITHIN GROUP (ORDER BY b.EVENTS_PER_SECOND), 4)
        AS EPS_P25,

    ROUND(MEDIAN(b.EVENTS_PER_SECOND), 4)
        AS EPS_MEDIAN,

    ROUND(PERCENTILE_CONT(0.75)
        WITHIN GROUP (ORDER BY b.EVENTS_PER_SECOND), 4)
        AS EPS_P75,


    -- Active duration
    ROUND(PERCENTILE_CONT(0.25)
        WITHIN GROUP (ORDER BY b.ACTIVE_DURATION_SECONDS), 2)
        AS DURATION_P25,

    ROUND(MEDIAN(b.ACTIVE_DURATION_SECONDS), 2)
        AS DURATION_MEDIAN,

    ROUND(PERCENTILE_CONT(0.75)
        WITHIN GROUP (ORDER BY b.ACTIVE_DURATION_SECONDS), 2)
        AS DURATION_P75,


    -- Component diversity
    ROUND(MEDIAN(b.COMPONENT_DIVERSITY), 4)
        AS DIVERSITY_MEDIAN,

    ROUND(AVG(b.COMPONENT_DIVERSITY), 4)
        AS DIVERSITY_AVG

FROM BLOCK_BEHAVIOR_DERIVED b

JOIN GROUND_TRUTH_LABELS g
    ON b.BLOCK_ID = g.BLOCK_ID

GROUP BY g.LABEL

ORDER BY g.LABEL;

-- =====================================================
-- SECTION 11: SHORT-LIVED THRESHOLD EVALUATION
-- =====================================================

SELECT
    t.DURATION_THRESHOLD,

    COUNT(*) AS FLAGGED_BLOCKS,

    COUNT_IF(g.LABEL = 'Anomaly')
        AS TRUE_POSITIVES,

    COUNT_IF(g.LABEL = 'Normal')
        AS FALSE_POSITIVES,

    ROUND(
        100.0 * COUNT_IF(g.LABEL = 'Anomaly')
        / NULLIF(COUNT(*), 0),
        2
    ) AS PRECISION_PCT,

    ROUND(
        100.0 * COUNT_IF(g.LABEL = 'Anomaly')
        /
        NULLIF(
            (
                SELECT COUNT(*)
                FROM GROUND_TRUTH_LABELS
                WHERE LABEL = 'Anomaly'
                  AND BLOCK_ID IN (
                      SELECT BLOCK_ID
                      FROM BLOCK_BEHAVIOR_DERIVED
                  )
            ),
            0
        ),
        2
    ) AS RECALL_PCT

FROM BLOCK_BEHAVIOR_DERIVED b

JOIN GROUND_TRUTH_LABELS g
    ON b.BLOCK_ID = g.BLOCK_ID

CROSS JOIN (
    SELECT 15 AS DURATION_THRESHOLD
    UNION ALL SELECT 30
    UNION ALL SELECT 45
    UNION ALL SELECT 60
    UNION ALL SELECT 90
    UNION ALL SELECT 120
) t

WHERE b.ACTIVE_DURATION_SECONDS <= t.DURATION_THRESHOLD

GROUP BY t.DURATION_THRESHOLD

ORDER BY t.DURATION_THRESHOLD;

-- =====================================================
-- SECTION 12: DURATION + EVENT RATE THRESHOLD GRID
-- =====================================================
-- Purpose:
-- Test combinations of the two strongest behavioral
-- signals before constructing the final anomaly score.
-- =====================================================

WITH THRESHOLDS AS (

    SELECT 10 AS DURATION_THRESHOLD, 0.50 AS EPS_THRESHOLD
    UNION ALL SELECT 10, 0.75
    UNION ALL SELECT 10, 1.00
    UNION ALL SELECT 10, 1.25

    UNION ALL SELECT 15, 0.50
    UNION ALL SELECT 15, 0.75
    UNION ALL SELECT 15, 1.00
    UNION ALL SELECT 15, 1.25

    UNION ALL SELECT 20, 0.50
    UNION ALL SELECT 20, 0.75
    UNION ALL SELECT 20, 1.00
    UNION ALL SELECT 20, 1.25

    UNION ALL SELECT 30, 0.50
    UNION ALL SELECT 30, 0.75
    UNION ALL SELECT 30, 1.00
    UNION ALL SELECT 30, 1.25
),

TOTALS AS (

    SELECT
        COUNT_IF(LABEL = 'Anomaly') AS TOTAL_ANOMALIES
    FROM GROUND_TRUTH_LABELS
    WHERE BLOCK_ID IN (
        SELECT BLOCK_ID
        FROM BLOCK_BEHAVIOR_DERIVED
    )

)

SELECT
    t.DURATION_THRESHOLD,
    t.EPS_THRESHOLD,

    COUNT(*) AS FLAGGED_BLOCKS,

    COUNT_IF(g.LABEL = 'Anomaly')
        AS TRUE_POSITIVES,

    COUNT_IF(g.LABEL = 'Normal')
        AS FALSE_POSITIVES,

    ROUND(
        100.0 *
        COUNT_IF(g.LABEL = 'Anomaly')
        / NULLIF(COUNT(*), 0),
        2
    ) AS PRECISION_PCT,

    ROUND(
        100.0 *
        COUNT_IF(g.LABEL = 'Anomaly')
        / NULLIF(MAX(x.TOTAL_ANOMALIES), 0),
        2
    ) AS RECALL_PCT,

    ROUND(
        (
            2 *
            (
                COUNT_IF(g.LABEL = 'Anomaly')
                / NULLIF(COUNT(*), 0)
            )
            *
            (
                COUNT_IF(g.LABEL = 'Anomaly')
                / NULLIF(MAX(x.TOTAL_ANOMALIES), 0)
            )
        )
        /
        NULLIF(
            (
                COUNT_IF(g.LABEL = 'Anomaly')
                / NULLIF(COUNT(*), 0)
            )
            +
            (
                COUNT_IF(g.LABEL = 'Anomaly')
                / NULLIF(MAX(x.TOTAL_ANOMALIES), 0)
            ),
            0
        ),
        4
    ) AS F1_SCORE

FROM BLOCK_BEHAVIOR_DERIVED b

JOIN GROUND_TRUTH_LABELS g
    ON b.BLOCK_ID = g.BLOCK_ID

CROSS JOIN THRESHOLDS t

CROSS JOIN TOTALS x

WHERE
    b.ACTIVE_DURATION_SECONDS <= t.DURATION_THRESHOLD
    AND b.EVENTS_PER_SECOND >= t.EPS_THRESHOLD

GROUP BY
    t.DURATION_THRESHOLD,
    t.EPS_THRESHOLD

ORDER BY
    F1_SCORE DESC,
    PRECISION_PCT DESC;

-- =====================================================
-- SECTION 13: MULTI-SIGNAL BEHAVIORAL ANOMALY SCORE
-- =====================================================
-- Duration is the strongest observed feature.
-- Event rate provides supporting evidence.
--
-- Score range: 0-6
-- =====================================================

CREATE OR REPLACE VIEW BLOCK_ANOMALY_SCORES AS

SELECT
    BLOCK_ID,
    EVENT_COUNT,
    COMPONENT_COUNT,
    ACTIVE_DURATION_SECONDS,
    EVENTS_PER_SECOND,
    EVENTS_PER_COMPONENT,
    COMPONENT_DIVERSITY,

    (
        -- Strong duration signal
        CASE
            WHEN ACTIVE_DURATION_SECONDS <= 10 THEN 3
            WHEN ACTIVE_DURATION_SECONDS <= 15 THEN 2
            WHEN ACTIVE_DURATION_SECONDS <= 30 THEN 1
            ELSE 0
        END

        +

        -- Event-rate signal
        CASE
            WHEN EVENTS_PER_SECOND >= 1.25 THEN 2
            WHEN EVENTS_PER_SECOND >= 0.75 THEN 1
            ELSE 0
        END

        +

        -- Small supporting diversity signal
        CASE
            WHEN COMPONENT_DIVERSITY < 0.35 THEN 1
            ELSE 0
        END

    ) AS ANOMALY_SCORE

FROM BLOCK_BEHAVIOR_DERIVED;

-- =====================================================
-- SECTION 14: SCORE DISTRIBUTION
-- =====================================================

SELECT
    ANOMALY_SCORE,

    COUNT(*) AS BLOCKS,

    COUNT_IF(g.LABEL = 'Anomaly') AS ANOMALIES,
    COUNT_IF(g.LABEL = 'Normal')  AS NORMALS,

    ROUND(
        100.0 * COUNT_IF(g.LABEL = 'Anomaly')
        / NULLIF(COUNT(*), 0),
        2
    ) AS ANOMALY_RATE_PCT

FROM BLOCK_ANOMALY_SCORES s

JOIN GROUND_TRUTH_LABELS g
    ON s.BLOCK_ID = g.BLOCK_ID

GROUP BY ANOMALY_SCORE
ORDER BY ANOMALY_SCORE;

-- =====================================================
-- SECTION 15: DECODE HIGH-VALUE ANOMALY SCORE PATTERNS
-- =====================================================

SELECT
    s.ANOMALY_SCORE,

    CASE
        WHEN s.ACTIVE_DURATION_SECONDS <= 10 THEN '<=10s'
        WHEN s.ACTIVE_DURATION_SECONDS <= 15 THEN '11-15s'
        WHEN s.ACTIVE_DURATION_SECONDS <= 30 THEN '16-30s'
        ELSE '>30s'
    END AS DURATION_BAND,

    CASE
        WHEN s.EVENTS_PER_SECOND >= 1.25 THEN '>=1.25'
        WHEN s.EVENTS_PER_SECOND >= 0.75 THEN '0.75-1.25'
        ELSE '<0.75'
    END AS EPS_BAND,

    CASE
        WHEN s.COMPONENT_DIVERSITY < 0.35 THEN '<0.35'
        ELSE '>=0.35'
    END AS DIVERSITY_BAND,

    COUNT(*) AS BLOCKS,

    COUNT_IF(g.LABEL = 'Anomaly') AS ANOMALIES,

    COUNT_IF(g.LABEL = 'Normal') AS NORMALS,

    ROUND(
        100.0 * COUNT_IF(g.LABEL = 'Anomaly')
        / NULLIF(COUNT(*), 0),
        2
    ) AS ANOMALY_RATE_PCT

FROM BLOCK_ANOMALY_SCORES s

JOIN GROUND_TRUTH_LABELS g
    ON s.BLOCK_ID = g.BLOCK_ID

WHERE s.ANOMALY_SCORE >= 2

GROUP BY
    s.ANOMALY_SCORE,
    DURATION_BAND,
    EPS_BAND,
    DIVERSITY_BAND

ORDER BY
    ANOMALY_RATE_PCT DESC,
    BLOCKS DESC;

-- ============================================================
-- SECTION 16: CANDIDATE BEHAVIORAL DETECTOR EVALUATION
-- ============================================================
-- Compare several interpretable behavioral rules against
-- ground truth before selecting a production detector.
-- ============================================================

WITH candidates AS (

    SELECT
        b.BLOCK_ID,
        g.LABEL,

        -- Candidate A:
        -- strongest reasonably-sized anomaly pocket
        IFF(
            b.ACTIVE_DURATION_SECONDS <= 10
            AND b.EVENTS_PER_SECOND < 0.75
            AND b.COMPONENT_DIVERSITY < 0.35,
            1, 0
        ) AS RULE_A,

        -- Candidate B:
        -- broaden EPS slightly
        IFF(
            b.ACTIVE_DURATION_SECONDS <= 10
            AND b.EVENTS_PER_SECOND < 1.25
            AND b.COMPONENT_DIVERSITY < 0.35,
            1, 0
        ) AS RULE_B,

        -- Candidate C:
        -- short-lived + low diversity
        IFF(
            b.ACTIVE_DURATION_SECONDS <= 15
            AND b.COMPONENT_DIVERSITY < 0.35,
            1, 0
        ) AS RULE_C,

        -- Candidate D:
        -- hybrid based on the strongest discovered pockets
        IFF(
            (
                b.ACTIVE_DURATION_SECONDS <= 10
                AND b.EVENTS_PER_SECOND < 1.25
                AND b.COMPONENT_DIVERSITY < 0.35
            )
            OR
            (
                b.ACTIVE_DURATION_SECONDS BETWEEN 11 AND 15
                AND b.EVENTS_PER_SECOND >= 1.25
                AND b.COMPONENT_DIVERSITY >= 0.35
            ),
            1, 0
        ) AS RULE_D

    FROM BLOCK_BEHAVIOR_DERIVED b

    JOIN GROUND_TRUTH_LABELS g
        ON b.BLOCK_ID = g.BLOCK_ID
),

rules AS (

    SELECT BLOCK_ID, LABEL, 'A_STRICT_PATTERN' AS RULE_NAME, RULE_A AS PREDICTED
    FROM candidates

    UNION ALL

    SELECT BLOCK_ID, LABEL, 'B_BROAD_EPS', RULE_B
    FROM candidates

    UNION ALL

    SELECT BLOCK_ID, LABEL, 'C_SHORT_LOW_DIVERSITY', RULE_C
    FROM candidates

    UNION ALL

    SELECT BLOCK_ID, LABEL, 'D_HYBRID_PATTERN', RULE_D
    FROM candidates
),

metrics AS (

    SELECT
        RULE_NAME,

        COUNT_IF(PREDICTED = 1 AND LABEL = 'Anomaly') AS TRUE_POSITIVES,

        COUNT_IF(PREDICTED = 1 AND LABEL = 'Normal') AS FALSE_POSITIVES,

        COUNT_IF(PREDICTED = 0 AND LABEL = 'Anomaly') AS FALSE_NEGATIVES,

        COUNT_IF(PREDICTED = 0 AND LABEL = 'Normal') AS TRUE_NEGATIVES

    FROM rules

    GROUP BY RULE_NAME
)

SELECT
    RULE_NAME,

    TRUE_POSITIVES,
    FALSE_POSITIVES,
    FALSE_NEGATIVES,
    TRUE_NEGATIVES,

    TRUE_POSITIVES + FALSE_POSITIVES AS FLAGGED_BLOCKS,

    ROUND(
        100.0 * TRUE_POSITIVES /
        NULLIF(TRUE_POSITIVES + FALSE_POSITIVES, 0),
        2
    ) AS PRECISION_PCT,

    ROUND(
        100.0 * TRUE_POSITIVES /
        NULLIF(TRUE_POSITIVES + FALSE_NEGATIVES, 0),
        2
    ) AS RECALL_PCT,

    ROUND(
        2 *
        (
            TRUE_POSITIVES /
            NULLIF(TRUE_POSITIVES + FALSE_POSITIVES, 0)
        ) *
        (
            TRUE_POSITIVES /
            NULLIF(TRUE_POSITIVES + FALSE_NEGATIVES, 0)
        )
        /
        NULLIF(
            (
                TRUE_POSITIVES /
                NULLIF(TRUE_POSITIVES + FALSE_POSITIVES, 0)
            )
            +
            (
                TRUE_POSITIVES /
                NULLIF(TRUE_POSITIVES + FALSE_NEGATIVES, 0)
            ),
            0
        ),
        4
    ) AS F1_SCORE

FROM metrics

ORDER BY F1_SCORE DESC;

-- ============================================================
-- SECTION 17: PRODUCTION BEHAVIORAL ANOMALY DETECTOR
-- ============================================================
-- Selected from Section 16 evaluation.
--
-- Rule C:
--   ACTIVE_DURATION_SECONDS <= 15
--   COMPONENT_DIVERSITY < 0.35
--
-- High-confidence signal:
--   Rule D pattern
-- ============================================================

CREATE OR REPLACE VIEW INCIDENTS_BEHAVIORAL AS

SELECT
    b.BLOCK_ID,

    b.EVENT_COUNT,
    b.COMPONENT_COUNT,
    b.ACTIVE_DURATION_SECONDS,
    b.EVENTS_PER_SECOND,
    b.COMPONENT_DIVERSITY,

    TRUE AS IS_INCIDENT,

    CASE
        WHEN
            (
                b.ACTIVE_DURATION_SECONDS <= 10
                AND b.EVENTS_PER_SECOND < 1.25
                AND b.COMPONENT_DIVERSITY < 0.35
            )
            OR
            (
                b.ACTIVE_DURATION_SECONDS BETWEEN 11 AND 15
                AND b.EVENTS_PER_SECOND >= 1.25
                AND b.COMPONENT_DIVERSITY >= 0.35
            )
        THEN 'HIGH'

        ELSE 'MEDIUM'
    END AS SEVERITY,

    'SHORT_DURATION_LOW_COMPONENT_DIVERSITY'
        AS DETECTION_REASON,

    CURRENT_TIMESTAMP() AS DETECTED_AT

FROM BLOCK_BEHAVIOR_DERIVED b

WHERE
    b.ACTIVE_DURATION_SECONDS <= 15
    AND b.COMPONENT_DIVERSITY < 0.35;

-- ============================================================
-- SECTION 18: PRODUCTION VIEW VALIDATION
-- ============================================================

SELECT
    COUNT(*) AS INCIDENT_ROWS,
    COUNT(DISTINCT BLOCK_ID) AS UNIQUE_BLOCKS,

    COUNT_IF(SEVERITY = 'HIGH') AS HIGH_SEVERITY,
    COUNT_IF(SEVERITY = 'MEDIUM') AS MEDIUM_SEVERITY,

    ROUND(AVG(ACTIVE_DURATION_SECONDS), 2)
        AS AVG_DURATION_SECONDS,

    ROUND(AVG(EVENTS_PER_SECOND), 4)
        AS AVG_EVENTS_PER_SECOND,

    ROUND(AVG(COMPONENT_DIVERSITY), 4)
        AS AVG_COMPONENT_DIVERSITY

FROM INCIDENTS_BEHAVIORAL;