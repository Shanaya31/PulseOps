USE ROLE ACCOUNTADMIN;

USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;


-- =========================================================
-- PulseOps analytical star-schema refresh
--
-- DDL lives only in schema.sql.
-- This file contains data refresh logic only.
-- Safe to rerun.
-- =========================================================


-- =========================================================
-- 1. Refresh DIM_BLOCK
-- =========================================================

MERGE INTO PULSEOPS.CORE.DIM_BLOCK AS target

USING (
    SELECT
        g.BLOCK_ID,
        g.GROUND_TRUTH_LABEL,
        r.FIRST_SEEN_AT,
        r.LAST_SEEN_AT
    FROM (
        SELECT
            BLOCK_ID,
            MAX(LABEL) AS GROUND_TRUTH_LABEL
        FROM PULSEOPS.CORE.GROUND_TRUTH_LABELS
        WHERE BLOCK_ID IS NOT NULL
        GROUP BY BLOCK_ID
    ) AS g

    LEFT JOIN (
        SELECT
            BLOCK_ID,
            MIN(EVENT_TIME) AS FIRST_SEEN_AT,
            MAX(EVENT_TIME) AS LAST_SEEN_AT
        FROM PULSEOPS.CORE.RAW_LOG_EVENTS
        WHERE BLOCK_ID IS NOT NULL
        GROUP BY BLOCK_ID
    ) AS r
        ON g.BLOCK_ID = r.BLOCK_ID

) AS source

ON target.BLOCK_ID = source.BLOCK_ID

WHEN MATCHED THEN
    UPDATE SET
        target.GROUND_TRUTH_LABEL = source.GROUND_TRUTH_LABEL,
        target.FIRST_SEEN_AT =
            COALESCE(source.FIRST_SEEN_AT, target.FIRST_SEEN_AT),
        target.LAST_SEEN_AT =
            COALESCE(source.LAST_SEEN_AT, target.LAST_SEEN_AT)

WHEN NOT MATCHED THEN
    INSERT (
        BLOCK_ID,
        GROUND_TRUTH_LABEL,
        FIRST_SEEN_AT,
        LAST_SEEN_AT
    )
    VALUES (
        source.BLOCK_ID,
        source.GROUND_TRUTH_LABEL,
        source.FIRST_SEEN_AT,
        source.LAST_SEEN_AT
    );


-- =========================================================
-- 2. Refresh DIM_DATE
-- =========================================================

MERGE INTO PULSEOPS.CORE.DIM_DATE AS target

USING (
    SELECT DISTINCT
        TO_DATE(WINDOW_START) AS FULL_DATE
    FROM PULSEOPS.CORE.WINDOWED_METRICS
    WHERE WINDOW_START IS NOT NULL
) AS source

ON target.FULL_DATE = source.FULL_DATE

WHEN NOT MATCHED THEN
    INSERT (
        DATE_KEY,
        FULL_DATE,
        YEAR_NUMBER,
        QUARTER_NUMBER,
        MONTH_NUMBER,
        MONTH_NAME,
        DAY_NUMBER,
        DAY_OF_WEEK,
        IS_WEEKEND
    )
    VALUES (
        TO_NUMBER(TO_CHAR(source.FULL_DATE, 'YYYYMMDD')),
        source.FULL_DATE,
        YEAR(source.FULL_DATE),
        QUARTER(source.FULL_DATE),
        MONTH(source.FULL_DATE),
        MONTHNAME(source.FULL_DATE),
        DAY(source.FULL_DATE),
        DAYNAME(source.FULL_DATE),
        IFF(
            DAYOFWEEKISO(source.FULL_DATE) IN (6, 7),
            TRUE,
            FALSE
        )
    );


-- =========================================================
-- 3. Refresh DIM_COMPONENT
-- =========================================================

MERGE INTO PULSEOPS.CORE.DIM_COMPONENT AS target

USING (
    SELECT DISTINCT
        COMPONENT AS COMPONENT_NAME
    FROM PULSEOPS.CORE.RAW_LOG_EVENTS
    WHERE COMPONENT IS NOT NULL
      AND TRIM(COMPONENT) <> ''
) AS source

ON target.COMPONENT_NAME = source.COMPONENT_NAME

WHEN NOT MATCHED THEN
    INSERT (
        COMPONENT_NAME
    )
    VALUES (
        source.COMPONENT_NAME
    );


-- =========================================================
-- 4. Refresh FACT_INCIDENT_EVENTS
--
-- This section will naturally insert zero rows until
-- RAW_LOG_EVENTS and INCIDENTS contain streaming data.
-- =========================================================

MERGE INTO PULSEOPS.CORE.FACT_INCIDENT_EVENTS AS target

USING (

    SELECT
        db.BLOCK_KEY,
        dd.DATE_KEY,
        dc.COMPONENT_KEY,

        i.WINDOW_START,
        i.WINDOW_END,

        COUNT(r.EVENT_TIME) AS COMPONENT_EVENT_COUNT,

        COUNT_IF(
            UPPER(r.LEVEL) = 'ERROR'
        ) AS COMPONENT_ERROR_COUNT,

        COUNT_IF(
            UPPER(r.LEVEL) = 'WARN'
        ) AS COMPONENT_WARN_COUNT,

        i.EVENT_COUNT AS WINDOW_EVENT_COUNT,
        i.ERROR_COUNT AS WINDOW_ERROR_COUNT,
        i.WARN_COUNT AS WINDOW_WARN_COUNT,

        i.IS_INCIDENT,

        IFF(
            i.IS_INCIDENT = TRUE
            AND UPPER(db.GROUND_TRUTH_LABEL) = 'ANOMALY',
            TRUE,
            FALSE
        ) AS IS_TRUE_POSITIVE

    FROM INCIDENTS AS i

    INNER JOIN PULSEOPS.CORE.DIM_BLOCK AS db
        ON db.BLOCK_ID = i.BLOCK_ID

    INNER JOIN PULSEOPS.CORE.DIM_DATE AS dd
        ON dd.FULL_DATE = TO_DATE(i.WINDOW_START)

    INNER JOIN PULSEOPS.CORE.RAW_LOG_EVENTS AS r
        ON r.BLOCK_ID = i.BLOCK_ID
       AND r.EVENT_TIME >= i.WINDOW_START
       AND r.EVENT_TIME < i.WINDOW_END
       AND r.COMPONENT IS NOT NULL

    INNER JOIN PULSEOPS.CORE.DIM_COMPONENT AS dc
        ON dc.COMPONENT_NAME = r.COMPONENT

    GROUP BY
        db.BLOCK_KEY,
        dd.DATE_KEY,
        dc.COMPONENT_KEY,
        i.WINDOW_START,
        i.WINDOW_END,
        i.EVENT_COUNT,
        i.ERROR_COUNT,
        i.WARN_COUNT,
        i.IS_INCIDENT,
        db.GROUND_TRUTH_LABEL

) AS source

ON  target.BLOCK_KEY = source.BLOCK_KEY
AND target.DATE_KEY = source.DATE_KEY
AND target.COMPONENT_KEY = source.COMPONENT_KEY
AND target.WINDOW_START = source.WINDOW_START
AND target.WINDOW_END = source.WINDOW_END

WHEN MATCHED THEN
    UPDATE SET
        target.COMPONENT_EVENT_COUNT =
            source.COMPONENT_EVENT_COUNT,

        target.COMPONENT_ERROR_COUNT =
            source.COMPONENT_ERROR_COUNT,

        target.COMPONENT_WARN_COUNT =
            source.COMPONENT_WARN_COUNT,

        target.WINDOW_EVENT_COUNT =
            source.WINDOW_EVENT_COUNT,

        target.WINDOW_ERROR_COUNT =
            source.WINDOW_ERROR_COUNT,

        target.WINDOW_WARN_COUNT =
            source.WINDOW_WARN_COUNT,

        target.IS_INCIDENT =
            source.IS_INCIDENT,

        target.IS_TRUE_POSITIVE =
            source.IS_TRUE_POSITIVE,

        target.LOADED_AT =
            CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
    INSERT (
        BLOCK_KEY,
        DATE_KEY,
        COMPONENT_KEY,
        WINDOW_START,
        WINDOW_END,
        COMPONENT_EVENT_COUNT,
        COMPONENT_ERROR_COUNT,
        COMPONENT_WARN_COUNT,
        WINDOW_EVENT_COUNT,
        WINDOW_ERROR_COUNT,
        WINDOW_WARN_COUNT,
        IS_INCIDENT,
        IS_TRUE_POSITIVE,
        LOADED_AT
    )
    VALUES (
        source.BLOCK_KEY,
        source.DATE_KEY,
        source.COMPONENT_KEY,
        source.WINDOW_START,
        source.WINDOW_END,
        source.COMPONENT_EVENT_COUNT,
        source.COMPONENT_ERROR_COUNT,
        source.COMPONENT_WARN_COUNT,
        source.WINDOW_EVENT_COUNT,
        source.WINDOW_ERROR_COUNT,
        source.WINDOW_WARN_COUNT,
        source.IS_INCIDENT,
        source.IS_TRUE_POSITIVE,
        CURRENT_TIMESTAMP()
    );