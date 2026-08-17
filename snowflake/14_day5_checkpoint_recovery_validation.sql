USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PULSEOPS_WH;
USE DATABASE PULSEOPS;
USE SCHEMA CORE;

-- ============================================================
-- PulseOps Day 5
-- Kafka + Spark Structured Streaming checkpoint recovery test
-- ============================================================

SELECT
    COUNT(*) AS RAW_ROWS_AFTER_RECOVERY,
    COUNT(DISTINCT BLOCK_ID) AS UNIQUE_BLOCKS_AFTER_RECOVERY
FROM RAW_LOG_EVENTS;

SELECT
    COUNT(*) AS RAW_ROWS_BEFORE_RECOVERY_TEST
FROM RAW_LOG_EVENTS;