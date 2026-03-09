-- ─────────────────────────────────────────────────────────────
-- queries.sql  —  Analytical SQL for the Weather ETL Project
-- Run these against weather.db using:
--   sqlite3 weather.db < sql/queries.sql
-- ─────────────────────────────────────────────────────────────


-- 1. VIEW ALL RECORDS (latest 10)
SELECT *
FROM   weather
ORDER  BY loaded_at DESC
LIMIT  10;


-- 2. FILTER: Only hot & humid readings
SELECT city, temp_c, humidity, description
FROM   weather
WHERE  temp_c > 25
  AND  humidity > 70
ORDER  BY temp_c DESC;


-- 3. AGGREGATE: Average stats per city
SELECT
    city,
    ROUND(AVG(temp_c), 2)    AS avg_temp_c,
    ROUND(AVG(humidity), 1)  AS avg_humidity,
    MAX(temp_c)              AS max_temp,
    MIN(temp_c)              AS min_temp,
    COUNT(*)                 AS total_records
FROM   weather
GROUP  BY city
ORDER  BY avg_temp_c DESC;


-- 4. PIPELINE AUDIT: Rows loaded per day
SELECT
    DATE(loaded_at) AS run_date,
    COUNT(*)        AS rows_loaded
FROM   weather
GROUP  BY DATE(loaded_at)
ORDER  BY run_date DESC;


-- 5. DATA QUALITY CHECK: Count nulls
SELECT
    SUM(CASE WHEN temp_c    IS NULL THEN 1 ELSE 0 END) AS null_temp,
    SUM(CASE WHEN humidity  IS NULL THEN 1 ELSE 0 END) AS null_humidity,
    SUM(CASE WHEN city      IS NULL THEN 1 ELSE 0 END) AS null_city
FROM   weather;
