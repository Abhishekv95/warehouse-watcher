# SQL Queries

# 1. Warehouse Utilization
warehouse_utilization_query = """
SELECT
    WAREHOUSE_NAME,
    AVG_RUNNING * 100 / AVG_CLUSTER_SIZE AS AVG_LOAD_PERCENT,
    SUM(EXECUTED) AS TOTAL_QUERIES,
    SUM(CREDITS_USED) AS TOTAL_CREDITS_USED,
    AVG(AVG_RUNNING) AS AVG_CONCURRENT_QUERIES,
    AVG(AVG_QUEUE_TIME/1000000000) AS AVG_QUEUE_TIME_SECS
FROM
    SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE
    START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP)  -- last 30 days
GROUP BY
    WAREHOUSE_NAME
ORDER BY
    TOTAL_CREDITS_USED DESC;
"""

# 2. Warehouse Performance by Hour
warehouse_performance_by_hour_query = """
SELECT
    WAREHOUSE_NAME,
    TO_CHAR(START_TIME, 'YYYY-MM-DD HH24') AS HOUR,
    AVG(AVG_RUNNING) AS AVG_RUNNING_QUERIES,
    MAX(AVG_RUNNING) AS MAX_RUNNING_QUERIES,
    SUM(CREDITS_USED) AS CREDITS_USED
FROM
    SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE
    START_TIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP)  -- last 7 days
GROUP BY
    WAREHOUSE_NAME,
    TO_CHAR(START_TIME, 'YYYY-MM-DD HH24')
ORDER BY
    WAREHOUSE_NAME,
    HOUR;
"""

# 3. Warehouse Credit Usage by Day
warehouse_credit_usage_by_day_query = """
SELECT
    WAREHOUSE_NAME,
    DATE(START_TIME) AS DATE,
    SUM(CREDITS_USED) AS CREDITS_USED
FROM
    SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE
    START_TIME >= DATEADD(MONTH, -1, CURRENT_DATE)  -- last 30 days
GROUP BY
    WAREHOUSE_NAME,
    DATE(START_TIME)
ORDER BY
    DATE DESC;
"""

# 4. Query History with Execution Times
query_history_execution_times_query = """
SELECT
    QUERY_ID,
    WAREHOUSE_NAME,
    USER_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    START_TIME,
    EXECUTION_TIME / 1000 AS EXECUTION_TIME_MS,
    CREDITS_USED,
    QUERY_TEXT
FROM
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE
    START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP)  -- last 30 days
    AND WAREHOUSE_NAME IS NOT NULL
ORDER BY
    START_TIME DESC
LIMIT 100;
"""

# 5. Warehouse Concurrency Levels
warehouse_concurrency_levels_query = """
SELECT
    WAREHOUSE_NAME,
    COUNT(*) AS HIGH_CONCURRENCY_EVENTS
FROM
    SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE
    AVG_RUNNING >= AVG_CLUSTER_SIZE * 0.8  -- warehouses running at 80% or more of their capacity
    AND START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP)  -- last 30 days
GROUP BY
    WAREHOUSE_NAME
ORDER BY
    HIGH_CONCURRENCY_EVENTS DESC;
"""

