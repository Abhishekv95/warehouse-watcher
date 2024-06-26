import snowflake.connector
from .config import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
)

def fetch_all_warehouses():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        cursor = conn.cursor()
        cursor.execute("SHOW WAREHOUSES")
        warehouses = cursor.fetchall()
        return warehouses
    except Exception as e:
        print("An error occurred:", str(e))
        return None
    finally:
        conn.close()

def fetch_warehouse_utilization(warehouse_name):
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        cursor = conn.cursor()
        cursor.execute(f"SHOW WAREHOUSES LIKE '{warehouse_name}'")
        warehouse_details = cursor.fetchone()
        return warehouse_details
    except Exception as e:
        print(f"An error occurred while fetching utilization for warehouse {warehouse_name}:", str(e))
        return None
    finally:
        conn.close()

def fetch_query_spillover():
    query = """
    SELECT
        query_id,
        warehouse_name,
        spilled_bytes,
        spilled_partitions
    FROM
        table(information_schema.query_history())
    WHERE
        spilled_bytes > 0
    ORDER BY
        start_time DESC;
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        cursor = conn.cursor()
        cursor.execute(query)
        spillover_details = cursor.fetchall()
        return spillover_details
    except Exception as e:
        print("An error occurred while fetching query spillover:", str(e))
        return None
    finally:
        conn.close()

def fetch_query_concurrency():
    query = """
    SELECT
        start_time,
        warehouse_name,
        COUNT(query_id) AS concurrent_queries
    FROM
        table(information_schema.query_history())
    GROUP BY
        start_time, warehouse_name
    ORDER BY
        start_time DESC
    LIMIT 10;
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        cursor = conn.cursor()
        cursor.execute(query)
        concurrency_details = cursor.fetchall()
        return concurrency_details
    except Exception as e:
        print("An error occurred while fetching query concurrency:", str(e))
        return None
    finally:
        conn.close()

def fetch_long_running_queries(threshold_seconds=300):
    query = """
    SELECT
        query_id,
        warehouse_name,
        user_name,
        execution_status,
        total_elapsed_time/1000 as elapsed_seconds
    FROM
        table(information_schema.query_history())
    WHERE
        total_elapsed_time > %s * 1000
    ORDER BY
        start_time DESC;
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        cursor = conn.cursor()
        cursor.execute(query, (threshold_seconds,))
        long_running_queries = cursor.fetchall()
        return long_running_queries
    except Exception as e:
        print("An error occurred while fetching long-running queries:", str(e))
        return None
    finally:
        conn.close()

def fetch_warehouse_credit_usage():
    query = """
    SELECT
        warehouse_name,
        SUM(credits_used) AS total_credits
    FROM
        warehouse_metering_history
    GROUP BY
        warehouse_name
    ORDER BY
        total_credits DESC;
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        cursor = conn.cursor()
        cursor.execute(query)
        credit_usage = cursor.fetchall()
        return credit_usage
    except Exception as e:
        print("An error occurred while fetching warehouse credit usage:", str(e))
        return None
    finally:
        conn.close()

def fetch_query_errors():
    query = """
    SELECT
        query_id,
        user_name,
        error_code,
        error_message
    FROM
        table(information_schema.query_history())
    WHERE
        error_code IS NOT NULL
    ORDER BY
        start_time DESC;
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
        )
        cursor = conn.cursor()
        cursor.execute(query)
        query_errors = cursor.fetchall()
        return query_errors
    except Exception as e:
        print("An error occurred while fetching query errors:", str(e))
        return None
    finally:
        conn.close()
