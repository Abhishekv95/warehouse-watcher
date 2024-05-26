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
