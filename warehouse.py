import os
import snowflake.connector

def fetch_all_warehouses():
    try:
        # Fetch Snowflake connection parameters from environment variables
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
        password = os.getenv("SNOWFLAKE_PASSWORD")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        
        # Establish Snowflake connection
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )

        # Execute SQL query to fetch warehouses
        cursor = conn.cursor()
        cursor.execute("SHOW WAREHOUSES")
        
        # Fetch and return the warehouses
        warehouses = cursor.fetchall()
        return warehouses
        
    except Exception as e:
        print("An error occurred:", str(e))
        return None
    finally:
        # Close connection
        conn.close()

if __name__ == "__main__":
    warehouses = fetch_all_warehouses()
    if warehouses:
        print("Fetched warehouses successfully:")
        for warehouse in warehouses:
            print(warehouse[1])  # Assuming the name of the warehouse is in the second column
    else:
        print("Failed to fetch warehouses.")
