import streamlit as st
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta

# Streamlit app title
st.title("Warehouse Watcher")
st.markdown("Monitor Snowflake warehouse metrics in real-time.")

# Sidebar for connection settings
st.sidebar.header("Warehouse Connection Settings")
account = st.sidebar.text_input("Snowflake Account", value="your_account_name")
user = st.sidebar.text_input("Username", value="your_username")
password = st.sidebar.text_input("Password", type="password")
warehouse = st.sidebar.text_input("Warehouse Name", value="your_warehouse")
database = st.sidebar.text_input("Database Name", value="your_database")
schema = st.sidebar.text_input("Schema Name", value="public")

@st.cache_data(show_spinner=True)
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

# Fetch data function
def fetch_data(query):
    conn = get_snowflake_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    return pd.DataFrame(data, columns=columns)

# Display Warehouse Stats
st.header("Warehouse Stats")

# Active Queries
st.subheader("Active Queries")
active_queries_query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, EXECUTION_STATUS, QUEUED_PROVISIONING_TIME, TOTAL_ELAPSED_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE WAREHOUSE_NAME = '{}' AND EXECUTION_STATUS = 'RUNNING' AND START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP)
    ORDER BY START_TIME DESC;
""".format(warehouse)
active_queries = fetch_data(active_queries_query)
if not active_queries.empty:
    st.dataframe(active_queries)
else:
    st.write("No active queries found.")

# Warehouse Utilization
st.subheader("Warehouse Utilization in Last 24 Hours")
utilization_query = """
    SELECT TO_TIMESTAMP(HOUR) AS HOUR, AVG(AVG_RUNNING) AS AVG_RUNNING, AVG(AVG_QUEUED_LOAD) AS AVG_QUEUED_LOAD
    FROM SNOWFLAKE.WAREHOUSE_METERING_HISTORY
    WHERE WAREHOUSE_NAME = '{}' AND HOUR >= DATEADD(day, -1, CURRENT_TIMESTAMP)
    GROUP BY HOUR
    ORDER BY HOUR;
""".format(warehouse)
utilization_data = fetch_data(utilization_query)
if not utilization_data.empty:
    st.line_chart(utilization_data.set_index("HOUR"))
else:
    st.write("No utilization data found.")

# Recent Query History
st.subheader("Recent Query History")
recent_queries_query = """
    SELECT QUERY_ID, USER_NAME, WAREHOUSE_NAME, EXECUTION_STATUS, TOTAL_ELAPSED_TIME, START_TIME
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE WAREHOUSE_NAME = '{}' AND START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP)
    ORDER BY START_TIME DESC;
""".format(warehouse)
recent_queries = fetch_data(recent_queries_query)
if not recent_queries.empty:
    st.dataframe(recent_queries)
else:
    st.write("No recent query history found.")

st.sidebar.markdown("Note: Ensure you have the required Snowflake permissions.")
