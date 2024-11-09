import streamlit as st
import pandas as pd
from warehouse import (
    fetch_all_warehouses,
    fetch_warehouse_utilization,
    fetch_query_spillover,
    fetch_query_concurrency,
    fetch_long_running_queries,
    fetch_warehouse_credit_usage,
    fetch_query_errors,
    fetch_warehouse_health,
    fetch_historical_usage,
    fetch_idle_warehouses,
    fetch_user_activity_log,
    fetch_latest_failures
)

# Streamlit app title and description
st.title("Warehouse Watcher")
st.markdown("Real-time monitoring of Snowflake warehouse metrics and performance insights.")

# Display all warehouses
st.header("Available Warehouses")
warehouses = fetch_all_warehouses()
if warehouses:
    st.table(pd.DataFrame(warehouses, columns=["Name", "State", "Size", "Credits Used", "Created On"]))
else:
    st.write("No warehouse data available.")

# Display warehouse utilization
st.subheader("Warehouse Utilization")
warehouse_utilization = fetch_warehouse_utilization()
if warehouse_utilization:
    st.table(pd.DataFrame(warehouse_utilization, columns=["Warehouse Name", "Utilization (%)"]))
else:
    st.write("No utilization data available.")

# New Feature: Warehouse Health Check
st.subheader("Warehouse Health Check")
health_data = fetch_warehouse_health()
if health_data:
    st.table(pd.DataFrame(health_data, columns=["Warehouse Name", "Queued Loads", "Running", "Queued"]))
else:
    st.write("No health data available.")

# New Feature: Historical Usage Trends
st.subheader("Historical Usage Trends")
days = st.slider("Select number of days for historical data", 1, 30, 7)
historical_usage = fetch_historical_usage(days)
if historical_usage:
    st.table(pd.DataFrame(historical_usage, columns=["Warehouse Name", "Day", "Daily Credits Used"]))
else:
    st.write("No historical data available.")

# New Feature: Idle Warehouses
st.subheader("Idle Warehouses")
idle_warehouses = fetch_idle_warehouses()
if idle_warehouses:
    st.table(pd.DataFrame(idle_warehouses, columns=["Warehouse Name", "State", "Last Active Time"]))
else:
    st.write("No idle warehouses found.")

# New Feature: User Activity Log
st.subheader("User Activity Log")
user_activity_log = fetch_user_activity_log()
if user_activity_log:
    st.table(pd.DataFrame(user_activity_log, columns=["User Name", "Total Queries", "Total Execution Time (s)"]))
else:
    st.write("No user activity log data available.")

# New Feature: Latest Failures
st.subheader("Latest Failures")
latest_failures = fetch_latest_failures()
if latest_failures:
    st.table(pd.DataFrame(latest_failures, columns=["Query ID", "User Name", "Error Code", "Error Message", "Start Time"]))
else:
    st.write("No recent failures found.")

# Other Monitoring Features
st.subheader("Query Spillover")
spillover_data = fetch_query_spillover()
if spillover_data:
    st.table(pd.DataFrame(spillover_data, columns=["Warehouse Name", "Spillover Queries"]))
else:
    st.write("No spillover data available.")

st.subheader("Query Concurrency")
concurrency_data = fetch_query_concurrency()
if concurrency_data:
    st.table(pd.DataFrame(concurrency_data, columns=["Warehouse Name", "Concurrency Level"]))
else:
    st.write("No concurrency data available.")

st.subheader("Long-Running Queries")
long_running_queries = fetch_long_running_queries()
if long_running_queries:
    st.table(pd.DataFrame(long_running_queries, columns=["Query ID", "Warehouse Name", "Duration"]))
else:
    st.write("No long-running queries data available.")

st.subheader("Warehouse Credit Usage")
credit_usage = fetch_warehouse_credit_usage()
if credit_usage:
    st.table(pd.DataFrame(credit_usage, columns=["Warehouse Name", "Total Credits Used"]))
else:
    st.write("No credit usage data available.")

st.subheader("Query Errors")
query_errors = fetch_query_errors()
if query_errors:
    st.table(pd.DataFrame(query_errors, columns=["Query ID", "User Name", "Error Message", "Timestamp"]))
else:
    st.write("No query error data available.")
