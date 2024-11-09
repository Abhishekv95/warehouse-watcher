import streamlit as st
import pandas as pd
from warehouse import (
    fetch_all_warehouses,
    fetch_warehouse_utilization,
    fetch_query_spillover,
    fetch_query_concurrency,
    fetch_long_running_queries,
    fetch_warehouse_credit_usage,
    fetch_query_errors
)

# Streamlit app title and description
st.title("Warehouse Watcher")
st.markdown("Real-time monitoring of Snowflake warehouse metrics.")

# Enhanced Features
st.header("Warehouse Metrics Dashboard")

# Display all warehouses
st.subheader("Available Warehouses")
warehouses = fetch_all_warehouses()
if warehouses:
    st.table(pd.DataFrame(warehouses, columns=["Name", "State", "Size", "Credits Used", "Created On"]))
else:
    st.write("No warehouse data available.")

# User input for warehouse details
st.subheader("Warehouse Utilization")
warehouse_name = st.text_input("Enter warehouse name for utilization details", "")
if warehouse_name:
    utilization = fetch_warehouse_utilization(warehouse_name)
    if utilization:
        st.table(pd.DataFrame([utilization], columns=["Name", "State", "Size", "Credits Used", "Created On"]))
    else:
        st.write(f"No utilization data available for warehouse '{warehouse_name}'.")

# Query spillover
st.subheader("Query Spillover")
spillover_data = fetch_query_spillover()
if spillover_data:
    st.table(pd.DataFrame(spillover_data, columns=["Query ID", "Warehouse Name", "Spilled Bytes", "Spilled Partitions"]))
else:
    st.write("No query spillover data available.")

# Query concurrency
st.subheader("Query Concurrency")
concurrency_data = fetch_query_concurrency()
if concurrency_data:
    st.table(pd.DataFrame(concurrency_data, columns=["Start Time", "Warehouse Name", "Concurrent Queries"]))
else:
    st.write("No query concurrency data available.")

# Long-running queries
st.subheader("Long-Running Queries")
threshold_seconds = st.number_input("Set threshold for long-running queries (seconds)", min_value=60, value=300, step=60)
long_running_queries = fetch_long_running_queries(threshold_seconds)
if long_running_queries:
    st.table(pd.DataFrame(long_running_queries, columns=["Query ID", "Warehouse Name", "User Name", "Execution Status", "Elapsed Time (s)"]))
else:
    st.write(f"No long-running queries exceeding {threshold_seconds} seconds.")

# Warehouse credit usage
st.subheader("Warehouse Credit Usage")
credit_usage = fetch_warehouse_credit_usage()
if credit_usage:
    st.table(pd.DataFrame(credit_usage, columns=["Warehouse Name", "Total Credits Used"]))
else:
    st.write("No credit usage data available.")

# Query errors
st.subheader("Recent Query Errors")
query_errors = fetch_query_errors()
if query_errors:
    st.table(pd.DataFrame(query_errors, columns=["Query ID", "User Name", "Error Code", "Error Message"]))
else:
    st.write("No recent query errors found.")

# Note for user
st.sidebar.info("Ensure Snowflake permissions are configured for data access.")
