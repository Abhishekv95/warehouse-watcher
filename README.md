# Warehouse Watcher

**Warehouse Watcher** is a tool to monitor Snowflake warehouse health and utilization in real-time, providing insights for better resource management and performance optimization.

## Features
- **Real-time Monitoring**: Tracks warehouse usage, credit consumption, and performance metrics.
- **Configurable Alerts**: Set custom thresholds for alerts.
- **Automated Logging**: Captures periodic logs for analysis and trend observation.

## Setup

### 1. Clone the Repository
```bash
git clone <repository_url>
cd warehouse-watcher


## Setup

1. Clone the repository:
    ```bash
    git clone <repository_url>
    cd warehouse-watcher
    ```

2. Create a virtual environment and activate it:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. Install the dependencies:
    ```bash
    pip install -r requirements.txt
    ```

4. Create a `.env` file in the root directory with your Snowflake credentials:
    ```dotenv
    SNOWFLAKE_ACCOUNT=your_account
    SNOWFLAKE_USER=your_user
    SNOWFLAKE_PASSWORD=your_password
    SNOWFLAKE_WAREHOUSE=your_warehouse
    SNOWFLAKE_DATABASE=your_database
    SNOWFLAKE_SCHEMA=your_schema
    ```

5. Run the monitor:
    ```bash
    python -m src.monitor
    ```

## Files

- `src/config.py`: Configuration handling.
- `src/warehouse.py`: Functions to fetch warehouse details.
- `src/monitor.py`: Monitoring script that runs periodically.

To-do 
Use Streamlit to create interactive dashboards for real-time warehouse utilization and performance metrics.
features: Display usage trends, peak hours, and alert thresholds in visually interactive charts and tables.
