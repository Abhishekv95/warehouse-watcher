# Warehouse Watcher

This project monitors Snowflake warehouse health and utilization in real-time.

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
