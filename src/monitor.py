import schedule
import time
from .warehouse import fetch_all_warehouses, fetch_warehouse_utilization
from .logger import logger

UTILIZATION_THRESHOLD = 80  # Utilization threshold percentage for logging warnings

def monitor_warehouses():
    try:
        warehouses = fetch_all_warehouses()
        if not warehouses:
            logger.error("Failed to fetch warehouses.")
            return

        logger.info("Monitoring warehouses:")
        for warehouse in warehouses:
            warehouse_name = warehouse[1]  # Assuming the name of the warehouse is in the second column
            details = fetch_warehouse_utilization(warehouse_name)
            if not details:
                logger.error(f"Failed to fetch details for warehouse {warehouse_name}")
                continue

            name = details[1]
            state = details[2]
            size = details[3]
            credit_used = details[4]
            credit_quota = details[5]
            utilization = details[6]

            logger.info(f"Warehouse: {name}, State: {state}, Size: {size}, Credit Used: {credit_used}, Credit Quota: {credit_quota}, Utilization: {utilization}%")

            # Check utilization and log a warning if it exceeds the threshold
            if utilization > UTILIZATION_THRESHOLD:
                logger.warning(f"High Utilization Alert: Warehouse {name} utilization is at {utilization}%")

    except Exception as e:
        logger.error(f"An error occurred during warehouse monitoring: {e}")

# Schedule the monitoring function to run every 5 minutes
schedule.every(5).minutes.do(monitor_warehouses)

if __name__ == "__main__":
    logger.info("Starting warehouse monitoring...")
    while True:
        schedule.run_pending()
        time.sleep(1)
