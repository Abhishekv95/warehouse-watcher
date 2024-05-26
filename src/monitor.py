import schedule
import time
from .warehouse import fetch_all_warehouses, fetch_warehouse_utilization

def monitor_warehouses():
    warehouses = fetch_all_warehouses()
    if warehouses:
        print("Monitoring warehouses:")
        for warehouse in warehouses:
            warehouse_name = warehouse[1]  # Assuming the name of the warehouse is in the second column
            details = fetch_warehouse_utilization(warehouse_name)
            if details:
                print(f"Warehouse: {details[1]}")
                print(f"State: {details[2]}")
                print(f"Size: {details[3]}")
                print(f"Credit Used: {details[4]}")
                print(f"Credit Quota: {details[5]}")
                print(f"Utilization: {details[6]}")
            else:
                print(f"Failed to fetch details for warehouse {warehouse_name}")
    else:
        print("Failed to fetch warehouses.")

schedule.every(5).minutes.do(monitor_warehouses)

if __name__ == "__main__":
    print("Starting warehouse monitoring...")
    while True:
        schedule.run_pending()
        time.sleep(1)
