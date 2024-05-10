import os
import requests

def fetch_all_warehouses(api_url):
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            warehouses = response.json()
            return warehouses
        else:
            print("Failed to fetch warehouses. Status code:", response.status_code)
            return None
    except Exception as e:
        print("An error occurred:", str(e))
        return None

if __name__ == "__main__":
    # Fetch API URL from environment variable
    api_url = os.getenv("WAREHOUSE_API_URL")
    if not api_url:
        print("Error: Environment variable WAREHOUSE_API_URL is not set.")
        exit(1)

    warehouses = fetch_all_warehouses(api_url)
    if warehouses:
        print("Fetched warehouses successfully:")
        for warehouse in warehouses:
            print(warehouse)
    else:
        print("Failed to fetch warehouses.")
