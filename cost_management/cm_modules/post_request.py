from datetime import datetime, timedelta
from cm_modules.log import log
import pandas as pd
import requests
import time

def post_request(subscription_id, bearer_token, greit_connection_string, klant, bron, script, script_id, start_date_str, end_date_str):
    # Zet strings om naar datetime-objecten
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Maak de API URL
    cost_url = f'https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01'
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }

    # Verzamel alle data
    all_data = []

    # Loop door de maanden heen
    current_start = start_date
    while current_start <= end_date:
        current_end = (current_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
        if current_end > end_date:
            current_end = end_date

        print(f'Ophalen data van {current_start.strftime("%Y-%m-%d")} tot {current_end.strftime("%Y-%m-%d")}')

        cost_body = {
            "type": "ActualCost",
            "timeframe": "Custom",
            "timePeriod": {
                "from": current_start.strftime("%Y-%m-%d"),
                "to": current_end.strftime("%Y-%m-%d")
            },
            "dataset": {
                "granularity": "Daily",
                "aggregation": {
                    "totalCost": {
                        "name": "PreTaxCost",
                        "function": "Sum"
                    }
                },
                "grouping": [
                    {"type": "Dimension", "name": "ServiceName"},
                    {"type": "Dimension", "name": "ResourceGroup"}
                ]
            }
        }

        # Pagination voor de huidige maand
        next_link = None
        while True:
            if next_link:
                response = requests.post(next_link, headers=headers)
            else:
                response = requests.post(cost_url, json=cost_body, headers=headers)

            if response.status_code == 429:
                print(f"Te veel verzoeken. Wachten en opnieuw proberen...")
                time.sleep(10)
                continue

            if response.status_code != 200:
                print(f"Fout bij het ophalen van kosteninformatie: {response.status_code} | {response.text}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Fout bij het ophalen van kosteninformatie: {response.status_code}", script, script_id)
                exit(1)

            json_data = response.json()
            rows = json_data["properties"]["rows"]
            all_data.extend(rows)

            # Controleer op een volgende pagina
            next_link = json_data.get("properties", {}).get("nextLink")
            if not next_link:
                break

        current_start = (current_start + timedelta(days=32)).replace(day=1)

    # Zet alles in een DataFrame
    column_names = [col["name"] for col in json_data["properties"]["columns"]]
    df = pd.DataFrame(all_data, columns=column_names)

    return df