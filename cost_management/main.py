import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from cost_management_modules.access_token import get_access_token
from cost_management_modules.database import connect_to_database, write_to_database, clear_table
from cost_management_modules.log import log
from cost_management_modules.config import fetch_script_id, fetch_all_connection_strings
from cost_management_modules.type_mapping import convert_column_types, kosten_typing
from cost_management_modules.table_mapping import transform_columns, kosten, klant_vervangen
import pandas as pd
import time
import logging

def main():

    if os.path.exists("/Users/maxrood/werk/greit/bedrijfs_scripts/cost_management/.env"):
        load_dotenv()
        print("Lokaal draaien: .env bestand gevonden en geladen.")
        logging.info("Lokaal draaien: .env bestand gevonden en geladen.")
    else:
        logging.info("Draaien in productieomgeving (Azure): .env bestand niet gevonden.")
        print("Draaien in productieomgeving (Azure): .env bestand niet gevonden.")
    
    # Definiëren van script
    script = "Cost Management"
    klant = "Greit"

    # Leg de starttijd vast
    start_time = time.time()

    # Aantal retries instellen
    max_retries = 3
    retry_delay = 5

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'

    # Verbindingsstring
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'
    
    # ScriptID ophalen
    database_conn = connect_to_database(greit_connection_string)
    if database_conn:
        cursor = database_conn.cursor()
        latest_script_id = fetch_script_id(cursor)
        database_conn.close()

        if latest_script_id:
            script_id = latest_script_id + 1
        else:
            script_id = 1

    # Start logging
    bron = 'python'
    log(greit_connection_string, klant, bron, f"Script gestart", script, script_id)

    # Verbinding maken met database
    database_conn = connect_to_database(greit_connection_string)
    if database_conn:
        cursor = database_conn.cursor()
        connection_dict = None
        for attempt in range(max_retries):
            try:
                connection_dict = fetch_all_connection_strings(cursor)
                if connection_dict:
                    break
            except Exception as e:
                time.sleep(retry_delay)
        database_conn.close()
        if connection_dict:

            # Start logging
            log(greit_connection_string, klant, bron, f"Ophalen connectiestrings gestart", script, script_id)
        else:
            # Foutmelding logging
            logging.error(f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen")
            log(greit_connection_string, klant, bron, f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen", script, script_id)
    else:
        # Foutmelding logging
        logging.error(f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen", script, script_id)

    # Genereer nieuw access token
    logging.info("Genereer nieuw access token")
    log(greit_connection_string, klant, bron, "Genereer nieuw access token", script, script_id)
    try:
        bearer_token = get_access_token()
    except Exception as e:
        logging.error(f"Fout bij het genereren van een nieuw access token: {e}")
        log(greit_connection_string, klant, bron, f"Fout bij het genereren van een nieuw access token: {e}", script, script_id)
        exit(1)

    # Vul deze variabelen in met je eigen gegevens
    subscription_id = os.getenv('SUBSCRIPTION_ID')

    # API URL voor het opvragen van kosten
    cost_url = f'https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.CostManagement/query?api-version=2023-03-01'

    # Bereken de startdatum als de eerste dag van de vorige maand
    start_date = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d")
    # Bereken de einddatum als de huidige datum
    end_date = datetime.now().strftime("%Y-%m-%d")

    # JSON-body voor het opvragen van de kosten voor de huidige maand per dag
    cost_body = {
    "type": "ActualCost",
    "timeframe": "Custom",
    "timePeriod": {
        "from": start_date,
        "to": end_date
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
            {
                "type": "Dimension",
                "name": "ServiceName"
            },
            {
                "type": "Dimension",
                "name": "ResourceGroup"  # Groeperen op ResourceGroup
            }
        ]
    }
}

    # Headers voor het verzoek
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }

    # Maak een POST-verzoek om de kosten op te vragen
    try:
        logging.info("Maak een POST-verzoek om de kosten op te vragen")
        log(greit_connection_string, klant, bron, "Maak een POST-verzoek om de kosten op te vragen", script, script_id)
        response = requests.post(cost_url, json=cost_body, headers=headers)
    except Exception as e:
        logging.error(f"Fout bij het opvragen van de kosten: {e}")
        log(greit_connection_string, klant, bron, f"Fout bij het opvragen van de kosten: {e}", script, script_id)
        exit(1)

    # Controleer of de gegevens succesvol zijn opgevraagd
    if response.status_code == 200:
        logging.info('Kosteninformatie ontvangen')
        log(greit_connection_string, klant, bron, "Kosteninformatie ontvangen", script, script_id)
        json_data = response.json()
    else:
        logging.error(f"Fout bij het ophalen van kosteninformatie:' {response.status_code} | {response.text}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Fout bij het ophalen van kosteninformatie: {response.status_code}", script, script_id)
        exit(1)

    # Maak een DataFrame van de gegevens
    column_names = [col["name"] for col in json_data["properties"]["columns"]]
    rows = json_data["properties"]["rows"]
    df = pd.DataFrame(rows, columns=column_names)

    # Kolom mapping
    column_mapping = {
        'Kosten': kosten,
    }

    # Tabel mapping
    for tabel, mapping in column_mapping.items():
            # Transformeer de kolommen
            try:
                df = transform_columns(df, mapping)
                logging.info(f"Kolommen getransformeerd")
                log(greit_connection_string, klant, bron, f"Mapping van kolommen correct uitgevoerd", script, script_id, tabel)
            except Exception as e:
                logging.error(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen transformeren mislukt: {e}", script, script_id, tabel)
                return

    # Kolom typing
    column_typing = {
        'Kosten': kosten_typing,
    }

    # Update typing van kolommen
    for tabel, typing in column_typing.items():
            # Type conversie
            try:
                df = convert_column_types(df, typing)
                logging.info(f"Kolommen type conversie")
                log(greit_connection_string, klant, bron, f"Kolommen type conversie correct uitgevoerd", script, script_id, tabel)
            except Exception as e:
                logging.error(f"FOUTMELDING | Kolommen type conversie mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen type conversie mislukt: {e}", script, script_id, tabel)
                return

    # Klant aanpassen
    df['Klant'] = df['Klant'].apply(klant_vervangen)

    # Tabel leeg maken
    try:
        clear_table(greit_connection_string, tabel, klant)
        logging.info(f"Tabel {tabel} leeg gemaakt vanaf begin van deze maand")
        log(greit_connection_string, klant, bron, f"Tabel {tabel} leeg gemaakt vanaf begin van deze maand", script, script_id, tabel)
    except Exception as e:
        logging.error(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel leeg maken mislukt: {e}", script, script_id, tabel)
        return
    
    # Tabel vullen
    try:
        logging.info(f"Volledige lengte {tabel}: {len(df)}")
        log(greit_connection_string, klant, bron, f"Volledige lengte {tabel}: {len(df)}", script, script_id,  tabel)
        added_rows_count = write_to_database(df, tabel, greit_connection_string)
        logging.info(f"Tabel {tabel} gevuld")
        log(greit_connection_string, klant, bron, f"Tabel gevuld met {added_rows_count} rijen", script, script_id,  tabel)
    except Exception as e:
        logging.error(f"FOUTMELDING | Tabel vullen mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Tabel vullen mislukt: {e}", script, script_id,  tabel)
        return
    
    # Eindtijd logging
    bron = 'python'
    eindtijd = time.time()
    tijdsduur = timedelta(seconds=(eindtijd - start_time))
    tijdsduur_str = str(tijdsduur).split('.')[0]
    log(greit_connection_string, klant, bron, f"Script gestopt in {tijdsduur_str}", script, script_id)
    logging.info(f"Script gestopt in {tijdsduur_str}")

if __name__ == '__main__':
    main()