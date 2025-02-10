from cm_modules.table_mapping import apply_mapping, klant_vervangen
from cm_modules.access_token import get_access_token
from cm_modules.database import empty_and_fill_table
from cm_modules.config import determine_script_id
from cm_modules.log import setup_logging, end_log
from cm_modules.type_mapping import apply_typing
from cm_modules.post_request import post_request
from dateutil.relativedelta import relativedelta
from cm_modules.env_tool import env_check
from datetime import datetime, timedelta
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    klant = "Greit"
    script = "Cost Management"
    bron = 'Azure'
    start_time = time.time()

    # Verbindingsinstellingen
    subscription_id = os.getenv('SUBSCRIPTION_ID')
    client_secret = os.getenv('CM_CLIENT_SECRET')
    client_id = os.getenv('CM_CLIENT_ID')
    tenant_id = os.getenv('TENANT_ID')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    database = os.getenv('DATABASE')
    server = os.getenv('SERVER')
    driver = '{ODBC Driver 18 for SQL Server}'
    tabel = 'Kosten'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'
    
    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string)

    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)
    
    try:
        # Access token ophalen
        bearer_token = get_access_token(tenant_id, client_id, client_secret)

        # Start datum en eind datum bepalen
        vandaag = datetime.today()
        start_datum = (vandaag - relativedelta(months=1)).replace(day=1)  # Begin vorige maand
        eind_datum = (vandaag + relativedelta(months=1)).replace(day=1) - timedelta(days=1)  # Eind deze maand
        start_datum = start_datum.strftime('%Y-%m-%d')
        eind_datum = eind_datum.strftime('%Y-%m-%d')
        
        # Post request
        df = post_request(subscription_id, bearer_token, start_datum, eind_datum)

        # Kolommen aanpassen
        altered_df = apply_mapping(df)

        # Kolommen type conversie
        converted_df = apply_typing(altered_df)

        # Klant aanpassen
        converted_df['Klant'] = converted_df['Klant'].apply(klant_vervangen)

        # Tabel legen en vullen
        empty_and_fill_table(converted_df, tabel, greit_connection_string, klant, start_datum, eind_datum)

    except Exception as e:
        logging.error(f"Script mislukt: {e}")
        
    # Eidn logging
    end_log(start_time)

if __name__ == '__main__':
    main()