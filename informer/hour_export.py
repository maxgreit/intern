from informer_modules.table_mapping import apply_mapping, select_columns
from informer_modules.get_request import get_hour_dataframe
from informer_modules.config import determine_script_id
from informer_modules.log import setup_logging, end_log
from informer_modules.env_tool import env_check
import pandas as pd
import logging
import time
import os

def main():
    
    env_check()
    
    # Script configuratie
    klant = "Greit"
    script = "Uren"
    bron = 'Informer'
    tabelnaam = 'Uren'
    start_time = time.time()

    # Omgevingsvariabelen
    api_url = os.environ.get('INFORMER_URL')
    Apikey = os.environ.get('INFORMER_API_KEY')
    Securitycode = os.environ.get('INFORMER_SECURITY_CODE')
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string, klant, bron, script)

    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)
    
    try:
        
        # Haal balans data op
        df = get_hour_dataframe(api_url, Apikey, Securitycode)
        
        # Mapping toepassen
        df = apply_mapping(df, tabelnaam)
        
        # Kolom keuze
        df = select_columns(df, tabelnaam)
        
        # Tabel exporteren naar Excel
        df = df[df['Bedrijfsnaam'] == 'Finn It']
        df.to_excel('finn_it_uren.xlsx', index=False)

    except Exception as e:
        logging.error(f"Error: {e}")
    
    # Eindtijd logging
    end_log(start_time)
    
if __name__ == "__main__":    
    main()
        
    

