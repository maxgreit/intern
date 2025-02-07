from informer_modules.table_mapping import apply_mapping, select_columns
from informer_modules.get_request import get_purchase_dataframe
from informer_modules.database import empty_and_fill_table
from informer_modules.type_mapping import apply_conversion
from informer_modules.config import determine_script_id
from informer_modules.log import setup_logging, end_log
from informer_modules.env_tool import env_check
import logging
import time
import os

def main():
    
    env_check()
    
    # Script configuratie
    klant = "Greit"
    script = "Inkoop"
    bron = 'Informer'
    tabelnaam = 'Inkoop'
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
        df = get_purchase_dataframe(api_url, Apikey, Securitycode)
        
        # Mapping toepassen
        df = apply_mapping(df, tabelnaam)
        
        # Kolom keuze
        df = select_columns(df, tabelnaam)
        
        # Type conversie toepassen
        df = apply_conversion(df, tabelnaam)        
        
        # Data overdracht
        empty_and_fill_table(df, tabelnaam, greit_connection_string)

    except Exception as e:
        logging.error(f"Error: {e}")
    
    # Eindtijd logging
    end_log(start_time)
    
if __name__ == "__main__":    
    main()
        
    

