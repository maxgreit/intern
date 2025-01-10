from clean_modules.config import determine_script_id
from clean_modules.database import empty_table
from clean_modules.env_tool import env_check
from clean_modules.log import log, end_log
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    klant = "Greit"
    script = "Log Cleanup"
    bron = 'Azure'
    start_time = time.time()

    # Verbindingsinstellingen
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    database = os.getenv('DATABASE')
    server = os.getenv('SERVER')
    driver = '{ODBC Driver 18 for SQL Server}'
    tabel = 'Logging'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'
    
    try:
        # Script ID bepalen
        script_id = determine_script_id(greit_connection_string, klant, bron, script)

        # Tabel legen en vullen
        empty_table(greit_connection_string, klant, tabel)

    except Exception as e:
        logging.error(f"FOUTMELDING | Script mislukt: {e}")
        log(greit_connection_string, klant, bron, f"FOUTMELDING | Script mislukt: {e}", script, script_id, tabel)
        
    # Eidn logging
    end_log(start_time, greit_connection_string, klant, bron, script, script_id)

if __name__ == '__main__':
    main()