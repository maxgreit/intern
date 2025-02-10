from gmail_modules.mail_retrieval import mail_dataframe
from gmail_modules.database import empty_and_fill_table
from gmail_modules.table_mapping import apply_mapping
from gmail_modules.config import determine_script_id
from gmail_modules.log import setup_logging, end_log
from gmail_modules.type_mapping import apply_typing
from dateutil.relativedelta import relativedelta
from gmail_modules.env_tool import env_check
from datetime import datetime, timedelta
import pandas as pd
import logging
import time
import os

def main():
    
    # Lokaal of productieomgeving bepaling
    env_check()
    
    # Script configuratie
    klant = "Greit"
    script = "Aandelen"
    bron = 'DeGiro'
    start_time = time.time()

    # Verbindingsinstellingen
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    database = os.getenv('DATABASE')
    server = os.getenv('SERVER')
    driver = '{ODBC Driver 18 for SQL Server}'
    tabel = 'Aandelen'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;'
    
    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string)

    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)
    
    try:

        # Start datum en eind datum bepalen
        vandaag = datetime.today()
        start_datum = (vandaag - relativedelta(months=1)).replace(day=1)  # Begin vorige maand
        eind_datum = (vandaag + relativedelta(months=1)).replace(day=1) - timedelta(days=1)  # Eind deze maand
        sql_start_datum = start_datum.strftime('%Y-%m-%d')
        sql_eind_datum = eind_datum.strftime('%Y-%m-%d')
        gmail_start_datum = start_datum.strftime('%Y/%m/%d')
        gmail_eind_datum = eind_datum.strftime('%Y/%m/%d')

        # Mails ophalen
        df = mail_dataframe(gmail_start_datum, gmail_eind_datum)

        # Kolommen aanpassen
        altered_df = apply_mapping(df)
        
        # Datum tabel toevoegen
        altered_df['Transactiedatum'] = pd.to_datetime(altered_df['Transactiedatumtijd'])

        # Kolommen type conversie
        converted_df = apply_typing(altered_df)
        
        # Tabel legen en vullen
        empty_and_fill_table(converted_df, tabel, greit_connection_string, sql_start_datum, sql_eind_datum)

    except Exception as e:
        logging.error(f"Script mislukt: {e}")
        
    # Eidn logging
    end_log(start_time)

if __name__ == '__main__':
    main()