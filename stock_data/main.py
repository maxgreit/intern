from stock_modules.log import setup_logging, start_log, end_log
from stock_modules.config import determine_script_id
from stock_modules.database import load_tickers
from stock_modules.yahoo import get_stock_data
from stock_modules.env_tool import env_check
import pandas as pd
import logging
import time
import os

def main():

    # Lokaal of productieomgeving bepaling
    env_check()

    # Script configuratie
    klant = "Greit"
    script = "Aandelen Data"
    bron = 'Yahoo Finance'
    tabelnaam = 'Aandelen_historisch'
    start_time = time.time()
    
    # Omgevingsvariabelen
    api_key = os.getenv("OPENFIGI_API_KEY")
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
    
    # Start logging
    start_log()
    
    # ISINs ophalen
    tickers = load_tickers(greit_connection_string)

    # Hou unieke waarden
    tickers = list(set(tickers))
    
    # Maak totaal DataFrame aan
    df = pd.DataFrame()
    
    # Stock data ophalen per ticker
    for ticker in tickers:
        logging.info(f"Stock data ophalen voor ticker: {ticker}")
        # Stock data ophalen
        stock_df = get_stock_data(ticker)
        stock_df.reset_index(inplace=True)
        df = pd.concat([df, stock_df], ignore_index=True)
    
    print(df)
    
if __name__ == "__main__":
    main()