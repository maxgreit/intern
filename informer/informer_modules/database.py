from sqlalchemy import create_engine
import logging
import urllib
import pyodbc
import time

def connect_to_database(connection_string):
    # Retries en delays
    max_retries = 3
    retry_delay = 5
    
    # Pogingen doen om connectie met database te maken
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            return conn
        except Exception as e:
            print(f"Fout bij poging {attempt + 1} om verbinding te maken: {e}")
            if attempt < max_retries - 1:  # Wacht alleen als er nog pogingen over zijn
                time.sleep(retry_delay)
    
    # Als het na alle pogingen niet lukt, return None
    print("Kan geen verbinding maken met de database na meerdere pogingen.")
    return None

def clear_table(connection_string, table, jaar=None):
    try:
        # Maak verbinding met de database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        if jaar is None:
            # Gebruik TRUNCATE TABLE als de hele tabel moet worden geleegd
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
            except pyodbc.Error as e:
                print(f"TRUNCATE TABLE {table} failed: {e}")
        else:
            # Verwijder alleen records van een specifiek jaar
            try:
                cursor.execute(f"DELETE FROM {table} WHERE Jaar = ?", jaar)
            except pyodbc.Error as e:
                print(f"DELETE FROM {table} WHERE Jaar = {jaar} failed: {e}")

        # Commit de transactie
        connection.commit()
        print(f"Leeggooien succesvol uitgevoerd voor tabel {table}.")
    
    except pyodbc.Error as e:
        print(f"Fout bij het leeggooien van tabel {table}: {e}")
    
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()

def write_to_database(df, tabel, connection_string, batch_size=1000):
    db_params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={db_params}", fast_executemany=True)

    total_rows = len(df)
    rows_added = 0
    
    try:
        # Werk in batches
        for start in range(0, total_rows, batch_size):
            batch_df = df.iloc[start:start + batch_size]
            # Schrijf direct naar de database
            batch_df.to_sql(tabel, con=engine, index=False, if_exists="append", schema="dbo")
            rows_added += len(batch_df)
            print(f"{rows_added} rijen toegevoegd aan de tabel tot nu toe...")
        
        print(f"DataFrame succesvol toegevoegd/bijgewerkt in de tabel: {tabel}")
    except Exception as e:
        print(f"Fout bij het toevoegen naar de database: {e}")

def empty_and_fill_table(df, tabelnaam, klant_connection_string, jaar=None):
    if jaar is None:
        # Tabel legen
        try:
            clear_table(klant_connection_string, tabelnaam)
            print(f"Tabel {tabelnaam} leeg gemaakt")
            logging.info(f"Tabel leeg gemaakt")
        except Exception as e:
            logging.error(f"FOUTMELDING | Tabel leeg maken mislukt: {e}")

        # Tabel vullen
        try:
            print(f"Volledige lengte {tabelnaam}: ", len(df))
            write_to_database(df, tabelnaam, klant_connection_string)
            logging.info(f"Tabel gevuld met {len(df)} rijen")
        except Exception as e:
            logging.error(f"FOUTMELDING | Tabel vullen mislukt: {e}")
        
    else:
        # Tabel legen
        try:
            clear_table(klant_connection_string, tabelnaam, jaar)
            print(f"Tabel {tabelnaam} leeg gemaakt")
            logging.info(f"Tabel leeg gemaakt voor {jaar}")
        except Exception as e:
            logging.error(f"FOUTMELDING | Tabel leeg maken mislukt voor {jaar}: {e}")

        # Tabel vullen
        try:
            print(f"Volledige lengte {tabelnaam}: ", len(df))
            write_to_database(df, tabelnaam, klant_connection_string)
            logging.info(f"Tabel gevuld met {len(df)} rijen")
        except Exception as e:
            logging.error(f"FOUTMELDING | Tabel vullen mislukt voor {jaar}: {e}")