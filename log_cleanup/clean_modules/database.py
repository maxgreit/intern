from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
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

def empty_table(greit_connection_string, klant, table):

    try:
        # Datums bepalen
        vandaag = datetime.today()
        start_datum = (vandaag - relativedelta(months=2)).replace(day=1)
        start_datumtijd = start_datum.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        print(f"Vóór datum: {start_datum}")
        
        # Maak verbinding met de database
        connection = pyodbc.connect(greit_connection_string)
        cursor = connection.cursor()
        
        # Leeg loop
        try:
            # Probeer de tabel leeg te maken met DELETE, gefilterd op de datums en klant
            cursor.execute(f"""
                DELETE FROM {table}
                WHERE Datumtijd < ? 
            """, (start_datumtijd))
            rows_deleted = cursor.rowcount  # Houd het aantal verwijderde rijen bij
            print(f"Aantal verwijderde rijen voor klant '{klant}': {rows_deleted}")
        except pyodbc.Error as e:
            print(f"DELETE FROM {table} voor klant '{klant}' en periode (alles vóór {start_datum} is mislukt: {e}")
        
        # Commit de transactie
        connection.commit()
        print(f"Leeggooien succesvol uitgevoerd voor tabel {table}.")
    except pyodbc.Error as e:
        print(f"Fout bij het leeggooien van tabel {table}: {e}")
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()