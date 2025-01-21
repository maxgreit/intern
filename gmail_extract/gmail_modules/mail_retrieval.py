from simplegmail.query import construct_query
from simplegmail import Gmail
from bs4 import BeautifulSoup
import pandas as pd

def mail_dataframe(start_datum, eind_datum):

    # Gmail initialiseren
    try:
        gmail = Gmail()
    except Exception as e:
        print(f"FOUTMELDING | Gmail initialiseren mislukt: {e}")
        return
    
    # Query parameters definiëren
    query_params = {
        "sender": "notificaties@degiro.nl",
        "subject": "Transactiebevestiging",
        "before": str(eind_datum),
        "after": str(start_datum)
    }

    # Berichten opvragen
    messages = gmail.get_messages(query=construct_query(query_params))

    # Lege dataframe initialiseren
    mail_dataframe = pd.DataFrame()

    # Berichten omzetten in dataframe
    for message in messages:

        # De e-mail HTML-inhoud (bijvoorbeeld van message.html)
        html_content = message.html

        # Parse de HTML met BeautifulSoup
        soup = BeautifulSoup(html_content, 'lxml')

        # Zoek naar alle tabellen met gegevens over de transacties
        transaction_tables = soup.find_all('table', bgcolor="#ffffff")

        # Initialiseer een lege lijst voor de transactiegegevens
        transaction_data = []

        # Loop door elke transactie tabel
        for table in transaction_tables:
            transaction_info = {}
            
            # Haal alle rijen uit de tabel
            rows = table.find_all('tr')
            
            # Extract de gegevens van elke rij
            for row in rows:
                # Haal de kolommen uit de rij
                columns = row.find_all('td')
                
                # Alleen rijen die twee kolommen bevatten (bijvoorbeeld label en waarde)
                if len(columns) == 2:
                    key = columns[0].get_text(strip=True)
                    value = columns[1].get_text(strip=True)
                    
                    # Voeg de data toe aan het transaction_info dictionary
                    transaction_info[key] = value
            
            # Voeg de transactie toe aan de lijst
            transaction_data.append(transaction_info)

        # Zet de transactiegegevens om naar een DataFrame
        df = pd.DataFrame(transaction_data)
        
        # Voeg de DataFrame toe aan de mail_dataframe
        mail_dataframe = pd.concat([mail_dataframe, df], ignore_index=True)

    return mail_dataframe