from gmail_modules.log import log
import logging

aandelen =   {
    "OrderID": "Order_ID",
    "Lokale waarde": "Lokale_waarde",
    "Transactiekosten en/of kosten van derden": "Transactiekosten",
    "Transactiedatum": "Transactiedatumtijd",
    "Totale Kosten": "Totale_kosten",
    "AutoFX kosten": "AutoFX_kosten",
}

def transform_columns(df, column_mapping):
    # Controleer of de DataFrame leeg is
    
    if df.empty:
        # Retourneer een melding en None
        print("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Hernoem de kolommen
    df = df.rename(columns=column_mapping)

    return df

def klant_vervangen(klant):
    if 'borst_bloembollen' in klant:
        return 'Borst'
    else:
        return 'Greit'
    
def apply_mapping(df, greit_connection_string, klant, bron, script, script_id):
    # Kolom mapping
    column_mapping = {
        'Aandelen': aandelen,
    }

    # Tabel mapping
    for tabel, mapping in column_mapping.items():
            # Transformeer de kolommen
            try:
                altered_df = transform_columns(df, mapping)
                logging.info(f"Kolommen getransformeerd")
                log(greit_connection_string, klant, bron, f"Mapping van kolommen correct uitgevoerd", script, script_id, tabel)
            except Exception as e:
                logging.error(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
                log(greit_connection_string, klant, bron, f"FOUTMELDING | Kolommen transformeren mislukt: {e}", script, script_id, tabel)
                return
    
    return altered_df