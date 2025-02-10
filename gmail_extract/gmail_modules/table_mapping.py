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
        logging.error("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Hernoem de kolommen
    df = df.rename(columns=column_mapping)

    return df

def klant_vervangen(klant):
    if 'borst_bloembollen' in klant:
        return 'Borst'
    else:
        return 'Greit'
    
def apply_mapping(df):
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
            except Exception as e:
                logging.error(f"Kolommen transformeren mislukt: {e}")
                return
    
    return altered_df