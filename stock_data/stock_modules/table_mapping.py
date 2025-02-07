import logging

plantgoed = {
    "OmsCultivar": "Cultivar",
    "OmsPartij": "Partij",
    "Nieuwe partij": "Nieuwe_partij",
    "Maat": "Maat",
    "UitMaat": "Uit_maat",
    "Minus7": "Minus_7",
    "Fustnummer": "Fustnummer",
    "Kg per fust": "Kg_per_fust",
    "Aantal per RR": "Aantal_per_rr",
    "VastgezetPlantdiktePartijMaat": "Vastgezet_plantdikte_partij_maat",
    "Aantal per Kg": "Aantal_per_kg",
    "TotaalBollen": "Totaal_bollen",
    "RestantFust": "Restant_fust",
    "Planten": "Planten",
    "Perceel": "Perceel",
    "Volgorde planten plantschema": "Volgorde_planten_plantschema",
    "Volgorde planten fust": "Volgorde_planten_fust",
    "Plantjaar": "Plantjaar",
    "Aantal per RR planten": "Aantal_per_rr_planten",
    "UitvalGeschatPartij": "Uitval_geschat_partij",
    "Augusta": "Augusta",
    "Vastgezet plantdikte": "Vastgezet_plantdikte",
    "Totaal RR": "Totaal_rr",
    "Opmerkingen": "Opmerkingen",
    "CEL": "Cel",
    "VAK": "Vak",
    "Geplant": "Geplant",
    "PlantdatumWerkelijk": "Plant_datum_werkelijk",
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

def apply_mapping(df, tabelnaam, greit_connection_string, klant, bron, script, script_id):
    # Kolom mappin
    column_mapping = {
        'Plantgoed': plantgoed,
    }

    # Tabel mapping
    for mapping_table, mapping in column_mapping.items():
        if tabelnaam == mapping_table:

            # Transformeer de kolommen
            try:
                transformed_df = transform_columns(df, mapping)
                logging.info(f"Kolommen getransformeerd")
                
                return transformed_df
            except Exception as e:
                logging.error(f"FOUTMELDING | Kolommen transformeren mislukt: {e}")
            
            