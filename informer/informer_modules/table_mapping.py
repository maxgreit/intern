import logging

balance_sheet = {
    'date': 'Datum',
    'period': 'Periode',
    'year': 'Jaar',
    'grootboekrekening': 'Grootboekrekening',
    'grootboekrekening_nummer': 'Grootboekrekening_nummer',
    'invoice_id': 'Factuur_nummer',
    'number': 'Lijn_nummer',
    'type': 'Type',
    'name': 'Bedrijfsnaam',
    'entry_description': 'Beschrijving',
    'line_description': 'Lijn_beschrijving',
    'debit': 'Debit',
    'credit': 'Credit',
}

balans_kolommen = [
    'Datum',
    'Periode',
    'Jaar',
    'Grootboekrekening',
    'Grootboekrekening_nummer',
    'Factuur_nummer',
    'Lijn_nummer',
    'Type',
    'Bedrijfsnaam',
    'Beschrijving',
    'Lijn_beschrijving',
    'Debit',
    'Credit',
]

sales = {
    'datum': 'Datum',
    'relatie_nummer': 'Relatie_nummer',
    'factuur_nummer': 'Factuur_nummer',
    'vervaldatum': 'Vervaldatum',
    'referentie': 'Referentie',
    'prijs_excl_btw': 'Prijs_excl_btw',
    'prijs_incl_btw': 'Prijs_incl_btw',
    'bedrag_voldaan': 'Bedrag_voldaan',
    'betaal_datum': 'Betaal_datum',
    'bedrijfsnaam': 'Bedrijfsnaam',
    'factuur_id': 'Factuur_id',
    'regel_id': 'Regel_id',
    'omschrijving': 'Omschrijving',
    'aantal': 'Aantal',
    'prijs_per_stuk': 'Prijs_per_stuk',
    'btw_percentage': 'Btw_percentage',
    'grootboek_id': 'Grootboek_id',
}

sales_kolommen = [
    'Datum',
    'Relatie_nummer',
    'Factuur_nummer',
    'Vervaldatum',
    'Referentie',
    'Prijs_excl_btw',
    'Prijs_incl_btw',
    'Bedrag_voldaan',
    'Betaal_datum',
    'Bedrijfsnaam',
    'Factuur_id',
    'Regel_id',
    'Omschrijving',
    'Aantal',
    'Prijs_per_stuk',
    'Btw_percentage',
    'Grootboek_id',
]

purchase = {
    'datum': 'Datum',
    'relatie_nummer': 'Relatie_nummer',
    'factuur_nummer': 'Factuur_nummer',
    'vervaldatum': 'Vervaldatum',
    'prijs_excl_btw': 'Prijs_excl_btw',
    'prijs_incl_btw': 'Prijs_incl_btw',
    'bedrag_voldaan': 'Bedrag_voldaan',
    'betaal_datum': 'Betaal_datum',
    'bedrijfsnaam': 'Bedrijfsnaam',
    'factuur_id': 'Factuur_id',
    'regel_id': 'Regel_id',
    'omschrijving': 'Omschrijving',
    'bedrag': 'Bedrag',
    'btw_percentage': 'Btw_percentage',
    'grootboek_id': 'Grootboek_id',
}

purchase_kolommen = [
    'Datum',
    'Relatie_nummer',
    'Factuur_nummer',
    'Vervaldatum',
    'Prijs_excl_btw',
    'Prijs_incl_btw',
    'Bedrag_voldaan',
    'Betaal_datum',
    'Bedrijfsnaam',
    'Factuur_id',
    'Regel_id',
    'Omschrijving',
    'Bedrag',
    'Btw_percentage',
    'Grootboek_id',
]

hours = {
    'datum': 'Datum',
    'bedrijfsnaam': 'Bedrijfsnaam',
    'factuur_nummer': 'Factuur_nummer',
    'referentie': 'Referentie',
    'omschrijving': 'Omschrijving',
    'aantal': 'Aantal',
}

hours_kolommen = [
    'Aantal',
    'Omschrijving',
    'Datum',
    'Bedrijfsnaam',
    'Factuur_nummer',
    'Referentie',
]

def transform_columns(df, column_mapping):
    # Controleer of de DataFrame leeg is
    
    if df.empty:
        # Retourneer een melding en None
        print("De DataFrame is leeg. Retourneer een lege DataFrame met de juiste kolommen.")
        return None

    # Hernoem de kolommen
    df = df.rename(columns=column_mapping)

    return df

def apply_mapping(df, tabelnaam):
    # Kolom mappin
    column_mapping = {
        'Balans': balance_sheet,
        'Verkoop': sales,
        'Inkoop': purchase,
        'Uren': hours
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
                
def select_columns(df, tabelnaam):
    # Kolom selectie
    column_selection = {
        'Balans': balans_kolommen,
        'Verkoop': sales_kolommen,
        'Inkoop': purchase_kolommen,
        'Uren': hours_kolommen
    }
    
    for selection_table, selection in column_selection.items():
        if tabelnaam == selection_table:
            return df[selection]
            
            