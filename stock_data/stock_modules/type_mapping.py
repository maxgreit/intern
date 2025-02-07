from pandas.errors import OutOfBoundsDatetime
from decimal import Decimal, ROUND_HALF_UP
import pandas as pd
import numpy as np
import logging


plantgoed_typing = {
    "Cultivar": "nvarchar",
    "Partij": "nvarchar",
    "Nieuwe_partij": "nvarchar",
    "Maat": "nvarchar",
    "Uit_maat": "nvarchar",
    "Minus_7": "nvarchar",
    "Fustnummer": "nvarchar",
    "Kg_per_fust": "float",
    "Aantal_per_rr": "float",
    "Vastgezet_plantdikte_partij_maat": "bit",
    "Aantal_per_kg": "float",
    "Totaal_bollen": "float",
    "Restant_fust": "float",
    "Planten": "bit",
    "Perceel": "nvarchar",
    "Volgorde_planten_plantschema": "float",
    "Volgorde_planten_fust": "float",
    "Plantjaar": "int",
    "Aantal_per_rr_planten": "float",
    "Uitval_geschat_partij": "float",
    "Augusta": "bit",
    "Vastgezet_plantdikte": "bit",
    "Totaal_rr": "float",
    "Opmerkingen": "nvarchar",
    "Cel": "nvarchar",
    "Vak": "nvarchar",
    "Geplant": "bit",
    "Plant_datum_werkelijk": "datetime",
    "Jaar": "int",
}

def convert_column_types(df, column_types):
    pd.set_option('future.no_silent_downcasting', True)

    for column, dtype in column_types.items():
        if column in df.columns:
            try:
                # Vervang None-waarden met een standaardwaarde voordat je de conversie uitvoert
                if dtype == 'int':
                    # Zet niet-numerieke waarden om naar NaN en vul None in met 0
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)

                    # Controleer of de waarden binnen het bereik van SQL Server's INT vallen
                    min_int = -2147483648
                    max_int = 2147483647
                    df[column] = df[column].apply(
                        lambda x: x if min_int <= x <= max_int else None
                    )
                elif dtype == 'nvarchar':
                    df[column] = df[column].fillna('').astype(str)  # Vervang None door lege string
                elif dtype == 'decimal':
                    # Hier wordt de precisie ingesteld
                    df[column] = df[column].apply(
                        lambda x: Decimal(str(x)).quantize(Decimal('1.00'), rounding=ROUND_HALF_UP) 
                        if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else None
                    )
                elif dtype == 'float':
                    df[column] = df[column].apply(
                        lambda x: float(x) if pd.notna(x) and str(x).replace('.', '', 1).isdigit() else None
                    )
                    
                    # Vervang NaN door NULL (None in Python)
                    df[column] = df[column].apply(
                        lambda x: None if isinstance(x, float) and np.isnan(x) else x
                    )

                    # Zorg ervoor dat de float binnen een specifiek bereik valt, bijvoorbeeld voor SQL Server
                    df[column] = df[column].apply(
                        lambda x: x if x is None or (-1.79e+308 <= x <= 1.79e+308) else None
                    )
                elif dtype == 'bit':
                    df[column] = df[column].apply(
                        lambda x: True if x in [-1, 'Ja', 'ja', 'YES', 'yes'] else False if x in [0, 'Nee', 'nee', 'NO', 'no'] else x
                    ).fillna(False)
                elif dtype == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                    df[column] = df[column].fillna(pd.NaT)
                elif dtype == 'time':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.time
                    df[column] = df[column].fillna(pd.NaT)
                elif dtype == 'datetime':
                    # Zet de kolom om naar datetime
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    
                    # Controleer of er datums buiten het bereik van SQL Server 'datetime' vallen (1753-9999)
                    if df[column].isna().sum() > 0:  # Foute datums worden vervangen door NaT
                        df[column] = df[column].fillna(pd.NaT)
                    
                    # Zet de datums buiten het bereik van SQL Server 'datetime' (1753-9999) op NaT
                    # SQL Server's datetime heeft een bereik van 1753-9999
                    min_date = pd.Timestamp('1753-01-01')
                    max_date = pd.Timestamp('9999-12-31')
                    df[column] = df[column].apply(lambda x: x if pd.isna(x) or (min_date <= x <= max_date) else pd.NaT)

                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
            except OutOfBoundsDatetime:
                # Als er een 'OutOfBoundsDatetime' fout is (zoals een datum buiten het bereik van SQL Server),
                # zet dan die waarde op NaT.
                df[column] = pd.NaT
        else:
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df

def apply_conversion(df, tabelnaam, greit_connection_string, klant, bron, script, script_id):
    column_typing = {
        'Plantgoed': plantgoed_typing,
    }

    # Update typing van kolommen
    for typing_table, typing in column_typing.items():
        if tabelnaam == typing_table:
            
            # Type conversie
            try:
                converted_df = convert_column_types(df, typing)
                logging.info(f"Kolommen type conversie")
                
                return converted_df
            except Exception as e:
                logging.error(f"Kolommen type conversie mislukt: {e}")
                
            