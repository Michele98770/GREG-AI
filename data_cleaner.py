import pandas as pd
import time
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# Configurazione nomi file
INPUT_FILE = './dataset/raw_dataset.csv'
OUTPUT_CSV = './dataset/clean_dataset.csv'


def main():
    # Verifica esistenza file input
    if not os.path.exists(INPUT_FILE):
        print(f"ERRORE: Il file '{INPUT_FILE}' non esiste nella cartella corrente.")
        return

    print("--- Inizio elaborazione GREG ---")

    # 1. Caricamento Dataset
    df = pd.read_csv(INPUT_FILE)
    print(f"Righe totali iniziali: {len(df)}")

    # 2. RIMOZIONE COLONNE SUPERFLUE
    columns_to_drop = [
        'opportunity_id', 'content_id', 'event_time', 'title', 'hits', 'is_priority',
        'amsl', 'amsl_unit', 'org_title', 'org_content_id', 'addresses', 'region',
        'recurrences', 'hours', 'primary_loc', 'created_date', 'last_modified_date',
        'start_date_date', 'end_date_date', 'status', 'BIN', 'BBL', 'NTA',
        'Zip Codes', 'Borough', 'Borough Boundaries', 'Community Board', 'Community Council',
        'Census Tract', 'Community', 'City Council', 'Police Precincts',
        # Nomi completi/alternativi
        'recurrence_type', 'addresses_count', 'Community Districts', 'City Council Districts',
        'Community Council ', 'category_id', 'display_url', 'locality'
    ]

    print("\nRimozione colonne superflue in corso...")
    df.drop(columns=columns_to_drop, axis=1, errors='ignore', inplace=True)
    print("Colonne rimosse con successo.")

    # 3. RIMOZIONE righe senza 'category_desc'
    df_clean = df.dropna(subset=['category_desc']).copy()
    print(f"Righe dopo rimozione categorie mancanti: {len(df_clean)}")

    # 4. GEOCODING + FILTRO RIGOROSO
    geolocator = Nominatim(user_agent="greg_project_final_v2")
    print("\nInizio geocoding... (I record senza coordinate recuperabili verranno eliminati)")

    indices_to_drop = []
    count_recovered = 0
    count_failed = 0

    for index, row in df_clean.iterrows():
        # Se mancano le coordinate
        if pd.isnull(row['Latitude']) or pd.isnull(row['Longitude']):
            success = False

            # Tentativo con CAP (Postcode)
            if pd.notnull(row['Postcode']):
                try:
                    cap = str(int(row['Postcode']))
                    query = f"{cap}, New York, USA"
                    location = geolocator.geocode(query, timeout=5)

                    if location:
                        df_clean.at[index, 'Latitude'] = location.latitude
                        df_clean.at[index, 'Longitude'] = location.longitude
                        success = True
                        count_recovered += 1
                        print(f"✓ Recuperato: {cap}")
                        time.sleep(1)  # Pausa anti-ban
                except Exception as e:
                    print(f"Errore API su riga {index}: {e}")

            if not success:
                indices_to_drop.append(index)
                count_failed += 1
                print(f"X Scartato (Coordinate non trovate): Riga {index}")

    # 5. RIMOZIONE FINALE DEI RECORD FALLITI
    if indices_to_drop:
        df_clean = df_clean.drop(indices_to_drop)

    print("\n--- Statistiche Finali ---")
    print(f"Coordinate recuperate: {count_recovered}")
    print(f"Record eliminati per mancanza coordinate: {count_failed}")
    print(f"Totale record validi nel dataset finale: {len(df_clean)}")

    # 6. Salvataggio CSV
    if not df_clean.empty:
        df_clean.to_csv(OUTPUT_CSV, index=False)
        print(f"File CSV salvato: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
