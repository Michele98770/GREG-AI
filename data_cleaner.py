import pandas as pd
import time
import os
from geopy.geocoders import Nominatim

# Configurazione
INPUT_FILE = './dataset/raw_dataset.csv'
OUTPUT_CSV = './dataset/clean_dataset.csv'


def get_subcategory(row):
    cat = row['category_desc']
    text = str(row.get('summary', '')).lower() if pd.notnull(row.get('summary')) else ""

    categories_to_split = ['Strengthening Communities', 'Helping Neighbors in Need', 'Education']

    if cat in categories_to_split:
        # 1. Education
        if any(x in text for x in ['esl', 'english', 'literacy', 'language']): return "ESL & Adult Literacy"
        if any(x in text for x in ['tutor', 'math', 'science', 'homework', 'academic']): return "Academic Tutoring"
        if any(x in text for x in ['mentor', 'youth', 'teen', 'role model']): return "Youth Mentoring"

        # 2. Operativo
        if any(
            x in text for x in ['garden', 'park', 'plant', 'clean', 'maintenance']): return "Manual Labor & Environment"
        if any(x in text for x in ['food', 'soup', 'kitchen', 'pantry', 'meal', 'cook']): return "Food Security"
        if any(x in text for x in ['event', 'gala', 'run', 'walk', 'registration']): return "Event Support"

        # 3. Professional
        if any(x in text for x in ['legal', 'tax', 'finance', 'marketing', 'website']): return "Professional Skills"
        if any(x in text for x in ['fundraising', 'grant', 'donor']): return "Fundraising"
        if any(x in text for x in ['admin', 'data', 'office', 'clerical']): return "Admin & Office Support"

        # 4. Care
        if any(x in text for x in ['senior', 'elderly', 'patient', 'hospital']): return "Senior & Patient Care"
        if any(x in text for x in ['disability', 'autism', 'special needs']): return "Disability Support"

        if cat == 'Strengthening Communities':
            return "General Support"
        return cat
    return cat


def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: '{INPUT_FILE}' not found.")
        return

    print("--- Starting GREG Processing ---")

    # PHASE 1: LOAD
    df = pd.read_csv(INPUT_FILE)
    print(f"Initial rows: {len(df)}")

    # PHASE 2: DROP USELESS COLUMNS
    # Note: Keeping 'summary' and 'title' strictly for NLP, will drop later.
    columns_to_drop = [
        'opportunity_id', 'content_id', 'event_time', 'hits', 'is_priority',
        'amsl', 'amsl_unit', 'org_title', 'org_content_id', 'addresses', 'region',
        'recurrences', 'hours', 'primary_loc', 'created_date', 'last_modified_date',
        'start_date_date', 'end_date_date', 'status', 'BIN', 'BBL', 'NTA',
        'Zip Codes', 'Borough', 'Borough Boundaries', 'Community Board', 'Community Council',
        'Census Tract', 'Community', 'City Council', 'Police Precincts',
        'recurrence_type', 'addresses_count', 'Community Districts', 'City Council Districts',
        'Community Council ', 'category_id', 'display_url', 'locality'
    ]
    df.drop(columns=columns_to_drop, axis=1, errors='ignore', inplace=True)

    # PHASE 3: DROP ROWS WITHOUT CATEGORY
    df_clean = df.dropna(subset=['category_desc']).copy()
    print(f"Rows after category filter: {len(df_clean)}")

    # PHASE 4: GEOCODING AND FILTERING
    # PHASE 4: GEOCODING AND FILTERING (ROBUST VERSION)
    # Usiamo un user_agent univoco e casuale per evitare blocchi
    geolocator = Nominatim(user_agent=f"greg_project_fix_{int(time.time())}")
    print("Starting geocoding (Verbose Mode)...")

    indices_to_drop = []
    count_recovered = 0

    # Contatore per feedback visivo
    total_missing = df_clean['Latitude'].isnull().sum()
    print(f"Rows to geocode: {total_missing}")

    for index, row in df_clean.iterrows():
        # Solo se mancano le coordinate
        if pd.isnull(row['Latitude']) or pd.isnull(row['Longitude']):
            success = False

            if pd.notnull(row['Postcode']):
                try:
                    cap = str(int(row['Postcode']))
                    query = f"{cap}, New York, USA"

                    # Aumentiamo il timeout e stampiamo cosa sta facendo
                    print(f"-> Geocoding row {index} (CAP: {cap})...", end=" ")

                    location = geolocator.geocode(query, timeout=10)

                    if location:
                        df_clean.at[index, 'Latitude'] = location.latitude
                        df_clean.at[index, 'Longitude'] = location.longitude
                        success = True
                        count_recovered += 1
                        print("OK ✓")
                    else:
                        print("Not Found X")

                    # Pausa un po' più lunga per sicurezza
                    time.sleep(1.5)

                except Exception as e:
                    print(f"ERROR ({e})")
            else:
                # Se non c'è nemmeno il CAP, è persa
                pass

            if not success:
                indices_to_drop.append(index)

    # Rimozione righe fallite
    if indices_to_drop:
        print(f"Dropping {len(indices_to_drop)} rows without coordinates.")
        df_clean = df_clean.drop(indices_to_drop)

    print(f"Recovered coords: {count_recovered}")

    # PHASE 5: NLP SUB-CATEGORIZATION
    if not df_clean.empty:
        print("Applying NLP Sub-categorization...")
        df_clean['subcategory'] = df_clean.apply(get_subcategory, axis=1)

        # PHASE 6: FINAL CLEANUP (Removing Text Columns)
        # Now that subcategory is extracted, we drop the source text columns
        cols_to_remove_final = ['title', 'summary', 'Postcode']
        df_clean.drop(columns=cols_to_remove_final, axis=1, errors='ignore', inplace=True)

        # Reorder columns for cleanliness
        cols = list(df_clean.columns)
        # Ensure logical order: Category -> Subcategory -> Geo
        if 'subcategory' in cols and 'category_desc' in cols:
            cols.insert(cols.index('category_desc') + 1, cols.pop(cols.index('subcategory')))
            df_clean = df_clean[cols]

        df_clean.to_csv(OUTPUT_CSV, index=False)
        print(f"Saved: {OUTPUT_CSV}")
        print(f"Final Columns: {list(df_clean.columns)}")


if __name__ == "__main__":
    main()
