import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import os

# CONFIGURAZIONE PESI (Default)
WEIGHT_DISTANCE = 0.70  # La vicinanza conta al 70%
WEIGHT_SUBCAT = 0.20  # Il match specifico conta al 20%
WEIGHT_POPULARITY = 0.10  # La popolarità conta al 10%

# Configura Pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Caricamento Dataset (Global)
df = pd.DataFrame()
centroids = None


def load_data():
    global df, centroids
    try:
        # Cerca il file in diverse posizioni comuni
        file_path = 'clean_dataset.csv'
        if not os.path.exists(file_path):
            file_path = './dataset/clean_dataset.csv'

        if os.path.exists(file_path):
            df = pd.read_csv(file_path)

            # Pre-calcolo centroidi
            if 'geo_cluster' not in df.columns:
                from sklearn.cluster import KMeans
                coords = df[['Latitude', 'Longitude']].dropna()
                if len(coords) > 0:
                    kmeans = KMeans(n_clusters=5, random_state=42, n_init=10)
                    clusters = kmeans.fit_predict(coords)
                    df.loc[coords.index, 'geo_cluster'] = clusters
                    centroids = df.groupby('geo_cluster')[['Latitude', 'Longitude']].mean().reset_index()
            else:
                centroids = df.groupby('geo_cluster')[['Latitude', 'Longitude']].mean().reset_index()
        else:
            print("ATTENZIONE: clean_dataset.csv non trovato.")

    except Exception as e:
        print(f"Errore caricamento dati: {e}")


# Carica i dati all'avvio del modulo
load_data()


def haversine(lon1, lat1, lon2, lat2):
    """Calcola distanza in Km"""
    try:
        lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        return c * 6371
    except:
        return 9999


def get_closest_cluster(user_lat, user_lon):
    """Trova l'ID del cluster più vicino all'utente"""
    if centroids is None: return -1
    try:
        distances = centroids.apply(
            lambda row: haversine(user_lon, user_lat, row['Longitude'], row['Latitude']), axis=1
        )
        return centroids.loc[distances.idxmin(), 'geo_cluster']
    except:
        return -1


def normalize_series(series):
    """Normalizza una colonna tra 0 e 1"""
    if series.empty: return series
    if series.max() == series.min(): return 0
    return (series - series.min()) / (series.max() - series.min())


def greg_recommend(user_lat, user_lon, target_category=None, target_subcategory=None, top_k=5):
    """
    Motore di raccomandazione GREG v2.1
    Supporta category=None per ricerca pure location-based.
    """
    global df
    if df.empty: load_data()
    if df.empty: return []

    # 1. FILTRO GEOGRAFICO (Broad Phase)
    user_cluster = get_closest_cluster(user_lat, user_lon)

    # Prendi candidati nel cluster (o tutti se cluster fallisce)
    candidates = df[df['geo_cluster'] == user_cluster].copy()
    if len(candidates) == 0:
        candidates = df.copy()

    # 2. FILTRO CATEGORIA (Hard Filter - OPZIONALE)
    is_category_valid = target_category and str(target_category).strip() != "" and str(
        target_category).lower() != "null"

    if is_category_valid:
        if 'category_desc' in candidates.columns:
            candidates = candidates[candidates['category_desc'] == target_category].copy()

    # Se dopo il filtro (o senza filtro) non c'è nessuno, prova fallback globale
    if len(candidates) == 0:
        if is_category_valid:
            # Prova a cercare ovunque per quella categoria
            candidates = df[df['category_desc'] == target_category].copy()
        else:
            # Se non c'era categoria e cluster vuoto, prendi tutto il dataset
            candidates = df.copy()

        if len(candidates) == 0:
            return []  # Nessun risultato

    # --- CALCOLO PUNTEGGI (Ranking Phase) ---

    # A. Punteggio Distanza
    candidates['distance_km'] = candidates.apply(
        lambda row: haversine(user_lon, user_lat, row['Longitude'], row['Latitude']), axis=1
    )
    dist_score = 1 - normalize_series(candidates['distance_km'])

    # B. Punteggio Subcategory (solo se ha senso)
    use_subcat = target_subcategory and str(target_subcategory).strip() != "" and str(
        target_subcategory).lower() != "null"

    if use_subcat:
        subcat_score = candidates['subcategory'].apply(
            lambda x: 1.0 if pd.notna(x) and str(x).lower() == str(target_subcategory).lower() else 0.0
        )
        candidates['GREG_SCORE'] = (
                (dist_score * WEIGHT_DISTANCE) +
                (subcat_score * WEIGHT_SUBCAT) +
                (normalize_series(candidates['vol_requests']) * WEIGHT_POPULARITY)
        )
    else:
        # Logica dinamica per i pesi
        if not is_category_valid:
            # CASO: Solo Posizione (No Categoria)
            # La vicinanza diventa predominante
            ADJUSTED_WEIGHT_DISTANCE = 0.90
            ADJUSTED_WEIGHT_POPULARITY = 0.10
        else:
            # CASO: Categoria presente ma no Sottocategoria
            ADJUSTED_WEIGHT_DISTANCE = WEIGHT_DISTANCE + (WEIGHT_SUBCAT * 0.8)
            ADJUSTED_WEIGHT_POPULARITY = WEIGHT_POPULARITY + (WEIGHT_SUBCAT * 0.2)

        candidates['GREG_SCORE'] = (
                (dist_score * ADJUSTED_WEIGHT_DISTANCE) +
                (normalize_series(candidates['vol_requests']) * ADJUSTED_WEIGHT_POPULARITY)
        )

    # Ordina e restituisci
    results = candidates.sort_values('GREG_SCORE', ascending=False).head(top_k)

    # Converti in lista di dizionari per JSON
    output = []
    for _, row in results.iterrows():
        output.append({
            'email': row.get('email', ''),
            'category': row.get('category_desc', ''),
            'subcategory': row.get('subcategory', ''),
            'distance_km': round(row.get('distance_km', 0), 2),
            'score': round(row.get('GREG_SCORE', 0), 2)
        })

    return output
