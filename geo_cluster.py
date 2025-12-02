import pandas as pd
from   sklearn.cluster import KMeans
import matplotlib.pyplot as plt  # Opzionale, se vuoi vedere il grafico

# CONFIGURAZIONE
INPUT_FILE = './dataset/clean_dataset.csv'
OUTPUT_FILE = './dataset/greg_clustered_data.csv'
NUM_CLUSTERS = 5  # Puoi aumentare questo numero se vuoi zone più piccole


def main():
    print("--- GREG Clustering Module ---")

    # 1. Carica Dati
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"Dataset caricato: {len(df)} righe")
    except FileNotFoundError:
        print("Errore: File non trovato.")
        return

    # 2. Prepara Coordinate
    # Rimuoviamo eventuali righe senza coordinate (se ce ne sono rimaste)
    mask_valid = df['Latitude'].notnull() & df['Longitude'].notnull()
    coords = df.loc[mask_valid, ['Latitude', 'Longitude']]

    if len(coords) == 0:
        print("ERRORE CRITICO: Nessuna coordinata valida trovata nel file.")
        return

    print(f"Avvio clustering su {len(coords)} punti geografici...")

    # 3. Applica K-Means
    kmeans = KMeans(n_clusters=NUM_CLUSTERS, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(coords)

    # 4. Assegna i risultati al DataFrame originale
    # Usiamo .loc per assegnare correttamente usando gli indici originali
    df.loc[mask_valid, 'geo_cluster'] = clusters.astype(int)

    # Riempie i cluster nulli (quelli senza coordinate) con -1 per indicare "Non assegnato"
    df['geo_cluster'] = df['geo_cluster'].fillna(-1).astype(int)

    # 5. Statistiche
    print("\n--- Risultati Clustering ---")
    print(df['geo_cluster'].value_counts().sort_index())
    print("(Nota: -1 indica righe senza coordinate)")

    # 6. Salva
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nFile salvato con successo: {OUTPUT_FILE}")
    print("La colonna 'geo_cluster' è stata aggiunta.")

    import matplotlib.pyplot as plt

    # ... (tutto il codice di clustering di prima) ...

    # AGGIUNGI QUESTO ALLA FINE DELLA FUNZIONE main():

    print("Generazione grafico in corso...")

    # Crea la figura
    plt.figure(figsize=(10, 8))

    # Disegna i punti (scatter plot)
    # c=clusters -> colora in base al numero del cluster
    # cmap='viridis' -> usa una scala di colori ben visibile
    plt.scatter(coords['Longitude'], coords['Latitude'], c=clusters, cmap='viridis', s=10, alpha=0.6, label='Volontari')

    # Disegna i Centroidi (i centri delle zone)
    centroids = kmeans.cluster_centers_
    plt.scatter(centroids[:, 1], centroids[:, 0], c='red', s=200, marker='x', label='Centroidi (Centri Zona)')

    # Abbellimento
    plt.title('Mappa dei Cluster Geografici GREG')
    plt.xlabel('Longitudine')
    plt.ylabel('Latitudine')
    plt.legend()
    plt.grid(True, alpha=0.3)

    # Mostra la finestra
    plt.show()


if __name__ == "__main__":
    main()

# ... (tutto il codice di clustering di prima) ...

