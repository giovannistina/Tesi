# Tesi / data_collection / campionamento_utenti.py

import pandas as pd
import numpy as np
import os
from sklearn.cluster import KMeans
from datetime import datetime

# --- CONFIGURAZIONE ---
INPUT_CSV = os.path.expanduser('~/Tesi/data_collection/data/utenti_completi.csv')
OUTPUT_DIR = os.path.expanduser('~/Tesi/data_collection/data/')
TARGET_SIZE = 30000

def campiona():
    print("📂 Caricamento dati dal CSV compatto...")
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Errore: File {INPUT_CSV} non trovato. Esegui prima l'estrazione.")
        return

    df = pd.read_csv(INPUT_CSV)
    
    # 1. Filtro attività minima
    df = df[(df['post_count'] >= 5) & (df['total_engagement'] > 0)].copy()
    df['avg_engagement'] = df['total_engagement'] / df['post_count']

    print("🧮 Esecuzione Clustering K-Means...")
    # Trasformazione logaritmica per gestire la distribuzione Power Law
    X = np.log1p(df[['avg_engagement']].values)
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    df['cluster_label'] = kmeans.fit_predict(X)
    
    # Ordinamento Tier: 0=Low, 1=Medium, 2=High
    centers = kmeans.cluster_centers_.flatten()
    mapping = {old: new for new, old in enumerate(np.argsort(centers))}
    df['Tier'] = df['cluster_label'].map(mapping).map({0:'Low', 1:'Medium', 2:'High'})

    # 2. Statistiche Popolazione Originale
    pop_stats = df.groupby('Tier')['avg_engagement'].agg(['count', 'min', 'max', 'mean']).reindex(['Low', 'Medium', 'High'])
    total_active = len(df)

    # 3. Campionamento Proporzionale
    sampled_dfs = []
    print("🎯 Generazione campione proporzionale...")
    for tier in ['Low', 'Medium', 'High']:
        n_available = len(df[df['Tier'] == tier])
        proportion = n_available / total_active
        n_target = int(proportion * TARGET_SIZE)
        
        sampled_dfs.append(df[df['Tier'] == tier].sample(min(n_target, n_available), random_state=42))

    final_df = pd.concat(sampled_dfs)

    # 4. Statistiche Campione Finale
    final_stats = final_df.groupby('Tier')['avg_engagement'].agg(['count', 'min', 'max', 'mean']).reindex(['Low', 'Medium', 'High'])

    # --- SALVATAGGIO OUTPUT (Solo TXT) ---

    # A. TXT Lista DID (per il Crawler delle timeline)
    final_txt_did = os.path.join(OUTPUT_DIR, "lista_did_30k.txt")
    final_df[['did']].to_csv(final_txt_did, index=False, header=False)

    # B. TXT Report Metodologico
    report_path = os.path.join(OUTPUT_DIR, "report_campionamento.txt")
    with open(report_path, "w") as f:
        f.write(f"RECAP CAMPIONAMENTO UTENTI - {datetime.now().strftime('%d/%m/%Y %H:%M')}\n")
        f.write("="*60 + "\n")
        f.write("METODOLOGIA DI CALCOLO:\n")
        f.write("1. Metrica: Engagement Medio (Somma Like, Repost, Commenti / Numero Post).\n")
        f.write("2. Normalizzazione: Trasformazione Logaritmica [log(1+x)] applicata all'engagement\n")
        f.write("   per correggere la forte asimmetria (Power Law) della distribuzione social.\n")
        f.write("3. Clustering: Algoritmo K-Means (k=3) per identificare i confini naturali\n")
        f.write("   tra le classi di utenti (Low, Medium, High engagement).\n")
        f.write("4. Campionamento: Stratificato Proporzionale. Il campione target di 30.000 utenti\n")
        f.write("   mantiene le stesse percentuali di rappresentazione della popolazione reale.\n")
        f.write("="*60 + "\n\n")
        
        f.write("--- STATISTICHE POPOLAZIONE TOTALE (CANDIDATI) ---\n")
        f.write(pop_stats.to_string())
        f.write(f"\nTotale utenti idonei: {total_active}\n\n")
        
        f.write("--- STATISTICHE CAMPIONE SELEZIONATO (OUTPUT) ---\n")
        f.write(final_stats.to_string())
        f.write(f"\nTotale utenti campionati: {len(final_df)}\n")
        f.write("="*60 + "\n")

    print(f"✅ Campionamento ultimato!")
    print(f"📄 Report salvato in: {report_path}")
    print(f"🆔 Lista DID salvata in: {final_txt_did}")

if __name__ == "__main__":
    campiona()