# Tesi/experiments/analisi_popolazione.py
















import gzip
import json
import os
import time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from datetime import datetime

# --- CONFIGURAZIONE PERCORSI ---
DATA_FILE = os.path.join("..", "data_collection", "data", "dataset_definitivo_1-4.jsonl.gz")
OUTPUT_DIR = os.path.join("results", "pop_analysis")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def human_format(value, pos):
    """Formatta i numeri in k (mila) e M (milioni) per una migliore leggibilità."""
    if value >= 1_000_000:
        return f'{value/1_000_000:.1f}M'
    if value >= 1_000:
        return f'{value/1_000:.0f}k'
    return str(int(value))

def load_data():
    if not os.path.exists(DATA_FILE):
        print(f"Errore: File non trovato in {DATA_FILE}")
        return None

    file_size_total = os.path.getsize(DATA_FILE)
    start_time = time.time()
    print(f"Lettura dati da: {DATA_FILE} ({file_size_total / (1024**2):.2f} MB)...")
    
    user_data = {}
    current_count = 0
    
    with gzip.open(DATA_FILE, 'rt', encoding='utf-8') as f:
        # Accesso al file binario sottostante per calcolare la percentuale di avanzamento
        f_raw = f.buffer.fileobj 
        
        for line in f:
            line = line.strip()
            if not line: continue
            
            try:
                post = json.loads(line)
                author_did = post.get('author', {}).get('did')
                if not author_did: continue
                
                meta = post.get('author_meta', post.get('author_enriched', {}))
                
                if author_did not in user_data:
                    user_data[author_did] = {
                        'followers': meta.get('followers', 0), # Capitale sociale statico 
                        'total_eng': 0,
                        'post_count': 0
                    }
                
                # Successo del post (V_i) = Like + Repost [cite: 111, 112]
                eng = post.get('like_count', 0) + post.get('repost_count', 0)
                user_data[author_did]['total_eng'] += eng
                user_data[author_did]['post_count'] += 1
                
                current_count += 1
                # Log ogni 500.000 post con orario e percentuale
                if current_count % 500000 == 0:
                    ora_attuale = datetime.now().strftime("%H:%M:%S")
                    bytes_read = f_raw.tell()
                    percent = (bytes_read / file_size_total) * 100
                    print(f"[{ora_attuale}] Processati {current_count} post (circa {percent:.1f}%) - Utenti unici: {len(user_data)}")
                    
            except json.JSONDecodeError:
                continue

    df = pd.DataFrame.from_dict(user_data, orient='index')
    # avg_eng rappresenta la capacità media di generare interesse (beta_i) [cite: 162, 163]
    df['avg_eng'] = df['total_eng'] / df['post_count']
    
    end_time = time.time()
    print(f"Fatto: {current_count} post processati in {end_time - start_time:.2f}s.")
    return df

def run_analyses(df):
    print(f"Generazione grafici 'human-readable' in {OUTPUT_DIR}...")
    formatter = mtick.FuncFormatter(human_format)
    
    # --- 1. CURVA DI LORENZ (Disuguaglianza) ---
    plt.figure(figsize=(10, 6))
    f_sorted = np.sort(df['followers'].values)
    lorenz = np.cumsum(f_sorted) / f_sorted.sum()
    
    plt.fill_between(np.linspace(0, 1, len(lorenz)), lorenz, alpha=0.2, color='blue')
    plt.plot(np.linspace(0, 1, len(lorenz)), lorenz, color='blue', lw=2, label='Distribuzione reale')
    plt.plot([0, 1], [0, 1], '--', color='red', label='Uguaglianza perfetta')
    
    plt.title('Disuguaglianza: Chi possiede i follower su Bluesky?', fontsize=14)
    plt.xlabel('% Popolazione (dagli utenti più piccoli ai più grandi)', fontsize=12)
    plt.ylabel('% Totale Follower accumulati', fontsize=12)
    plt.legend()
    plt.grid(True, alpha=0.2)
    plt.savefig(os.path.join(OUTPUT_DIR, 'lorenz_readable.png'))

    # --- 2. POWER LAW (Rank vs Followers) [cite: 171] ---
    plt.figure(figsize=(10, 6))
    ranks = np.arange(1, len(f_sorted) + 1)
    plt.loglog(ranks, f_sorted[::-1], color='darkgreen', lw=2)
    
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.title('Classifica Follower: La "Coda Lunga" di Bluesky', fontsize=14)
    plt.xlabel('Posizione in Classifica (Ranking)', fontsize=12)
    plt.ylabel('Numero di Follower', fontsize=12)
    plt.grid(True, which="both", ls="-", alpha=0.2)
    plt.savefig(os.path.join(OUTPUT_DIR, 'power_law_readable.png'))

    # --- 3. MATRICE DI POSIZIONAMENTO (Analisi sociologica [cite: 158]) ---
    X_log = np.log1p(df[['followers', 'avg_eng']]) 
    X_scaled = StandardScaler().fit_transform(X_log)
    df['cluster'] = KMeans(n_clusters=3, random_state=42, n_init=10).fit_predict(X_scaled)

    plt.figure(figsize=(12, 8))
    
    # s=5 e alpha=0.1 per gestire l'alta densità di punti (300k+)
    scatter = plt.scatter(df['followers'], df['avg_eng'], 
                         c=df['cluster'], cmap='viridis', 
                         alpha=0.1, s=5, edgecolors='none')
    
    plt.xscale('log')
    plt.yscale('log')
    plt.gca().xaxis.set_major_formatter(formatter)
    plt.gca().yaxis.set_major_formatter(formatter)
    
    # Annotazioni per rendere i dati "human readable"
    plt.text(df['followers'].max()*0.1, df['avg_eng'].max()*0.5, 'INFLUENCER\nCONSOLIDATI', 
             fontsize=12, fontweight='bold', bbox=dict(facecolor='white', alpha=0.7))
    
    plt.text(df['followers'].min()+1, df['avg_eng'].max()*0.5, 'WANNA-BE\n(CREATORI VIRALI)', 
             fontsize=12, fontweight='bold', bbox=dict(facecolor='white', alpha=0.7))
    
    plt.text(df['followers'].min()+1, df['avg_eng'].min()+0.1, 'UTENTI\nCOMUNI', 
             fontsize=12, fontweight='bold', bbox=dict(facecolor='white', alpha=0.7))

    plt.xlabel('Capitale Sociale (Followers )', fontsize=12)
    plt.ylabel('Engagement Medio (Performance Dinamica )', fontsize=12)
    plt.title('Mappa Sociale di Bluesky: Chi sono i veri protagonisti?', fontsize=14)
    plt.grid(True, which="both", alpha=0.1)
    
    plt.savefig(os.path.join(OUTPUT_DIR, 'positioning_matrix_readable.png'), dpi=300)
    # Salvataggio delle statistiche aggregate per analisi future
    df.to_csv(os.path.join(OUTPUT_DIR, 'statistiche_utenti.csv'))

if __name__ == "__main__":
    stats_df = load_data()
    if stats_df is not None:
        run_analyses(stats_df)
        print(f"Analisi completata. Risultati in: {os.path.abspath(OUTPUT_DIR)}")