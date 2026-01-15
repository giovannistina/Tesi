# .../ experiments / analyze_census_100k.py



import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import time

# --- CONFIGURAZIONE ---
INPUT_CSV = "results/feed_stats/bluesky_feed_census_experimental.csv"
OUTPUT_FOLDER = "results/plots_100k"

def calculate_gini(array):
    """Calcola Gini index."""
    array = np.sort(array)
    index = np.arange(1, array.shape[0] + 1)
    n = array.shape[0]
    return ((np.sum((2 * index - n - 1) * array)) / (n * np.sum(array)))

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Errore: File {INPUT_CSV} non trovato.")
        return

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    print("📂 Caricamento dataset...")
    df = pd.read_csv(INPUT_CSV)
    
    # 2. Pulizia e Preparazione
    df = df[df['feed_likes'] >= 0]
    
    # Conversioni Logaritmiche
    df['log_likes'] = np.log10(df['feed_likes'] + 1)
    df['log_followers'] = np.log10(df['creator_followers'] + 1)
    
    # Conversione Data (Fondamentale per i grafici temporali)
    df['creation_date'] = pd.to_datetime(df['creation_date'])
    df['month_year'] = df['creation_date'].dt.to_period('M')

    print(f"📊 Analisi su {len(df)} feed totali.")

    # --- STATISTICHE REPORT ---
    gini_score = calculate_gini(df['feed_likes'].values)
    spearman_corr = df['creator_followers'].corr(df['feed_likes'], method='spearman')
    
    # Salviamo il report
    with open(os.path.join(OUTPUT_FOLDER, "summary_statistics.txt"), "w") as f:
        f.write(f"REPORT COMPLETO\nData: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Totale Feed: {len(df)}\n")
        f.write(f"Gini Index: {gini_score:.4f}\n")
        f.write(f"Correlazione Follower-Like: {spearman_corr:.4f}\n")
    
    # ==========================================
    # SEZIONE 1: DISTRIBUZIONE E DISUGUAGLIANZA
    # ==========================================

    # 1. Lorenz Curve
    print("📈 1/7 Generazione Lorenz Curve...")
    plt.figure(figsize=(8, 8))
    likes_sorted = np.sort(df['feed_likes'])
    lorenz = np.cumsum(likes_sorted) / np.sum(likes_sorted)
    lorenz = np.insert(lorenz, 0, 0)
    plt.plot(np.linspace(0, 1, len(lorenz)), lorenz, color='blue', lw=2, label='Bluesky')
    plt.plot([0, 1], [0, 1], color='red', ls='--', label='Perfect Equality')
    plt.title(f'1. Disuguaglianza (Gini = {gini_score:.2f})')
    plt.xlabel('% Cumulativa Feed')
    plt.ylabel('% Cumulativa Like')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{OUTPUT_FOLDER}/1_lorenz.png")
    plt.close()

    # 2. Hexbin Plot (Follower vs Like)
    print("📈 2/7 Generazione Hexbin Plot...")
    plt.figure(figsize=(10, 8))
    plt.hexbin(df['log_followers'], df['log_likes'], gridsize=40, cmap='inferno', mincnt=1)
    plt.colorbar(label='Densità di Feed')
    plt.xlabel('Follower Creatore (Log10)')
    plt.ylabel('Like Feed (Log10)')
    plt.title('2. Capitale Sociale vs Successo')
    plt.text(0.05, 0.95, f"Corr: {spearman_corr:.2f}", transform=plt.gca().transAxes, color='white', backgroundcolor='black')
    plt.savefig(f"{OUTPUT_FOLDER}/2_hexbin_social_capital.png")
    plt.close()

    # 3. Istogramma Like
    print("📈 3/7 Generazione Istogramma...")
    plt.figure(figsize=(10, 6))
    sns.histplot(df['log_likes'], bins=40, kde=True, color='skyblue')
    plt.title('3. Distribuzione Popolarità (Long Tail)')
    plt.xlabel('Like (Log10)')
    plt.savefig(f"{OUTPUT_FOLDER}/3_histogram_likes.png")
    plt.close()

    # ==========================================
    # SEZIONE 2: ANALISI TEMPORALE
    # ==========================================

    # 4. Trend Temporale (Nuovi Feed per Mese)
    print("📈 4/7 Generazione Trend Temporale...")
    plt.figure(figsize=(12, 6))
    # Contiamo quanti feed sono nati ogni mese
    monthly_counts = df['month_year'].value_counts().sort_index()
    # Convertiamo in stringhe per il plot
    monthly_counts.index = monthly_counts.index.astype(str)
    
    sns.lineplot(x=monthly_counts.index, y=monthly_counts.values, marker='o', color='green', lw=2)
    plt.xticks(rotation=45)
    plt.title('4. Evoluzione Temporale: Nuovi Feed creati per Mese')
    plt.ylabel('Numero Nuovi Feed')
    plt.xlabel('Mese')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_FOLDER}/4_time_trend.png")
    plt.close()

    # ==========================================
    # SEZIONE 3: ANALISI CREATORI
    # ==========================================

    # 5. Top 20 Creators (Bar Chart Orizzontale)
    print("📈 5/7 Generazione Top Creators...")
    plt.figure(figsize=(10, 8))
    # Sommiamo i like per ogni creatore
    top_creators = df.groupby('creator_handle')['feed_likes'].sum().nlargest(20).sort_values(ascending=True)
    
    top_creators.plot(kind='barh', color='purple')
    plt.title('5. Top 20 Creatori per Like Totali accumulati')
    plt.xlabel('Somma dei Like su tutti i loro feed')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_FOLDER}/5_top_creators.png")
    plt.close()

    # 6. Produttività (Quanti feed fa un creatore?)
    print("📈 6/7 Generazione Produttività...")
    plt.figure(figsize=(10, 6))
    # Contiamo quante volte appare ogni DID
    feeds_per_creator = df['creator_did'].value_counts()
    # Usiamo scala logaritmica anche qui perché c'è chi ne fa 1 e chi ne fa 100
    sns.histplot(feeds_per_creator, bins=30, log_scale=True, color='orange')
    plt.title('6. Produttività: Quanti feed crea un singolo utente?')
    plt.xlabel('Numero di Feed creati (Log Scale)')
    plt.ylabel('Numero di Creatori')
    plt.axvline(x=1, color='red', linestyle='--', label='1 Solo Feed')
    plt.legend()
    plt.savefig(f"{OUTPUT_FOLDER}/6_creator_productivity.png")
    plt.close()

    # ==========================================
    # SEZIONE 4: CORRELAZIONI
    # ==========================================

    # 7. Heatmap Correlazioni
    print("📈 7/7 Generazione Heatmap...")
    plt.figure(figsize=(8, 6))
    # Selezioniamo solo colonne numeriche utili
    corr_matrix = df[['feed_likes', 'creator_followers', 'log_likes', 'log_followers']].corr(method='spearman')
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt=".2f")
    plt.title('7. Matrice di Correlazione (Spearman)')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_FOLDER}/7_correlation_matrix.png")
    plt.close()

    print("\n✅ FINITO! Grafici salvati in:", os.path.abspath(OUTPUT_FOLDER))

if __name__ == "__main__":
    main()