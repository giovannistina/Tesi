# .../ experiments / 2_analyze_census_100k.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np  # Importato qui in alto (Fondamentale)
import os
import time
from scipy import stats
import matplotlib.ticker as ticker
import re

# ==========================================
# 1. Configurazione e Funzioni Helper
# ==========================================
# MODIFICA: Aggiornato nome file input con la versione che include le bio
INPUT_CSV = "../data_collection/results/feed_stats/bluesky_feed_census_v2_with_lang_and_bios.csv"
OUTPUT_FOLDER = "results/plots/feed_analysis"

def calculate_gini(array):
    """Calcola Gini index su array numpy."""
    if len(array) == 0: return 0
    array = np.sort(array)
    index = np.arange(1, array.shape[0] + 1)
    n = array.shape[0]
    return ((np.sum((2 * index - n - 1) * array)) / (n * np.sum(array)))

def remove_emojis(text):
    """Rimuove emoji mantenendo caratteri standard e accenti."""
    if pd.isna(text): return "Unknown"
    return re.sub(r'[^\x00-\xFFFF]', '', str(text)).strip()

def main():
    # --- Check Esistenza File ---
    # Normalizziamo il path per evitare errori su sistemi diversi
    input_path = os.path.normpath(os.path.join(os.path.dirname(__file__), INPUT_CSV))
    
    if not os.path.exists(input_path):
        print(f"❌ Errore: File non trovato in: {input_path}")
        # Fallback per debug: prova a cercarlo nella cartella corrente se non lo trova nel path relativo
        if os.path.exists(os.path.basename(INPUT_CSV)):
             input_path = os.path.basename(INPUT_CSV)
             print(f"⚠️ Trovato file nella cartella corrente, uso: {input_path}")
        else:
             return

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    print(f"📂 Caricamento dataset: {os.path.basename(input_path)}...")
    df = pd.read_csv(input_path)
    
    # Metadata iniziali
    original_count = len(df)
    
    # ==========================================
    # 2. Pulizia e Filtri
    # ==========================================
    
    # A) Conversione Date (Ottimizzata in un passaggio)
    df['creation_date'] = pd.to_datetime(df['creation_date']).dt.tz_localize(None)
    
    # B) Filtro Temporale (Dal 2023 in poi)
    cutoff_date = "2023-01-01"
    pre_2023_mask = df['creation_date'] < pd.to_datetime(cutoff_date)
    removed_by_date = pre_2023_mask.sum()
    df = df[~pre_2023_mask]
    
    # C) Sanity Check (Like non negativi)
    invalid_likes_mask = df['feed_likes'] < 0
    removed_invalid = invalid_likes_mask.sum()
    df = df[~invalid_likes_mask]

    # D) Filtro Creatori Eliminati (Deleted/Suspended)
    # Rimuoviamo righe dove la bio è "N/A (Error or Deleted)"
    removed_deleted_creators = 0
    if 'creator_description' in df.columns:
        # Ci assicuriamo di trattare i NaN come stringhe vuote per evitare errori di confronto
        deleted_mask = df['creator_description'].fillna("") == "N/A (Error or Deleted)"
        removed_deleted_creators = deleted_mask.sum()
        df = df[~deleted_mask]
    
    # Conteggi Finali
    final_count = len(df)
    
    print(f"🧹 Pulizia completata.")
    print(f"   - Rimossi per data (< {cutoff_date}): {removed_by_date}")
    print(f"   - Rimossi per errori dati (Like < 0): {removed_invalid}")
    print(f"   - Rimossi per creatore eliminato: {removed_deleted_creators}")
    print(f"📊 Analisi su {final_count} feed totali.")

    
    # ==========================================
    # 3. Calcolo Variabili e Statistiche
    # ==========================================
    
    # Trasformazioni Logaritmiche (+1 per evitare log(0))
    df['log_likes'] = np.log10(df['feed_likes'] + 1)
    df['log_followers'] = np.log10(df['creator_followers'] + 1)
    
    # Variabili Temporali
    df['month_year'] = df['creation_date'].dt.to_period('M')
    df['days_old'] = (pd.Timestamp.now() - df['creation_date']).dt.days.clip(lower=1)
    
    # Indici di Viralità
    df['virality_index'] = df['feed_likes'] / (df['creator_followers'] + 1)
    df['log_virality'] = np.log10(df['virality_index'] + 1e-5) 

    # Statistiche Globali
    gini_score = calculate_gini(df['feed_likes'].values)
    spearman_corr = df['creator_followers'].corr(df['feed_likes'], method='spearman')
    pearson_corr_log = df['log_followers'].corr(df['log_likes'], method='pearson')

    # ==========================================
    # 4. Scrittura Report (Ottimizzata)
    # ==========================================
    print("📝 Scrittura report metodologico...")
    
    # Calcolo statistiche linguistiche per il report
    lang_stats = ""
    if 'language' in df.columns:
        en_count = len(df[df['language']=='en'])
        percent = (en_count/final_count)*100 if final_count > 0 else 0
        lang_stats = f"   Feed Inglesi (en):                {en_count} ({percent:.1f}% del campione)\n"

    # Creazione testo report usando f-string multiriga (Molto più leggibile)
    report_content = f"""======================================================
   REPORT ANALISI STATISTICA - BLUESKY CENSUS
======================================================

1. METODOLOGIA E PROVENANCE
   Il dataset è stato generato mediante un approccio ibrido 'Scrape-then-Validate':
   A. Discovery: Web Scraping iterativo di 'Bluesky Directory' per identificare URL candidati.
   B. Validation: Interrogazione puntuale dell'API ufficiale Bluesky (AT Protocol).
   C. Enrichment: Estrazione di metriche verificate (Like, Follower Autore, Data Creazione).
   Data Elaborazione:    {time.strftime('%Y-%m-%d %H:%M:%S')}
   Script Generatore:    {os.path.basename(__file__)}
   File Report:          summary_statistics.txt
   File Sorgente Dati:   {os.path.basename(input_path)}

2. DATA CLEANING & FILTRAGGIO
   Popolazione Iniziale (Raw):       {original_count} feed scaricati.
   Criterio di Esclusione:           Feed creati prima del {cutoff_date}.
   --> Feed Eliminati (Obsoleti):    {removed_by_date}
   --> Feed Eliminati (Errori):      {removed_invalid}
   -------------------------------------------
   POPOLAZIONE FINALE (ANALIZZATA): {final_count} feed.

3. RISULTATI STATISTICI
   Gini Index (Disuguaglianza):     {gini_score:.4f}
   Spearman Corr (Rank):            {spearman_corr:.4f} (Relazione ordinale)
   Pearson Corr (Log-Log):          {pearson_corr_log:.4f} (Relazione lineare su scala Log)

4. STATISTICHE LINGUISTICHE
{lang_stats}
"""

    # Scrittura su file in un colpo solo
    report_path = os.path.join(OUTPUT_FOLDER, "summary_statistics.txt")
    with open(report_path, "w") as f:
        f.write(report_content)


    # ==========================================
    # GENERAZIONE GRAFICI (1-9)
    # ==========================================

    # Controllo di sicurezza: procediamo solo se ci sono dati
    if len(df) == 0:
        print("⚠️ Nessun dato disponibile per i grafici. Termino qui.")
        return

    # ################################
    # 1. Lorenz Curve (Disuguaglianza)
    # ################################
    print("📈 1/9 Lorenz Curve...")
    plt.figure(figsize=(8, 8))
    
    # Calcolo curva
    likes_sorted = np.sort(df['feed_likes'])
    lorenz = np.cumsum(likes_sorted) / np.sum(likes_sorted)
    lorenz = np.insert(lorenz, 0, 0) # Aggiunge il punto (0,0)
    
    # Plot
    plt.plot(np.linspace(0, 1, len(lorenz)), lorenz, color='blue', lw=2, label='Bluesky')
    plt.plot([0, 1], [0, 1], color='red', ls='--', label='Perfect Equality')
    
    plt.title(f'1. Disuguaglianza Distribuzione Like (Gini = {gini_score:.2f})')
    plt.xlabel('% Cumulativa Feed')
    plt.ylabel('% Cumulativa Like')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{OUTPUT_FOLDER}/1_lorenz.png"); plt.close()

    # ################################
    # 2. Analisi Capitale Sociale & Creator Tiers
    # ################################
    print("📈 2/9 Analisi Capitale Sociale & Creator Tiers...")

    # --- Preparazione Dati (Classi) ---
    # Definiamo le fasce una volta sola
    bins_tiers = [-1, 100, 1000, 10000, float('inf')]
    labels_tiers = ['Emergenti\n(<100)', 'Micro\n(100-1k)', 'Macro\n(1k-10k)', 'Star\n(>10k)']
    df['creator_tier'] = pd.cut(df['creator_followers'], bins=bins_tiers, labels=labels_tiers)

    # --- 2a. Hexbin Social Capital (Log-Log) ---
    print("   > Generazione 2a (Hexbin Social Capital)...")
    plt.figure(figsize=(10, 8))
    
    plt.hexbin(df['log_followers'], df['log_likes'], gridsize=40, cmap='inferno', mincnt=1)
    plt.colorbar(label='Densità di Feed')
    
    plt.xlabel('Follower Creatore (Log10)')
    plt.ylabel('Like Feed (Log10)')
    plt.title('2a. Capitale Sociale vs Successo (Scala Log-Log)')
    
    # Box statistico (usa variabili calcolate nella sezione 4)
    stats_text = f"Spearman: {spearman_corr:.2f}\nPearson (Log): {pearson_corr_log:.2f}"
    plt.text(0.05, 0.90, stats_text, transform=plt.gca().transAxes, 
             color='white', backgroundcolor='black', fontsize=11, 
             bbox=dict(facecolor='black', alpha=0.7))
    plt.savefig(f"{OUTPUT_FOLDER}/2a_hexbin_social.png"); plt.close()

    # --- 2b. Creator Tiers (Boxplot) ---
    print("   > Generazione 2b (Creator Tiers - Boxplot)...")
    plt.figure(figsize=(12, 8))
    
    sns.boxplot(
        x='creator_tier', y='log_likes', hue='creator_tier', 
        data=df, palette="viridis", showfliers=False, legend=False
    )
    plt.title('2b. Distribuzione Successo per Fascia di Popolarità')
    plt.xlabel('Tipologia di Creatore')
    plt.ylabel('Like Feed (Scala Log10)')
    plt.grid(True, alpha=0.3, axis='y')
    plt.savefig(f"{OUTPUT_FOLDER}/2b_creator_tiers_boxplot.png"); plt.close()

    # --- 2c. Creator Tiers (Strip Plot) ---
    print("   > Generazione 2c (Creator Tiers - Strip Plot)...")
    plt.figure(figsize=(12, 8))
    
    sns.stripplot(
        x='creator_tier', y='feed_likes', hue='creator_tier', 
        data=df, palette="viridis_r", jitter=0.3, size=5, alpha=0.8, legend=False
    )
    plt.title('2c. Distribuzione Reale dei Feed (Palette Invertita)')
    plt.xlabel('Tipologia di Creatore')
    plt.ylabel('Numero di Like (Valore Assoluto)')
    plt.grid(True, alpha=0.3, axis='y')
    plt.savefig(f"{OUTPUT_FOLDER}/2c_creator_tiers_stripplot.png"); plt.close()

    
    
    # ################################
    # 3. Analisi Distribuzione Like (Long Tail & Scientific)
    # ################################
    print("📈 3/9 Analisi Distribuzione Like (Pie, Standard, Scientifici)...")

    # --- Preparazione Dati (Classi) ---
    bins_class = [-1, 10, 100, 1000, 10000, float('inf')]
    labels_class = ['0-10', '11-100', '101-1k', '1k-10k', '>10k']
    df['likes_class'] = pd.cut(df['feed_likes'], bins=bins_class, labels=labels_class)

    # --- 3a. Grafico a Torta (0 vs 1 vs Altri) ---
    print("   > Generazione 3a (Pie Chart: 0 vs 1 vs >1)...")
    plt.figure(figsize=(10, 8))
    
    count_0 = len(df[df['feed_likes'] == 0])
    count_1 = len(df[df['feed_likes'] == 1])
    count_rest = len(df[df['feed_likes'] > 1])
    
    # Dati per la torta
    sizes = [count_0, count_1, count_rest]
    labels = ['0 Like', '1 Like', '> 1 Like']
    colors = ['lightgray', 'lightblue', 'dodgerblue']
    explode = (0.05, 0, 0)  # "Esplode" leggermente la fetta degli zero per evidenziarla
    
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, 
            colors=colors, explode=explode, textprops={'fontsize': 12})
    
    plt.title(f'3a. Composizione del Dataset: La sfida dei "Cold Start"\n(Totale Feed: {len(df):,})')
    
    # Aggiungiamo i valori assoluti nella legenda o a lato
    plt.legend(
        title="Conteggi Assoluti",
        labels=[f"{l}: {s:,}" for l, s in zip(labels, sizes)],
        loc="upper right",
        bbox_to_anchor=(1.15, 1)
    )
    plt.savefig(f"{OUTPUT_FOLDER}/3a_pie_zeros.png", bbox_inches='tight'); plt.close()

    # --- 3b. Istogramma Like (Log Scale - X Axis) ---
    print("   > Generazione 3b (Log Scale X)...")
    plt.figure(figsize=(10, 6))
    sns.histplot(df['log_likes'], bins=40, kde=True, color='skyblue')
    plt.title('3b. Distribuzione Popolarità (Scala X Logaritmica)')
    plt.xlabel('Like (Log10)')
    plt.ylabel('Frequenza (Scala Lineare)')
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{OUTPUT_FOLDER}/3b_histogram_likes_log.png"); plt.close()

    # --- 3c. Istogramma Like (Linear Zoom 0-100) ---
    print("   > Generazione 3c (Linear Zoom 0-100)...")
    plt.figure(figsize=(12, 6))
    subset_df = df[df['feed_likes'] <= 100]
    sns.histplot(subset_df['feed_likes'], bins=100, kde=False, color='dodgerblue', element="step", alpha=0.6)
    plt.title('3c. Distribuzione Popolarità (Zoom Lineare: 0-100 Like)')
    plt.xlabel('Numero di Like (Valore Assoluto)')
    plt.ylabel('Numero di Feed')
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{OUTPUT_FOLDER}/3c_histogram_likes_zoom_100.png"); plt.close()

    # --- 3d. Istogramma Classi (Barplot + Etichette) ---
    print("   > Generazione 3d (Distribuzione per Classi)...")
    plt.figure(figsize=(12, 7))
    class_counts = df['likes_class'].value_counts().sort_index()
    ax = sns.barplot(x=class_counts.index, y=class_counts.values, hue=class_counts.index, palette="Blues_d", legend=False)
    
    plt.title('3d. Distribuzione Feed per Classi di Popolarità')
    plt.xlabel('Fascia di Like (Classi)')
    plt.ylabel('Numero di Feed')
    plt.grid(True, alpha=0.3, axis='y')
    ax.set_ylim(0, max(class_counts.values) * 1.15) 

    total_feeds = len(df)
    for i, v in enumerate(class_counts.values):
        perc = (v / total_feeds) * 100
        ax.text(i, v + (max(class_counts.values)*0.02), f"{v:,}\n({perc:.1f}%)", ha='center', fontsize=11, color='black')
    plt.savefig(f"{OUTPUT_FOLDER}/3d_histogram_classes.png"); plt.close()

    # --- 3e. Ipson Logaritmica (Scala Y Log) ---
    print("   > Generazione 3e (Ipson - Scala Y Log)...")
    plt.figure(figsize=(10, 6))
    sns.histplot(df['log_likes'], bins=40, kde=False, color='lightgreen', edgecolor='black')
    plt.yscale('log') 
    
    plt.title('3e. Distribuzione Like (Ipson: Asse Y Logaritmico)')
    plt.xlabel('Like (Scala Log10)')
    plt.ylabel('Frequenza (Scala Logaritmica)')
    plt.grid(True, alpha=0.3, which="both")
    plt.text(0.05, 0.90, "Nota: L'asse Y logaritmico permette di\nvedere le frequenze molto basse nella coda.", 
             transform=plt.gca().transAxes, fontsize=10, bbox=dict(facecolor='white', alpha=0.8))
    plt.savefig(f"{OUTPUT_FOLDER}/3e_histogram_ipson_ylog.png"); plt.close()

    # --- 3f. CDF in Log (Survival Function / Power Law Check) ---
    print("   > Generazione 3f (CCDF Log-Log)...")
    plt.figure(figsize=(10, 6))
    
    data_nonzero = df[df['feed_likes'] > 0]['feed_likes']
    if len(data_nonzero) > 0:
        sorted_data = np.sort(data_nonzero)
        yvals = np.arange(len(sorted_data), 0, -1) / len(sorted_data)
        
        plt.plot(sorted_data, yvals, marker='.', linestyle='none', color='crimson', alpha=0.3, markersize=3)
        plt.xscale('log'); plt.yscale('log')
        
        plt.title('3f. Distribuzione CCDF (Log-Log Plot)')
        plt.xlabel('Numero di Like (Scala Log)')
        plt.ylabel('P(X > x) (Scala Log)')
        plt.grid(True, alpha=0.3, which="both")
        plt.text(0.05, 0.05, "Linearità = Power Law", transform=plt.gca().transAxes, bbox=dict(facecolor='white', alpha=0.8))
    else:
        plt.text(0.5, 0.5, "Dati insufficienti (>0) per Log-Log plot", ha='center')
        
    plt.savefig(f"{OUTPUT_FOLDER}/3f_cdf_log_log.png"); plt.close()



    # ################################
    # 4. Trend Temporale (Time Series)
    # ################################
    print("📈 4/9 Trend Temporale...")
    plt.figure(figsize=(12, 6))
    
    # Calcolo frequenze mensili
    monthly_counts = df['month_year'].value_counts().sort_index()
    
    # Conversione indice a stringa per plot pulito (evita errori con PeriodDtype)
    x_dates = monthly_counts.index.astype(str)
    y_values = monthly_counts.values
    
    sns.lineplot(x=x_dates, y=y_values, marker='o', color='green', lw=2)
    
    plt.title(f'4. Nuovi Feed creati per Mese (Post-{cutoff_date})')
    plt.xlabel('Mese-Anno')
    plt.ylabel('Nuovi Feed Creati')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_FOLDER}/4_time_trend.png"); plt.close()

    
    
    # ################################
    # 5. TOP LISTS (Creators & Feeds)
    # ################################
    print("📈 5/9 Top Lists (Creators & Feeds)...")
    
    # --- 5a. Top 20 Creators (Like Totali) ---
    print("   > Generazione 5a (Top 20 Creators)...")
    plt.figure(figsize=(10, 8))
    
    # Raggruppa per autore e somma i like di tutti i suoi feed
    top_creators = df.groupby('creator_handle')['feed_likes'].sum().nlargest(20).reset_index()
    
    sns.barplot(x='feed_likes', y='creator_handle', data=top_creators, color='purple')
    
    plt.title('5a. Top 20 Creatori per Like Totali accumulati')
    plt.xlabel('Somma dei Like su tutti i loro feed')
    plt.ylabel('Handle Creatore')
    plt.grid(True, alpha=0.3, axis='x')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_FOLDER}/5a_top_creators.png"); plt.close()

    # --- 5b. Top 20 Individual Feeds (Feed Singoli) ---
    print("   > Generazione 5b (Top 20 Individual Feeds)...")
    plt.figure(figsize=(10, 10))
    
    # 1. Smart Name Detection (Priorità: Display Name > Name > URI)
    name_col = 'uri' # Fallback
    for col in ['name', 'display_name']:
        if col in df.columns:
            name_col = col
    
    # 2. Pulizia Nomi
    clean_names = df[name_col].apply(remove_emojis)
    clean_handles = df['creator_handle'].apply(remove_emojis)
    
    # 3. Creazione Label: "Nome Feed\n(@handle)"
    df['plot_label'] = clean_names + "\n(" + clean_handles + ")"
    
    # 4. Plot
    top_feeds = df.nlargest(20, 'feed_likes')
    sns.barplot(x='feed_likes', y='plot_label', data=top_feeds, color='purple')
    
    plt.title('5b. Top 20 Feed Singoli per numero di Like')
    plt.xlabel('Numero di Like')
    plt.ylabel('')
    plt.yticks(fontsize=9)
    plt.grid(True, alpha=0.3, axis='x')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_FOLDER}/5b_top_feeds.png"); plt.close()


    # ################################
    # 6. Produttività (Feed per Creatore)
    # ################################
    print("📈 6/9 Produttività...")
    
    # Conteggio feed per ogni DID univoco
    feeds_per_creator = df['creator_did'].value_counts()
    total_creators = len(feeds_per_creator)

    # --- 6a. Scala Logaritmica (Panoramica Completa) ---
    print("   > Generazione 6a (Log Scale)...")
    plt.figure(figsize=(10, 6))
    
    sns.histplot(feeds_per_creator, bins=30, log_scale=True, color='orange')
    
    plt.title('6a. Quanti feed crea un singolo utente? (Intero Dataset - Scala Log)')
    plt.xlabel('Numero di Feed creati (Log Scale)')
    plt.ylabel('Numero di Creatori')
    plt.axvline(x=1, color='red', ls='--', label='1 Feed (Moda)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{OUTPUT_FOLDER}/6a_productivity_log.png"); plt.close()

    # --- 6b. Scala Lineare (Zoom 1-25) ---
    print("   > Generazione 6b (Linear Zoom 1-25)...")
    plt.figure(figsize=(12, 7))
    
    # Filtro: Prendiamo solo chi ha creato tra 1 e 25 feed
    LIMIT_X = 25
    subset_productivity = feeds_per_creator[feeds_per_creator <= LIMIT_X]
    
    # Plot Discreto
    ax = sns.histplot(
        subset_productivity, 
        discrete=True, 
        color='moccasin', 
        edgecolor='orange', 
        linewidth=1
    )
    
    plt.title(f'6b. Dettaglio Produttività (Zoom: 1-{LIMIT_X} Feed)')
    plt.xlabel('Numero di Feed Creati')
    plt.ylabel('Numero di Creatori')
    
    # Tick asse X
    plt.xticks(range(1, LIMIT_X + 1))
    plt.xlim(0.5, LIMIT_X + 0.5)
    plt.grid(True, alpha=0.3, axis='y')
    
    # 1. Etichetta specifica sulla colonna "1" (con specifica "del Totale")
    count_1 = subset_productivity.value_counts().get(1, 0)
    if count_1 > 0:
        perc_1 = (count_1 / total_creators) * 100
        ax.text(
            1, count_1, 
            f"{count_1:,}\n({perc_1:.1f}% del Totale)", 
            ha='center', va='bottom', 
            fontsize=10, fontweight='bold', color='black'
        )

    # 2. Riquadro Metodologico Esplicito
    creators_shown = len(subset_productivity)
    stats_text = (
        f"ZOOM VISIVO (1-{LIMIT_X} Feed)\n"
        f"--------------------------\n"
        f"Utenti visualizzati: {creators_shown:,}\n"
        f"Popolazione Totale: {total_creators:,}\n\n"
        f"*Nota: La percentuale è calcolata\n"
        f"sulla Popolazione Totale."
    )
    
    plt.text(
        0.95, 0.95, stats_text, 
        transform=ax.transAxes, 
        ha='right', va='top', 
        fontsize=10, 
        bbox=dict(facecolor='white', alpha=0.9, edgecolor='gray', boxstyle='round')
    )

    plt.savefig(f"{OUTPUT_FOLDER}/6b_productivity_linear_zoom.png"); plt.close()


    # ################################
    # 7. Distribuzione Lingue
    # ################################
    print("📈 7/9 Distribuzione Lingue...")
    
    if 'language' not in df.columns:
        print("   ⚠️ Colonna 'language' non trovata. Salto grafico 7.")
    else:
        # 1. Mappatura ISO -> Nome Esteso
        iso_map = {
            'en': 'Inglese', 'ja': 'Giapponese', 'pt': 'Portoghese', 'de': 'Tedesco',
            'fr': 'Francese', 'es': 'Spagnolo', 'ko': 'Coreano', 'zh': 'Cinese',
            'ru': 'Russo', 'it': 'Italiano', 'nl': 'Olandese', 'pl': 'Polacco',
            'tr': 'Turco', 'uk': 'Ucraino', 'vi': 'Vietnamita', 'th': 'Thailandese',
            'id': 'Indonesiano', 'hi': 'Hindi', 'ar': 'Arabo', 'ca': 'Catalano'
        }
        
        # Mappa e riempi i mancanti con il codice originale
        df['lang_full'] = df['language'].map(iso_map).fillna(df['language'])
        
        # 2. Aggregazione (Top 10 + Altri)
        lang_counts = df['lang_full'].value_counts()
        top_langs = lang_counts.head(10)
        
        others_count = lang_counts.iloc[10:].sum()
        if others_count > 0:
            top_langs['Altri'] = others_count

        # 3. Plot
        plt.figure(figsize=(12, 8))
        ax = sns.barplot(x=top_langs.index, y=top_langs.values, color='teal')
        
        plt.title('7. Distribuzione Lingue dei Feed')
        plt.xlabel('Lingua')
        plt.ylabel('Numero di Feed')
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3, axis='y')
        
        # Margine per etichette
        ax.set_ylim(0, max(top_langs.values) * 1.15)
        
        # 4. Etichette (Valore + %)
        total_lang = top_langs.sum()
        for i, v in enumerate(top_langs.values):
            perc = (v / total_lang) * 100
            ax.text(i, v + (max(top_langs.values)*0.02), f"{v:,}\n({perc:.1f}%)", 
                    ha='center', fontsize=11, color='black')
            
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_FOLDER}/7_languages.png"); plt.close()

        # ################################
    # 8. Anzianità vs Successo
    # ################################
    print("📈 8/9 Anzianità vs Successo (Scatter Plots & Table)...")
    
    # --- 8a. Scala Logaritmica (Scatter) ---
    print("   > Generazione 8a (Log Scale)...")
    plt.figure(figsize=(12, 8))
    
    sns.scatterplot(x='days_old', y='log_likes', data=df, alpha=0.1, s=15, color='dodgerblue', edgecolor=None)
    sns.regplot(x='days_old', y='log_likes', data=df, scatter=False, color='red', ci=None, line_kws={"linewidth": 2})
    
    plt.title('8a. Anzianità vs Successo (Scala Logaritmica)')
    plt.xlabel('Giorni di Anzianità (Età)')
    plt.ylabel('Like (Log10)')
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{OUTPUT_FOLDER}/8a_age_vs_success_log.png"); plt.close()

    # --- 8b. Scala Lineare (Scatter) ---
    print("   > Generazione 8b (Linear Scale)...")
    plt.figure(figsize=(12, 8))
    
    sns.scatterplot(x='days_old', y='feed_likes', data=df, alpha=0.3, s=15, color='dodgerblue', edgecolor=None)
    sns.regplot(x='days_old', y='feed_likes', data=df, scatter=False, color='red', ci=None, line_kws={"linewidth": 2})
    
    plt.title('8b. Anzianità vs Successo (Scala Lineare)')
    plt.xlabel('Giorni di Anzianità (Età)')
    plt.ylabel('Numero di Like (Valore Assoluto)')
    plt.grid(True, alpha=0.3)
    plt.savefig(f"{OUTPUT_FOLDER}/8b_age_vs_success_linear.png"); plt.close()

    # --- 8c. Tabella "Neonati & Virali" (CSV) ---
    print("   > Generazione 8c (Tabella CSV 'New & Viral')...")
    
    # 1. Filtri: < 10 giorni E > 100 like
    mask_recent = df['days_old'] < 10
    mask_viral = df['feed_likes'] > 100
    viral_new_df = df[mask_recent & mask_viral].copy().sort_values(by='feed_likes', ascending=False)
    
    if not viral_new_df.empty:
        # 2. Smart Name Detection (Coerente con Sez. 5)
        name_col = 'uri'
        for col in ['name', 'display_name']:
            if col in df.columns: name_col = col

        # 3. Selezione Colonne Sicura (MODIFICATO: Include creator_description se presente)
        cols_to_save = [name_col, 'description', 'creation_date', 'creator_handle', 'creator_followers', 'feed_likes', 'days_old', 'creator_description']
        cols_final = [c for c in cols_to_save if c in viral_new_df.columns]
        
        export_table = viral_new_df[cols_final]
        
        # 4. Rinomina per output leggibile
        rename_map = {
            name_col: 'Nome Feed', 'description': 'Descrizione Feed',
            'creation_date': 'Data Creazione', 'creator_handle': 'Handle Creator',
            'creator_followers': 'Follower Creator', 'feed_likes': 'Like Totali',
            'days_old': 'Giorni Vita', 'creator_description': 'Bio Creator'
        }
        export_table = export_table.rename(columns=rename_map)
        
        # 5. Salvataggio
        csv_path = os.path.join(OUTPUT_FOLDER, "8c_tabella_new_viral_feeds.csv")
        export_table.to_csv(csv_path, index=False)
        print(f"    ✅ Salvata tabella con {len(export_table)} feed in: {os.path.basename(csv_path)}")
    else:
        print("    ℹ️ Nessun feed trovato con criteri (<10gg e >100 like). Nessun CSV generato.")

        
        
    # ################################
    # 9. Analisi Viralità e Correlazione
    # ################################
    print("📈 9/9 Analisi Viralità e Correlazione...")
    
    # --- 9a. Indice di Viralità (Istogramma) ---
    print("   > Generazione 9a (Istogramma Viralità)...")
    plt.figure(figsize=(10, 6))
    
    if 'log_virality' in df.columns:
        sns.histplot(df['log_virality'], bins=40, kde=True, color='crimson')
        plt.axvline(x=0, color='black', linestyle='--', label='Parità (1 Like per Follower)')
        plt.title('9a. Distribuzione Indice di Viralità')
        plt.xlabel('Viralità (Log Scale) [>0: Il feed è più famoso dell\'autore]')
        plt.ylabel('Conteggio Feed')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.savefig(f"{OUTPUT_FOLDER}/9a_virality_histogram.png"); plt.close()
    else:
        print("     ⚠️ Salto 9a: colonna 'log_virality' mancante.")

    # --- 9b. Correlazione Popolarità vs Successo (Scatter Log-Log) ---
    print("   > Generazione 9b (Scatter Correlation & Regression)...")
    
    # Nota: Usiamo le colonne log_likes e log_followers già calcolate nella Sezione 3
    
    # Calcolo Pearson
    corr_coef, p_value = stats.pearsonr(df['log_followers'], df['log_likes'])
    strength = "Forte" if corr_coef > 0.5 else "Moderata" if corr_coef > 0.3 else "Debole"

    plt.figure(figsize=(10, 10))
    sns.scatterplot(x='log_followers', y='log_likes', data=df, alpha=0.2, s=20, color='lightgray', edgecolor=None)
    
    sns.regplot(
        x='log_followers', y='log_likes', data=df, scatter=False, 
        color='green', line_kws={"linewidth": 3},
        label=f'Trend Reale (Regressione)\nr = {corr_coef:.2f}'
    )
    
    # Parità 1:1
    min_val = min(df['log_followers'].min(), df['log_likes'].min())
    max_val = max(df['log_followers'].max(), df['log_likes'].max())
    plt.plot([min_val, max_val], [min_val, max_val], color='red', linestyle='--', linewidth=2, label='Limite Teorico (1:1)')

    stats_box = f"Correlazione (Pearson): {corr_coef:.2f}\nSignificatività: {p_value:.1e}\nInterpretazione: {strength}"
    plt.text(min_val + 0.2, max_val - 0.5, stats_box, fontsize=11, bbox=dict(facecolor='white', alpha=0.9, boxstyle='round'))

    plt.title('9b. Correlazione Popolarità Autore vs Successo Feed')
    plt.xlabel('Popolarità Autore (Log10)')
    plt.ylabel('Successo Feed (Log10)')
    plt.legend(loc='lower right')
    plt.grid(True, alpha=0.3)
    plt.xlim(min_val, max_val); plt.ylim(min_val, max_val)
    plt.savefig(f"{OUTPUT_FOLDER}/9b_correlation_real.png"); plt.close()

    # --- 9c. Analisi Comparativa Split (Massa vs Elite 99.5%) ---
    print("   > Generazione 9c (Confronto Mainstream vs Elite 99.5%)...")
    
    # 1. Preparazione Dati (Escluso bsky.app)
    df_9c = df[df['creator_handle'] != 'bsky.app'].copy()
    
    # 2. Calcolo Soglia (99.5%)
    percentile_target = 0.995
    x_col = 'creator_followers'
    y_col = 'feed_likes'
    dynamic_threshold = df_9c[x_col].quantile(percentile_target)
    
    # Split
    df_elite = df_9c[df_9c[x_col] > dynamic_threshold]
    df_mass = df_9c[df_9c[x_col] <= dynamic_threshold]
    
    # 3. Plotting
    fig, axes = plt.subplots(1, 2, figsize=(20, 9))
    comma_fmt = ticker.FuncFormatter(lambda x, p: format(int(x), ','))

    # SX: MASSA
    ax_mass = axes[0]
    if not df_mass.empty:
        corr_m, _ = stats.pearsonr(df_mass[x_col], df_mass[y_col])
        sns.scatterplot(x=x_col, y=y_col, data=df_mass, alpha=0.5, s=30, color='dodgerblue', edgecolor='black', linewidth=0.2, ax=ax_mass)
        sns.regplot(x=x_col, y=y_col, data=df_mass, scatter=False, color='green', ci=None, line_kws={"linewidth": 3}, label=f'Trend Reale (r={corr_m:.2f})', ax=ax_mass)
        
        limit_vis_m = min(df_mass[x_col].max(), df_mass[y_col].max())
        ax_mass.plot([0, limit_vis_m], [0, limit_vis_m], color='red', linestyle='--', linewidth=2, label='Parità (1:1)')
        
        stats_txt = f"Correlazione: {corr_m:.2f}\nSoglia (99.5%): {int(dynamic_threshold):,}"
        ax_mass.text(0.05, 0.95, stats_txt, transform=ax_mass.transAxes, fontsize=11, va='top', bbox=dict(facecolor='white', alpha=0.9, boxstyle='round'))
        
        ax_mass.set_title('A. MAINSTREAM (Il 99.5% della popolazione)', fontsize=14, fontweight='bold')
        ax_mass.legend(loc='lower right')
        ax_mass.set_xlim(0, df_mass[x_col].max() * 1.05)
        ax_mass.set_ylim(0, df_mass[y_col].max() * 1.05)

    # DX: ELITE
    ax_elite = axes[1]
    if not df_elite.empty:
        sns.scatterplot(x=x_col, y=y_col, data=df_elite, alpha=0.8, s=100, color='purple', edgecolor='black', ax=ax_elite)
        
        limit_vis_e = min(df_elite[x_col].max(), df_elite[y_col].max())
        ax_elite.plot([0, limit_vis_e], [0, limit_vis_e], color='red', linestyle='--', label='Parità (1:1)')
        
        for _, row in df_elite.nlargest(10, x_col).iterrows():
            ax_elite.text(row[x_col], row[y_col], f"{row['creator_handle']}\n({row[y_col]:,})", fontsize=8, ha='right', va='bottom')
            
        ax_elite.set_title(f'B. ELITE (Top 0.5% -> {int(dynamic_threshold):,} Follower)', fontsize=14, fontweight='bold')
        ax_elite.legend()
        ax_elite.set_xlim(dynamic_threshold, df_elite[x_col].max() * 1.05)
        ax_elite.set_ylim(0, df_elite[y_col].max() * 1.05)

    # Formattazione
    for ax in axes:
        ax.set_xlabel('Follower Autore', fontsize=12)
        ax.set_ylabel('Like Feed', fontsize=12)
        ax.grid(True, alpha=0.3)
        ax.get_xaxis().set_major_formatter(comma_fmt)
        ax.get_yaxis().set_major_formatter(comma_fmt)

    plt.suptitle(f"Analisi Comparativa: 99.5° Percentile (Soglia: {int(dynamic_threshold):,} Follower)", fontsize=16, y=0.96)
    fig.text(0.5, 0.02, "Nota: L'account 'bsky.app' è stato escluso per evitare distorsioni.", ha='center', fontsize=10, style='italic', color='gray')
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(f"{OUTPUT_FOLDER}/9c_comparison_split.png"); plt.close()

    print(f"\n✅ FINITO! Tutti i grafici salvati in: {os.path.abspath(OUTPUT_FOLDER)}")

if __name__ == "__main__":
    main()