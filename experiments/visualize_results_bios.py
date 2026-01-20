# .../Tesi/experiments/visualize_results_bios.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import datetime
import pytz

# --- CONFIGURAZIONE ---
INPUT_FILE = 'results/feed_stats/bluesky_feed_census_classified.csv'
OUTPUT_DIR = 'results/plots/ai_vs_human' 
MIN_CONFIDENCE_THRESHOLD = 0.10

# Definiamo le frasi usate (solo per scriverle nel report)
PROTOTYPES_USED = {
    "Automated Bot": "This is an automated bot account posting updates via script, feed generator or algorithm.",
    "Professional/Dev": "I am a software engineer, developer, official organization, news outlet or project maintainer.",
    "Amateur User": "I am a private person sharing my personal interests, hobbies, life, thoughts and opinions."
}

# Percorsi script
SCRIPT_CLASSIFICATION = "/experiments/classify_creators_semantic.py"
SCRIPT_VISUALIZATION = "/experiments/visualize_results_bios.py"

def get_rome_time():
    """Restituisce l'orario corrente formattato con fuso orario Roma"""
    rome_tz = pytz.timezone('Europe/Rome')
    return datetime.datetime.now(rome_tz).strftime("%d/%m/%Y alle ore %H:%M:%S")

def main():
    print("--- INIZIO VISUALIZZAZIONE RISULTATI E CALCOLO METRICHE ---")
    
    # Crea la cartella di output
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Caricamento Dati
    if not os.path.exists(INPUT_FILE):
        print(f"Errore: File {INPUT_FILE} non trovato.")
        return

    df = pd.read_csv(INPUT_FILE)
    
    # Filtriamo via i "Non-English"
    df_clean = df[df['predicted_category'] != 'Unknown/Non-English'].copy()
    
    # --- CREAZIONE DATASET ---
    # Dataset A: Creator Univoci (per demografia e follower)
    df_unique_creators = df_clean.drop_duplicates(subset='creator_did').copy()
    
    # Dataset B: Produttività (conteggio feed per creator)
    feed_counts = df_clean['creator_did'].value_counts().reset_index()
    feed_counts.columns = ['creator_did', 'feed_count']
    productivity_df = df_unique_creators.merge(feed_counts, on='creator_did', how='left')

    print(f"Dati caricati. Creator Univoci: {len(df_unique_creators)}")

    # --- GENERAZIONE GRAFICI ---
    sns.set_theme(style="whitegrid")

    # Grafico 1: Distribuzione
    plt.figure(figsize=(10, 6))
    order = df_unique_creators['predicted_category'].value_counts().index
    ax = sns.countplot(data=df_unique_creators, x='predicted_category', order=order, hue='predicted_category', palette='viridis', legend=False)
    for container in ax.containers: ax.bar_label(container)
    plt.title('Demografia: Distribuzione dei Creator Unici (Bio Inglesi)', fontsize=15)
    plt.ylabel('Numero di Utenti Unici')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/1_distribution_creators_unique.png", dpi=300)
    plt.close()

    # Grafico 2: Confidenza
    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df_unique_creators, x='predicted_category', y='confidence', hue='predicted_category', palette='coolwarm', legend=False)
    plt.axhline(y=MIN_CONFIDENCE_THRESHOLD, color='r', linestyle='--', label=f'Soglia ({MIN_CONFIDENCE_THRESHOLD})')
    plt.title('Affidabilità Classificazione per Utente Unico', fontsize=15)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/2_confidence_analysis_unique.png", dpi=300)
    plt.close()

    # Grafico 3: Follower
    plt.figure(figsize=(10, 6))
    df_fol = df_unique_creators[df_unique_creators['creator_followers'] > 0].copy()
    sns.boxplot(data=df_fol, x='predicted_category', y='creator_followers', hue='predicted_category', palette='magma', legend=False)
    plt.yscale('log')
    plt.title('Popolarità: Distribuzione Follower (Scala Log)', fontsize=15)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/3_followers_by_creator_unique.png", dpi=300)
    plt.close()

    # --- MODIFICA QUI: Grafico 4 (Scatterplot/Stripplot) ---
    plt.figure(figsize=(10, 6))
    # Usiamo stripplot: è uno scatterplot ottimizzato per categorie.
    # jitter=True sparpaglia i punti per vedere la densità.
    # alpha=0.5 li rende semitrasparenti.
    sns.stripplot(
        data=productivity_df, 
        x='predicted_category', 
        y='feed_count', 
        hue='predicted_category', 
        palette='rocket', 
        legend=False,
        jitter=0.25, 
        alpha=0.6,
        size=4 # Dimensione dei punti
    )
    plt.yscale('log')
    plt.title('Produttività: Quanti feed crea un singolo utente? (Scatter View)', fontsize=15)
    plt.ylabel('Numero di Feed Creati (Scala Log)')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/4_productivity_per_creator_scatter.png", dpi=300)
    plt.close()

    print("Grafici generati e salvati.")

    # --- CALCOLO E SALVATAGGIO METRICHE SU FILE ---
    metrics_file = os.path.join(OUTPUT_DIR, "detailed_metrics.txt")
    
    current_time_str = get_rome_time()

    with open(metrics_file, "w") as f:
        # Intestazione con DATA E ORA
        f.write("========================================================\n")
        f.write(f"REPORT ANALISI CREATOR: AI vs HUMAN\n")
        f.write(f"Eseguito il: {current_time_str} (Rome Time)\n")
        f.write("========================================================\n\n")

        # 0. METODOLOGIA E RIFERIMENTI
        f.write("0. RIFERIMENTI METODOLOGICI\n")
        f.write(f"Script di classificazione: {SCRIPT_CLASSIFICATION}\n")
        f.write(f"Script di visualizzazione: {SCRIPT_VISUALIZATION}\n")
        f.write("\nAnchor Sentences (Prototipi) utilizzate per il clustering S-BERT:\n")
        for category, sentence in PROTOTYPES_USED.items():
            f.write(f"   - {category}: \"{sentence}\"\n")
        f.write("-" * 40 + "\n\n")

        # 1. INFO GENERALI
        f.write("1. INFORMAZIONI GENERALI DEL DATASET\n")
        f.write(f"Totale Feed nel file originale: {len(df)}\n")
        f.write(f"Totale Feed analizzabili (Inglese): {len(df_clean)}\n")
        f.write(f"Totale Creator Univoci (Inglese): {len(df_unique_creators)}\n")
        f.write("-" * 40 + "\n\n")

        # 2. DISTRIBUZIONE DEMOGRAFICA
        f.write("2. DISTRIBUZIONE CATEGORIE (Demografia)\n")
        counts = df_unique_creators['predicted_category'].value_counts()
        percs = df_unique_creators['predicted_category'].value_counts(normalize=True) * 100
        dist_df = pd.DataFrame({'Conteggio': counts, 'Percentuale %': percs.round(2)})
        f.write(dist_df.to_string())
        f.write("\n\n")

        # 3. ANALISI POPOLARITÀ (Followers)
        f.write("3. ANALISI POPOLARITÀ (Followers per Creator)\n")
        f.write("Nota: La mediana è più rappresentativa della media in presenza di outlier.\n")
        fol_stats = df_unique_creators.groupby('predicted_category')['creator_followers'].agg(
            ['count', 'mean', 'median', 'max', 'sum']
        ).round(1)
        fol_stats.columns = ['N. Utenti', 'Media Follower', 'Mediana Follower', 'Max Follower', 'Reach Totale']
        f.write(fol_stats.to_string())
        f.write("\n\n")

        # 4. ANALISI PRODUTTIVITÀ (Feed per Creator)
        f.write("4. ANALISI PRODUTTIVITÀ (Numero di Feed creati per utente)\n")
        prod_stats = productivity_df.groupby('predicted_category')['feed_count'].agg(
            ['mean', 'median', 'max']
        ).round(2)
        prod_stats.columns = ['Media Feed/Utente', 'Mediana Feed/Utente', 'Record Feed (Max)']
        f.write(prod_stats.to_string())
        f.write("\n\n")

        # 5. ANALISI AFFIDABILITÀ MODELLO (Confidence)
        f.write("5. QUALITÀ DELLA CLASSIFICAZIONE (Confidence Score)\n")
        conf_stats = df_unique_creators.groupby('predicted_category')['confidence'].agg(
            ['mean', 'min', 'max']
        ).round(3)
        f.write(conf_stats.to_string())
        f.write("\n\n")

        # 6. CONTROLLO AMBIGUITÀ
        ambiguous = df_unique_creators[df_unique_creators['confidence'] < MIN_CONFIDENCE_THRESHOLD]
        f.write("6. CONTROLLO QUALITÀ (Casi Ambigui)\n")
        f.write(f"Soglia di confidenza minima impostata: {MIN_CONFIDENCE_THRESHOLD}\n")
        f.write(f"Utenti sotto soglia (incerti): {len(ambiguous)} su {len(df_unique_creators)}\n")
        f.write(f"Percentuale dati incerti: {(len(ambiguous)/len(df_unique_creators)*100):.2f}%\n")
        
        # Dettaglio ambigui per categoria
        amb_counts = ambiguous['predicted_category'].value_counts()
        f.write("\nDistribuzione dei casi incerti per categoria assegnata:\n")
        f.write(amb_counts.to_string())

    print(f"\n--- OPERAZIONE COMPLETATA ---")
    print(f"Tutti i grafici sono in: {OUTPUT_DIR}")
    print(f"Il report completo delle metriche è salvato in: {metrics_file}")

if __name__ == "__main__":
    main()