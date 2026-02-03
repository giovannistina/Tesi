# Tesi / experiments / replica_paper / analisi_campione_30k / overview_dataset.py

import json
import gzip
import os
import sys
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import resource 
from collections import Counter, defaultdict
from datetime import datetime

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DATASET = os.path.abspath(os.path.join(CURRENT_DIR, "../../../data_collection/data/dataset_definitivo_6mesi.jsonl.gz"))

OUTPUT_DIR = os.path.join(CURRENT_DIR, "results_overview")
OUTPUT_REPORT = os.path.join(OUTPUT_DIR, "report_overview.txt")

API_LIMIT = 4320
SATURATION_THRESHOLD = 4300 # Soglia per considerare un utente "Saturo"
LOG_INTERVAL_SECONDS = 10 

def get_ram_usage():
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return f"{usage / 1024:.0f} MB"
    except:
        return "N/A"

def main():
    print("--- OVERVIEW DATASET (BIAS ANALYSIS TIMELINE) ---")
    print(f"🕒 Inizio: {datetime.now().strftime('%H:%M:%S')}")
    
    if not os.path.exists(INPUT_DATASET):
        print(f"❌ Errore: File non trovato: {INPUT_DATASET}")
        sys.exit(1)

    file_size_bytes = os.path.getsize(INPUT_DATASET)
    print(f"📦 Dimensione File: {file_size_bytes / (1024*1024):.2f} MB")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- STRUTTURE DATI ---
    user_metrics = {}      
    daily_volume = Counter()       # Totale Post per Giorno
    
    # IMPORTANTE: Teniamo traccia di CHI ha postato in ogni data
    # daily_users: { '2024-01-01': set('did1', 'did2'...), ... }
    # Usiamo un defaultdict(set) che è efficiente
    daily_users = defaultdict(set)
    
    langs_volume = Counter() 
    total_posts_read = 0
    
    print(f"📥 Avvio lettura stream...")
    start_time = time.time()
    last_print_time = start_time

    try:
        with open(INPUT_DATASET, 'rb') as raw_f:
            with gzip.open(raw_f, 'rt', encoding='utf-8') as f:
                
                for line in f:
                    try:
                        data = json.loads(line)
                        total_posts_read += 1
                        
                        # --- ESTRAZIONE DATI ---
                        did = None
                        if 'author' in data and 'did' in data['author']:
                            did = data['author']['did']
                        elif 'did' in data:
                            did = data['did']
                        
                        likes = data.get('like_count', 0)
                        
                        date_str = None
                        if 'created_at' in data:
                            date_str = data['created_at'][:10]
                        elif 'record' in data and 'created_at' in data['record']:
                            date_str = data['record']['created_at'][:10]
                            
                        langs = []
                        if 'record' in data and 'langs' in data['record']:
                            langs = data['record']['langs']
                        elif 'langs' in data:
                            langs = data['langs']

                        # --- AGGIORNAMENTO ---
                        if did:
                            if did not in user_metrics:
                                user_metrics[did] = {'posts': 0, 'likes': 0}
                            user_metrics[did]['posts'] += 1
                            user_metrics[did]['likes'] += likes
                            
                            # Memorizziamo il DID nel giorno specifico
                            if date_str:
                                daily_users[date_str].add(did)
                        
                        if date_str:
                            daily_volume[date_str] += 1
                            
                        if langs:
                            for l in langs:
                                langs_volume[l] += 1

                        # --- LOGGING ---
                        current_time = time.time()
                        if current_time - last_print_time > LOG_INTERVAL_SECONDS:
                            current_bytes = raw_f.tell()
                            perc = (current_bytes / file_size_bytes) * 100
                            unique_users = len(user_metrics)
                            speed = total_posts_read / (current_time - start_time)
                            
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                                  f"Progress: {perc:.1f}% | "
                                  f"Post: {total_posts_read:,} | "
                                  f"Utenti: {unique_users:,} | "
                                  f"RAM: {get_ram_usage()}", flush=True)
                            
                            last_print_time = current_time

                    except Exception:
                        continue
                    
    except Exception as e:
        print(f"\n❌ Errore critico: {e}")
        sys.exit(1)

    print(f"\n✅ Lettura completata. Analisi Bias Temporale...")
    
    df = pd.DataFrame.from_dict(user_metrics, orient='index')
    df.fillna(0, inplace=True)
    total_users = len(df)
    
    # IDENTIFICHIAMO GLI UTENTI SATURI (POWER USERS)
    # Creiamo un SET per accesso veloce O(1)
    saturated_dids = set(df[df['posts'] >= SATURATION_THRESHOLD].index)
    print(f"⚠️ Utenti Saturi (Truncated History): {len(saturated_dids)}")

    print("📊 Generazione Grafici...")

    # =========================================================================
    # 1. ISTOGRAMMA POST
    # =========================================================================
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='posts', bins=50, color='royalblue', kde=False)
    plt.axvline(x=API_LIMIT, color='red', linestyle='--', label=f'Limite API ({API_LIMIT})')
    plt.yscale('log')
    plt.title(f"Distribuzione Post per Utente (Tot: {total_users})")
    plt.xlabel("Numero Post")
    plt.ylabel("Numero Utenti (Log)")
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR, "1_distribuzione_post.png"))
    plt.close()

    # =========================================================================
    # 2. BOXPLOT
    # =========================================================================
    plt.figure(figsize=(12, 4))
    sns.boxplot(x=df['posts'], color='orange', flierprops={"marker": "x", "markersize": 2})
    plt.axvline(x=API_LIMIT, color='red', linestyle='--')
    plt.title("Boxplot Attività Utenti")
    plt.xlabel("Numero Post")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "2_boxplot_post.png"))
    plt.close()

    # =========================================================================
    # 3. TIMELINE SPLIT (BIAS ANALYSIS)
    # =========================================================================
    # Qui avviene la magia: separiamo i conteggi per giorno
    timeline_data = []
    all_dates = sorted(daily_users.keys())
    
    for d in all_dates:
        if not d.startswith("202"): continue # Skip date errate
        
        users_today = daily_users[d] # Set di DID attivi oggi
        
        # Intersezione: Quanti sono saturi? Quanti no?
        count_saturated = 0
        count_normal = 0
        
        for did in users_today:
            if did in saturated_dids:
                count_saturated += 1
            else:
                count_normal += 1
        
        timeline_data.append({
            'date': d,
            'active_saturated': count_saturated,
            'active_normal': count_normal,
            'total_volume': daily_volume[d]
        })
    
    df_time = pd.DataFrame(timeline_data)
    df_time['date'] = pd.to_datetime(df_time['date'])
    df_time.sort_values('date', inplace=True)
    
    # Medie mobili per pulizia visiva
    df_time['norm_ma7'] = df_time['active_normal'].rolling(7).mean()
    df_time['sat_ma7'] = df_time['active_saturated'].rolling(7).mean()

    if not df_time.empty:
        fig, ax1 = plt.subplots(figsize=(15, 8))

        # SFONDO: Volume Totale
        ax1.bar(df_time['date'], df_time['total_volume'], color='lightgray', alpha=0.5, label='Volume Post Totale')
        ax1.set_ylabel('Volume Post (Totale)', color='gray')
        ax1.tick_params(axis='y', labelcolor='gray')

        # PRIMO PIANO: Linee Utenti Attivi separate
        ax2 = ax1.twinx()
        
        # Linea BLU: Utenti Normali (dovrebbe essere stabile)
        ax2.plot(df_time['date'], df_time['norm_ma7'], color='#2ecc71', linewidth=2, label='Utenti Standard (Storia Completa)')
        
        # Linea ROSSA: Utenti Saturi (dovrebbe crollare nel passato)
        ax2.plot(df_time['date'], df_time['sat_ma7'], color='#e74c3c', linewidth=2, linestyle='--', label='Power Users (Storia Tronca)')
        
        ax2.set_ylabel('Utenti Attivi Giornalieri (DAU)', color='black', fontweight='bold')
        
        # Aggiungiamo un'annotazione per spiegare il bias
        max_sat = df_time['sat_ma7'].max()
        mid_date = df_time.iloc[len(df_time)//2]['date']
        
        # Annotazione visuale sul grafico
        ax2.text(mid_date, max_sat, "↓ Data Cut-off effect", color='#e74c3c', ha='center', fontweight='bold')

        plt.title("Analisi Bias Temporale: Effetto del limite API sui dati storici")
        
        # Legenda Combinata
        lines_1, labels_1 = ax1.get_legend_handles_labels()
        lines_2, labels_2 = ax2.get_legend_handles_labels()
        plt.legend(lines_1 + lines_2, labels_1 + labels_2, loc='upper left')
        
        ax1.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "3_timeline_bias_check.png"))
        plt.close()

    # =========================================================================
    # 4. SCATTER PLOT
    # =========================================================================
    plt.figure(figsize=(8, 8))
    plot_data = df.sample(10000) if len(df) > 10000 else df
    plt.scatter(plot_data['posts'], plot_data['likes'], alpha=0.3, s=10, c='purple')
    plt.xscale('log'); plt.yscale('log')
    plt.xlabel("Post Pubblicati")
    plt.ylabel("Like Totali Ricevuti")
    plt.title("Attività vs Popolarità")
    plt.grid(True, which="both", alpha=0.2)
    plt.savefig(os.path.join(OUTPUT_DIR, "4_scatter.png"))
    plt.close()

    # =========================================================================
    # 5. LINGUE
    # =========================================================================
    top_langs = langs_volume.most_common(10)
    if top_langs:
        langs, l_counts = zip(*top_langs)
        plt.figure(figsize=(10, 5))
        plt.bar(langs, l_counts, color='teal')
        plt.title("Top 10 Lingue")
        plt.savefig(os.path.join(OUTPUT_DIR, "5_languages.png"))
        plt.close()

    # --- REPORT FINALE ---
    stats = df['posts'].describe()
    saturated = len(saturated_dids)
    
    top_10_users = df.nlargest(10, 'posts')
    top_10_str = top_10_users[['posts', 'likes']].to_string()
    
    report = f"""
    === REPORT ANALISI DATASET (CON BIAS CHECK) ===
    Data Analisi: {datetime.now()}
    
    1. VOLUMETRIA
       - Post Totali Letti: {total_posts_read:,}
       - Utenti Unici: {total_users:,}
       - Like Totali Distribuiti: {df['likes'].sum():,}
    
    2. ANALISI LIMITE API & BIAS
       - Utenti Totali: {total_users}
       - Utenti Saturi (Power Users >= {SATURATION_THRESHOLD}): {saturated}
       - Percentuale Saturi: {saturated/total_users*100:.2f}%
       
       NOTA SUL GRAFICO 3 (Timeline):
       Osserva la linea ROSSA tratteggiata. Se cala drasticamente andando indietro 
       nel tempo, mentre la linea VERDE rimane stabile, è la conferma che il 
       calo di volume storico è dovuto esclusivamente al limite di download 
       dei Power Users.
    
    3. STATISTICHE ATTIVITÀ
       - Media: {stats['mean']:.2f}
       - Mediana: {stats['50%']:.0f}
       - Max: {stats['max']:.0f}
       
    4. TOP 10 UTENTI PIÙ ATTIVI
    {top_10_str}

    5. TOP LINGUE
       {top_langs}
       
    Grafici salvati in: {OUTPUT_DIR}
    """
    
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
        
    print(report)
    print("✅ Tutto completato.")

if __name__ == "__main__":
    main()