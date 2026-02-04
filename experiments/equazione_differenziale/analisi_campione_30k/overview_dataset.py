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
OUTPUT_DIR = os.path.join(CURRENT_DIR)
OUTPUT_REPORT = os.path.join(OUTPUT_DIR, "report_overview.txt")

API_LIMIT = 4320
SATURATION_THRESHOLD = 4300 
LOG_INTERVAL_SECONDS = 10 

def get_ram_usage():
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return f"{usage / 1024:.0f} MB"
    except:
        return "N/A"

def main():
    print("--- OVERVIEW DATASET (FULL BIAS CHECK) ---")
    print(f"🕒 Inizio: {datetime.now().strftime('%H:%M:%S')}")
    
    if not os.path.exists(INPUT_DATASET):
        print(f"❌ Errore: File non trovato: {INPUT_DATASET}")
        sys.exit(1)

    file_size_bytes = os.path.getsize(INPUT_DATASET)
    print(f"📦 Dimensione File: {file_size_bytes / (1024*1024):.2f} MB")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- STRUTTURE DATI ---
    # user_metrics: Totali per utente (per identificare i saturi alla fine)
    user_metrics = {}      
    
    # daily_counts: { '2024-01-01': { 'did_A': 5, 'did_B': 10 } }
    # Serve per calcolare a posteriori il volume separato tra saturi e normali
    daily_counts = defaultdict(lambda: defaultdict(int))
    
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
                        
                        # Estrazione Dati Base
                        did = data.get('did') or data.get('author', {}).get('did')
                        likes = data.get('like_count', 0)
                        created_at = data.get('created_at') or data.get('record', {}).get('created_at', "")
                        date_str = created_at[:10]
                        langs = data.get('langs') or data.get('record', {}).get('langs', [])

                        # Aggiornamento Metriche
                        if did:
                            # 1. Totali Utente
                            if did not in user_metrics:
                                user_metrics[did] = {'posts': 0, 'likes': 0}
                            user_metrics[did]['posts'] += 1
                            user_metrics[did]['likes'] += likes
                            
                            # 2. Dettaglio Giornaliero (Fondamentale per il grafico 3)
                            if date_str:
                                daily_counts[date_str][did] += 1
                        
                        if isinstance(langs, list):
                            for l in langs:
                                langs_volume[l] += 1

                        # Logging
                        current_time = time.time()
                        if current_time - last_print_time > LOG_INTERVAL_SECONDS:
                            current_bytes = raw_f.tell()
                            perc = (current_bytes / file_size_bytes) * 100
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Progress: {perc:.1f}% | Post: {total_posts_read:,} | RAM: {get_ram_usage()}", flush=True)
                            last_print_time = current_time

                    except Exception:
                        continue
                        
    except Exception as e:
        print(f"\n❌ Errore critico: {e}")
        sys.exit(1)

    print(f"\n✅ Lettura completata. Analisi Struttura...")
    df = pd.DataFrame.from_dict(user_metrics, orient='index').fillna(0)
    total_users = len(df)
    
    # Identificazione Power Users
    saturated_dids = set(df[df['posts'] >= SATURATION_THRESHOLD].index)
    print(f"⚠️ Utenti Saturi identificati: {len(saturated_dids)}")

    # --- GENERAZIONE GRAFICI ---
    print("📊 Generazione Grafici...")

    # 1. ISTOGRAMMA
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df, x='posts', bins=50, color='royalblue', kde=False)
    plt.axvline(x=API_LIMIT, color='red', linestyle='--', label=f'Limite API ({API_LIMIT})')
    plt.yscale('log')
    plt.title(f"Distribuzione Post per Utente (Tot: {total_users})")
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR, "1_distribuzione_post.png"))
    plt.close()

    # 2. BOXPLOT
    plt.figure(figsize=(12, 5))
    sns.boxplot(x=df['posts'], color='orange', flierprops={"marker": "x", "markersize": 2})
    plt.axvline(x=API_LIMIT, color='red', linestyle='--', linewidth=2)
    plt.text(API_LIMIT + 50, -0.35, f'Soglia API ({API_LIMIT})', color='red', fontweight='bold')
    plt.title("Boxplot Attività Utenti")
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, "2_boxplot_post.png"))
    plt.close()

    # =========================================================================
    # 3. TIMELINE BIAS CHECK (AVANZATO)
    # =========================================================================
    print("   ↳ Elaborazione Timeline Bias...")
    
    # A. Calcoliamo la "Data di Nascita nel Dataset" per i Power User (Coverage)
    # Serve per la linea ROSSA (Disponibilità Dati)
    pu_first_date = {}
    for d, user_counts in daily_counts.items():
        if not d.startswith("202"): continue
        for did in user_counts:
            if did in saturated_dids:
                if did not in pu_first_date or d < pu_first_date[did]:
                    pu_first_date[did] = d

    timeline_data = []
    all_dates = sorted([d for d in daily_counts.keys() if d.startswith("202")])
    
    for d in all_dates:
        day_users = daily_counts[d]
        
        # 1. Volume totale del giorno (Barre Grigie)
        vol_total = sum(day_users.values())
        
        # 2. Volume generato dai NON SATURI (Linea Verde)
        # Sommiamo i post fatti oggi solo dai DID che NON sono nel set saturated_dids
        vol_normal_users = sum(cnt for did, cnt in day_users.items() if did not in saturated_dids)
        
        # 3. Copertura Power Users (Linea Rossa)
        # Quanti Power User hanno dati che coprono questa data (data <= d)?
        pu_coverage = sum(1 for first_d in pu_first_date.values() if first_d <= d)
        
        timeline_data.append({
            'date': pd.to_datetime(d),
            'total_volume': vol_total,
            'normal_users_volume': vol_normal_users,
            'power_users_coverage': pu_coverage
        })
    
    df_time = pd.DataFrame(timeline_data).sort_values('date')

    if not df_time.empty:
        fig, ax1 = plt.subplots(figsize=(15, 8))
        
        # ASSE SX: VOLUMI (Barre e Linea Verde)
        # Barre Grigie: Volume Totale
        ax1.bar(df_time['date'], df_time['total_volume'], color='lightgray', alpha=0.6, label='Volume Totale (Tutti)')
        
        # Linea Verde: Volume Utenti Normali (Control Group)
        # Usiamo rolling(3) per smussare leggermente i picchi giornalieri
        ax1.plot(df_time['date'], df_time['normal_users_volume'].rolling(3, min_periods=1).mean(), 
                 color='green', linewidth=2.5, label='Volume Post: Utenti Non-Saturi')
        
        ax1.set_ylabel('Volume Post Giornaliero', color='black', fontweight='bold')
        ax1.tick_params(axis='y', labelcolor='black')
        
        # ASSE DX: COPERTURA POWER USER (Linea Rossa)
        ax2 = ax1.twinx()
        ax2.plot(df_time['date'], df_time['power_users_coverage'], 
                 color='#e74c3c', linewidth=2, linestyle='--', label='Disponibilità Dati Power Users')
        
        ax2.set_ylabel('N. Power Users Presenti nel Dataset', color='#e74c3c', fontweight='bold')
        ax2.tick_params(axis='y', labelcolor='#e74c3c')
        
        plt.title("Analisi Bias: Confronto Volume Utenti Normali vs Disponibilità Dati Power Users")
        
        # Legenda Unica
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
        
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_DIR, "3_timeline_bias_check.png"))
        plt.close()

    # 4. SCATTER PLOT
    plt.figure(figsize=(9, 9))
    plot_data = df.sample(min(15000, len(df)))
    plt.scatter(plot_data['posts'], plot_data['likes'], alpha=0.2, s=8, c='purple', label='Utenti')
    clean_df = df[(df['posts'] > 0) & (df['likes'] > 0)]
    if not clean_df.empty:
        log_x, log_y = np.log10(clean_df['posts']), np.log10(clean_df['likes'])
        m, q = np.polyfit(log_x, log_y, 1)
        x_range = np.logspace(np.log10(clean_df['posts'].min()), np.log10(clean_df['posts'].max()), 100)
        plt.plot(x_range, 10**(m * np.log10(x_range) + q), color='darkorange', linewidth=2, label=f'Trend (Slope: {m:.2f})')
    plt.xscale('log'); plt.yscale('log')
    plt.xlabel("Post (Log)"); plt.ylabel("Like (Log)")
    plt.title("Attività vs Popolarità")
    plt.legend(); plt.grid(True, which="both", alpha=0.15)
    plt.savefig(os.path.join(OUTPUT_DIR, "4_scatter.png"))
    plt.close()

    # 5. LINGUE
    top_langs = langs_volume.most_common(10)
    if top_langs:
        langs, l_counts = zip(*top_langs)
        plt.figure(figsize=(10, 5))
        plt.bar(langs, l_counts, color='teal')
        plt.title("Top 10 Lingue")
        plt.savefig(os.path.join(OUTPUT_DIR, "5_languages.png"))
        plt.close()

    # REPORT
    stats = df['posts'].describe()
    report = f"Post Totali: {total_posts_read:,}\nUtenti: {total_users:,}\nSaturi: {len(saturated_dids)} ({len(saturated_dids)/total_users*100:.2f}%)\nMedia Post: {stats['mean']:.2f}\nTop 10 Lingue: {top_langs}"
    with open(OUTPUT_REPORT, "w") as f: f.write(report)
    print(report)

if __name__ == "__main__":
    main()