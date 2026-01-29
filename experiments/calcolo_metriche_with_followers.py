# ======================================================================================
# FILE: Tesi/experiments/calcolo_metriche_with_followers.py
# --------------------------------------------------------------------------------------
# DESCRIZIONE:
#   Calcola metriche statistiche e genera grafici partendo dai dataset uniti (merged).
#
# INPUT:
#   Legge i file *_merged.jsonl.gz da: 
#   ../data_collection/data/chunk_1
#
# OUTPUT:
#   Salva report .txt e grafici .png in:
#   ./results/plots/followers/
# ======================================================================================

import gzip
import json
import os
import statistics
import matplotlib.pyplot as plt
from datetime import datetime
from tqdm import tqdm

# --- CONFIGURAZIONE PERCORSI DINAMICI ---

# 1. Cartella base dove si trova QUESTO script (Tesi/experiments)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Percorso Input (Tesi/data_collection/data/chunk_1)
#    Saliamo di uno (..) ed entriamo in data_collection
INPUT_DIR = os.path.join(BASE_DIR, '..', 'data_collection', 'data', 'chunk_1')

# 3. Percorso Output (Tesi/experiments/results/plots/followers)
OUTPUT_DIR = os.path.join(BASE_DIR, 'results', 'plots', 'followers')

# Creiamo la cartella di output se non esiste
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Definizioni file Output
OUTPUT_TXT = os.path.join(OUTPUT_DIR, 'report_statistiche_completo.txt')
IMG_ISTOGRAMMA = os.path.join(OUTPUT_DIR, 'grafico_istogramma_post.png')
IMG_BOXPLOT = os.path.join(OUTPUT_DIR, 'grafico_boxplot.png')
IMG_SCATTER = os.path.join(OUTPUT_DIR, 'grafico_scatter_fama.png')
IMG_TORTA = os.path.join(OUTPUT_DIR, 'grafico_torta_tipologia.png')

SUFFIX = '_merged.jsonl.gz'

CAP_LIMIT = 530       
HIST_LIMIT = 1200     
# ----------------------------------------

def calcola_statistiche_complete():
    # Stampa iniziale per conferma percorsi
    print("\n" + "="*60)
    print("🚀 AVVIO ANALISI METRICHE")
    print("="*60)
    print(f"📥 INPUT DIR:  {os.path.abspath(INPUT_DIR)}")
    print(f"📤 OUTPUT DIR: {os.path.abspath(OUTPUT_DIR)}")
    print("-" * 60 + "\n")

    print(f"🔍 Cerco file *{SUFFIX}...")
    
    if not os.path.exists(INPUT_DIR):
        print(f"❌ Errore critico: La cartella di input non esiste: {INPUT_DIR}")
        return

    files = [f for f in os.listdir(INPUT_DIR) if f.endswith(SUFFIX)]
    
    if not files:
        print(f"❌ Nessun file {SUFFIX} trovato in {INPUT_DIR}.")
        return

    # --- STRUTTURE DATI ---
    users_data = {} 
    user_post_counts = {}   
    user_english_counts = {} 

    all_likes = []
    all_replies = []
    all_reposts = []
    
    totale_post_dataset = 0
    totale_post_en_dataset = 0
    
    oldest_str = None
    newest_str = None
    
    print(f"📂 Trovati {len(files)} file. Analisi completa in corso...")
    print("-" * 60)

    for file_name in files:
        full_path = os.path.join(INPUT_DIR, file_name)
        file_size = os.path.getsize(full_path)
        
        try:
            with tqdm.wrapattr(open(full_path, "rb"), "read", total=file_size, unit="B", unit_scale=True, desc="Analisi") as f_raw:
                with gzip.open(f_raw, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            data = json.loads(line)
                            
                            # 1. IDENTIFICAZIONE
                            author_info = data.get('author', {})
                            did = data.get('user') or author_info.get('did')
                            
                            if not did: continue

                            totale_post_dataset += 1

                            # 2. DATI UTENTE
                            if did not in users_data:
                                users_data[did] = {
                                    'handle': author_info.get('handle', 'Unknown'),
                                    'followers': 0,
                                    'follows': 0,
                                    'has_enrich': False
                                }
                            
                            if not users_data[did]['has_enrich']:
                                enrich = data.get('author_enriched')
                                if enrich:
                                    users_data[did]['followers'] = enrich.get('followers', 0)
                                    users_data[did]['follows'] = enrich.get('follows', 0)
                                    users_data[did]['has_enrich'] = True

                            # 3. CONTEGGI POST
                            user_post_counts[did] = user_post_counts.get(did, 0) + 1

                            # 4. ENGAGEMENT
                            all_likes.append(data.get('like_count', 0))
                            all_replies.append(data.get('reply_count', 0))
                            all_reposts.append(data.get('repost_count', 0))

                            # 5. LINGUA & TEMPO
                            record = data.get('record', {})
                            if record:
                                langs = record.get('langs')
                                if langs and 'en' in langs:
                                    user_english_counts[did] = user_english_counts.get(did, 0) + 1
                                    totale_post_en_dataset += 1
                                
                                created_at = record.get('created_at')
                                if created_at:
                                    if oldest_str is None or created_at < oldest_str:
                                        oldest_str = created_at
                                    if newest_str is None or created_at > newest_str:
                                        newest_str = created_at

                        except json.JSONDecodeError:
                            continue
                        
        except Exception as e:
            print(f"\n❌ Errore leggendo {file_name}: {e}")

    # --- ELABORAZIONE DATI ---
    if not user_post_counts:
        print("\n⚠️ Nessun dato trovato.")
        return

    print("\n🧮 Calcolo statistiche finali...")

    # 1. POST
    list_posts = list(user_post_counts.values())
    num_users = len(list_posts)
    min_posts = min(list_posts)
    max_posts = max(list_posts)
    avg_posts = statistics.mean(list_posts)
    median_posts = statistics.median(list_posts)

    # 2. FOLLOWERS & CATEGORIE
    list_followers = []
    users_enriched_count = 0
    
    cat_influencer = 0 
    cat_normal = 0     
    cat_lurker = 0     

    for did, info in users_data.items():
        if info['has_enrich']:
            users_enriched_count += 1
            foll = info['followers']
            fing = info['follows']
            list_followers.append(foll)
            
            # Calcolo Ratio
            ratio = foll / fing if fing > 0 else foll
            
            # Assegnazione Categoria
            if ratio > 10: cat_influencer += 1
            elif ratio > 0.5: cat_normal += 1
            else: cat_lurker += 1

    if list_followers:
        min_foll, max_foll = min(list_followers), max(list_followers)
        avg_foll = statistics.mean(list_followers)
    else:
        min_foll, max_foll, avg_foll = 0, 0, 0

    # 3. LINGUA
    users_primary_english = 0
    for did, total in user_post_counts.items():
        en_count = user_english_counts.get(did, 0)
        if total > 0 and (en_count / total) > 0.5:
            users_primary_english += 1
    
    perc_users_en = (users_primary_english / num_users) * 100 if num_users > 0 else 0
    perc_posts_en = (totale_post_en_dataset / totale_post_dataset) * 100 if totale_post_dataset > 0 else 0

    # 4. TEMPO
    dt_start, dt_end, duration_days = None, None, 0
    if oldest_str and newest_str:
        try:
            old_clean = oldest_str.replace('Z', '+00:00')
            new_clean = newest_str.replace('Z', '+00:00')
            dt_start = datetime.fromisoformat(old_clean)
            dt_end = datetime.fromisoformat(new_clean)
            duration_days = (dt_end - dt_start).days
        except: pass

    # 5. OUTLIERS
    upper_bound = 0
    num_outliers = 0
    perc_outliers = 0
    if num_users > 1:
        quantiles = statistics.quantiles(list_posts, n=4)
        q1, q3 = quantiles[0], quantiles[2]
        iqr = q3 - q1
        upper_bound = q3 + (1.5 * iqr)
        num_outliers = sum(1 for x in list_posts if x > upper_bound)
        perc_outliers = (num_outliers / num_users) * 100

    # 6. ENGAGEMENT STATS
    def get_stats(lst):
        if not lst: return 0, 0, 0
        return min(lst), max(lst), statistics.mean(lst)

    min_lk, max_lk, avg_lk = get_stats(all_likes)
    min_rp, max_rp, avg_rp = get_stats(all_replies)
    min_rr, max_rr, avg_rr = get_stats(all_reposts)

    # --- GRAFICI ---
    print("🎨 Generazione grafici...")
    try:
        # Istogramma
        plt.figure(figsize=(10, 6))
        counts_hist = [x if x < HIST_LIMIT else HIST_LIMIT for x in list_posts]
        plt.hist(counts_hist, bins=50, color='#3498db', edgecolor='black', alpha=0.7)
        plt.title('Distribuzione Post per Utente')
        plt.axvline(upper_bound, color='orange', linestyle='--', label='Outlier Bound')
        plt.legend()
        plt.savefig(IMG_ISTOGRAMMA)
        plt.close()

        # Boxplot
        plt.figure(figsize=(10, 5))
        plt.boxplot(list_posts, vert=False, patch_artist=True, boxprops=dict(facecolor='#2ecc71'))
        plt.title('Boxplot Attività')
        plt.savefig(IMG_BOXPLOT)
        plt.close()

        # Scatter
        if list_followers:
            plt.figure(figsize=(10, 6))
            x, y = [], []
            for did, info in users_data.items():
                if info['has_enrich']:
                    x.append(user_post_counts[did])
                    y.append(info['followers'])
            plt.scatter(x, y, alpha=0.4, c='#9b59b6', s=10)
            plt.xscale('log'); plt.yscale('log')
            plt.title('Attività vs Followers (Log)')
            plt.savefig(IMG_SCATTER)
            plt.close()

        # Torta
        if users_enriched_count > 0:
            plt.figure(figsize=(7, 7))
            plt.pie([cat_influencer, cat_normal, cat_lurker], 
                    labels=['Influencer', 'Normal', 'Lurker'], autopct='%1.1f%%', 
                    colors=['#f1c40f', '#2ecc71', '#95a5a6'])
            plt.title('Tipologia Utenti (Ratio)')
            plt.savefig(IMG_TORTA)
            plt.close()

    except Exception as e:
        print(f"❌ Errore Grafici: {e}")

    # --- SCRITTURA REPORT ---
    lines = []
    lines.append("="*60)
    lines.append(f"📊 REPORT STATISTICHE COMPLETO")
    lines.append("="*60 + "\n")
    
    lines.append(f"DATI GENERALI")
    lines.append(f"- Utenti unici analizzati:     {num_users}")
    lines.append(f"- Post totali analizzati:      {totale_post_dataset}")
    
    lines.append(f"\nFINESTRA TEMPORALE")
    if dt_start:
        lines.append(f"- Inizio:   {dt_start}")
        lines.append(f"- Fine:     {dt_end}")
        lines.append(f"- Durata:   {duration_days} giorni")
    else:
        lines.append("- Dati temporali non disponibili.")

    lines.append(f"\nANALISI LIMITE CRAWLER ({CAP_LIMIT} Post)")
    users_at_cap = list_posts.count(CAP_LIMIT)
    perc_at_cap = (users_at_cap / num_users) * 100
    lines.append(f"- Utenti con esattamente {CAP_LIMIT} post:  {users_at_cap} ({perc_at_cap:.2f}%)")

    lines.append(f"\nANALISI SUPER-UTENTI (Soglia Grafico {HIST_LIMIT})")
    users_over_hist = sum(1 for x in list_posts if x >= HIST_LIMIT)
    lines.append(f"- Utenti con >={HIST_LIMIT} post:            {users_over_hist}")

    lines.append(f"\nANALISI OUTLIERS (Attività anomala)")
    lines.append(f"(Metodo: Tukey Boxplot, Q3 + 1.5*IQR)")
    lines.append(f"- Soglia 'Normale' (Upper Bound):  {upper_bound:.2f} post")
    lines.append(f"- Utenti Outliers (> soglia):      {num_outliers} ({perc_outliers:.2f}%)")

    lines.append(f"\nFREQUENZA PUBBLICAZIONE")
    lines.append(f"- Min/Max post per utente:     {min_posts} / {max_posts}")
    lines.append(f"- Media post per utente:       {avg_posts:.2f}")
    lines.append(f"- Mediana post per utente:     {median_posts:.1f}")

    lines.append(f"\nMETRICHE ENGAGEMENT (Per singolo post)")
    lines.append(f"{'Metrica':<15} {'Min':<8} {'Max':<10} {'Media':<10}")
    lines.append("-" * 45)
    lines.append(f"{'❤️ Likes':<15} {min_lk:<8} {max_lk:<10} {avg_lk:<10.2f}")
    lines.append(f"{'💬 Commenti':<15} {min_rp:<8} {max_rp:<10} {avg_rp:<10.2f}")
    lines.append(f"{'🔁 Repost':<15} {min_rr:<8} {max_rr:<10} {avg_rr:<10.2f}")

    lines.append(f"\nMETRICHE UTENTE (Followers)")
    if users_enriched_count > 0:
        lines.append(f"(Dati basati su {users_enriched_count} utenti uniti)")
        lines.append(f"- Min Followers:          {min_foll}")
        lines.append(f"- Max Followers:          {max_foll}")
        lines.append(f"- Media Followers:        {avg_foll:.2f}")
        
        # --- QUI LA SPIEGAZIONE DEL RATIO ---
        lines.append(f"\n  [LEGENDA RATIO (Followers / Following)]")
        lines.append(f"  * Influencer (>10):   Broadcaster, VIP, Account virali.")
        lines.append(f"  * Normali (0.5-10):   Utenti 'social' con relazioni reciproche.")
        lines.append(f"  * Lurker (<0.5):      Osservatori passivi o Bot (seguono molti, seguiti da pochi).")
        
        lines.append(f"\n- Influencer (>10 ratio): {cat_influencer}")
        lines.append(f"- Normali (0.5-10 ratio): {cat_normal}")
        lines.append(f"- Lurker (<0.5 ratio):    {cat_lurker}")
    else:
        lines.append("⚠️ Nessun dato followers trovato.")

    lines.append(f"\nLINGUA (Inglese)")
    lines.append(f"- % Utenti prevalentemente EN: {perc_users_en:.2f}%")
    lines.append(f"- % Totale Post in EN:         {perc_posts_en:.2f}%")

    lines.append(f"\n🏆 TOP 5 UTENTI PIÙ ATTIVI")
    lines.append(f"{'#':<3} {'HANDLE':<45} {'POST':>10}")
    lines.append("-" * 60)
    
    top_active = sorted(user_post_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    for rank, (did, count) in enumerate(top_active, 1):
        handle = users_data[did]['handle']
        lines.append(f"{rank:<3} {handle:<45} {count:>10}")

    with open(OUTPUT_TXT, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
        
    print("\n" + "\n".join(lines))
    print("\n" + "="*60)
    print("✅ ANALISI COMPLETATA")
    print("="*60)
    print(f"📥 Input caricato da: {os.path.abspath(INPUT_DIR)}")
    print(f"📤 Report e grafici salvati in: {os.path.abspath(OUTPUT_DIR)}")
    print("="*60 + "\n")

if __name__ == "__main__":
    calcola_statistiche_complete()