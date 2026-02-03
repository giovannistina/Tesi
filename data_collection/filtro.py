# Tesi/data_collection/filtro.py

import json
import gzip
import os
import pandas as pd
import time
from collections import defaultdict
from datetime import datetime, timezone

INPUT_PATH = os.path.expanduser('~/Tesi/data_collection/data/dataset_definitivo.jsonl.gz')
OUTPUT_CSV = os.path.expanduser('~/Tesi/data_collection/data/utenti_completi.csv')
MIN_DAYS_OLD = 180

def estrai_dati():
    print(f"🚀 AVVIO ESTRAZIONE...", flush=True)
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Errore: File non trovato in {INPUT_PATH}")
        return

    user_data = defaultdict(lambda: {'total_engagement': 0, 'post_count': 0, 'created_at': None})
    now = datetime.now(timezone.utc)
    start_time = time.time()
    last_log_time = start_time
    file_size_total = os.path.getsize(INPUT_PATH)
    total_lines = 0 # Contatore per feedback

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 📂 Analisi file: {file_size_total / (1024**3):.2f} GB", flush=True)

    with gzip.open(INPUT_PATH, 'rt', encoding='utf-8') as f:
        f_raw = f.buffer.fileobj 
        for line in f:
            total_lines += 1
            
            # --- FEEDBACK IMMEDIATO ---
            if total_lines == 1: print("✅ Lettura iniziata con successo!", flush=True)
            if total_lines == 10000: print("⚡ Prime 10.000 righe elaborate...", flush=True)

            try:
                post = json.loads(line)
                author_did = post.get('author', {}).get('did') or post.get('did')
                if not author_did: continue
                
                engagement = (post.get('like_count', 0) or 0) + \
                             (post.get('repost_count', 0) or 0) + \
                             (post.get('reply_count', 0) or 0)

                # Gestione utente
                if not user_data[author_did]['created_at']:
                    meta = post.get('author_meta', {})
                    created_at_str = meta.get('created_at') or post.get('author', {}).get('createdAt')
                    
                    if not created_at_str: continue
                    
                    dt_created = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    if (now - dt_created).days < MIN_DAYS_OLD: continue
                    user_data[author_did]['created_at'] = created_at_str

                user_data[author_did]['total_engagement'] += engagement
                user_data[author_did]['post_count'] += 1

                # --- LOG OGNI MINUTO ---
                if time.time() - last_log_time >= 60:
                    pct = (f_raw.tell() / file_size_total) * 100
                    ts = datetime.now().strftime('%H:%M:%S')
                    print(f"[{ts}] ⏳ {pct:.1f}% | Righe: {total_lines:,} | Utenti in memoria: {len(user_data)}", flush=True)
                    last_log_time = time.time()
                    
            except Exception:
                continue

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] ✅ Estrazione completata. Creazione DataFrame...")
    
    df = pd.DataFrame.from_dict(user_data, orient='index').reset_index()
    df.columns = ['did', 'total_engagement', 'post_count', 'created_at']
    
    # Pulizia finale
    df = df[df['created_at'].notnull()].copy()
    df.to_csv(OUTPUT_CSV, index=False)
    
    durata = int(time.time() - start_time)
    print(f"🎉 Finito! Salvati {len(df)} utenti in {OUTPUT_CSV}")
    print(f"⏱️ Tempo impiegato: {durata // 60}m {durata % 60}s")

if __name__ == "__main__":
    estrai_dati()