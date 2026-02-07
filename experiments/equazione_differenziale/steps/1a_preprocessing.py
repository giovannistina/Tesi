# Tesi / experiments / equazione_differenziale / steps / 1a_preprocessing.py

import gzip
import json
import csv
import os
import sys
import time
from datetime import datetime

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Input file (quello nuovo con i followers)
INPUT_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "../../../data_collection/data/dataset_definitivo_6mesi.jsonl.gz"))
OUTPUT_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_log.csv.gz"))

def parse_record(line):
    try:
        data = json.loads(line)

        # 1. TIMESTAMP
        created_at = data.get('created_at') or data.get('record', {}).get('created_at')
        if not created_at: return None
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        except ValueError:
            return None
        ts = dt.timestamp()

        # 2. DID (Chi sta compiendo l'azione)
        actor_did = data.get('did') or data.get('author', {}).get('did')
        if not actor_did: return None

        # 3. URI (L'oggetto del post)
        uri = data.get('uri', '')
        
        # 4. METRICHE GREZZE
        likes = data.get('like_count', 0) or 0
        reposts = data.get('repost_count', 0) or 0
        replies = data.get('reply_count', 0) or 0
        
        # --- LOGICA CRUCIALE: Rilevamento Repost ---
        # Un post è un REPOST se:
        # A. C'è un flag esplicito 'is_repost' (se il dataset lo ha)
        # B. Il DID dentro l'URI è diverso dal DID dell'attore (sta condividendo roba altrui)
        
        is_repost_explicit = data.get('is_repost') is True
        
        # Estraiamo il DID dall'URI: "at://did:plc:abcdef/..."
        uri_owner_did = None
        if uri.startswith("at://"):
            parts = uri.split('/')
            if len(parts) >= 3:
                uri_owner_did = parts[2]
        
        # Se l'URI appartiene a un altro, è un Repost!
        # (A meno che non sia nullo, nel qual caso ci fidiamo dei flag)
        is_repost_implicit = (uri_owner_did is not None) and (uri_owner_did != actor_did)

        if is_repost_explicit or is_repost_implicit:
            # È UN REPOST: Il merito dei like non è mio
            likes = 0
            reposts = 0
            replies = 0
            post_type = 'repost'
        else:
            # È MIO: Mi prendo il merito
            post_type = 'post'
        
        # 5. CALCOLO ENGAGEMENT (v_i)
        v_i = 1 + likes + reposts + replies
        
        # 6. DATA CREAZIONE PROFILO
        user_meta = data.get('author_meta', {})
        user_created_str = user_meta.get('created_at')
        user_created_ts = -1.0
        if user_created_str:
            try:
                dt_u = datetime.fromisoformat(user_created_str.replace('Z', '+00:00'))
                user_created_ts = dt_u.timestamp()
            except: pass 
            
        # 7. FOLLOWERS
        followers = data.get('followers_count', 0)

        return [ts, actor_did, likes, replies, reposts, v_i, user_created_ts, post_type, followers]

    except Exception:
        return None

def count_existing_lines(filepath):
    if not os.path.exists(filepath): return 0
    print(f"🔄 Controllo file esistente...", flush=True)
    count = 0
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            for _ in f: count += 1
        return max(0, count - 1)
    except Exception: return 0

def main():
    print(f"--- Modulo A: Preprocessing v3 (Smart Repost Detection) ---", flush=True)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Errore: Input {INPUT_FILE} non trovato.", flush=True)
        sys.exit(1)

    total_bytes = os.path.getsize(INPUT_FILE)
    print(f"📦 Dimensione Input: {total_bytes / (1024**3):.2f} GB", flush=True)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    # Nota: Ripartiamo DA ZERO per correggere gli errori precedenti
    # Se vuoi puoi commentare queste righe se vuoi fare append, ma consiglio sovrascrittura
    mode = 'wt'
    lines_already_done = 0
    
    HEADER = ['ts', 'did', 'likes', 'replies', 'reposts', 'v_i', 'user_created_ts', 'post_type', 'followers_count']

    print(f"🛠  Rigenerazione completa del file eventi...", flush=True)
    
    new_lines_written = 0
    skipped_lines = 0 
    
    with open(INPUT_FILE, 'rb') as f_raw:
        with gzip.open(f_raw, 'rt', encoding='utf-8') as f_in, \
             gzip.open(OUTPUT_FILE, mode, encoding='utf-8', newline='') as f_out:
            
            writer = csv.writer(f_out)
            writer.writerow(HEADER)
            
            start_time = time.time()
            last_print_time = start_time

            for line in f_in:
                row = parse_record(line)
                if row:
                    writer.writerow(row)
                    new_lines_written += 1
                else:
                    skipped_lines += 1 
                
                if new_lines_written > 0 and new_lines_written % 50000 == 0:
                    current_time = time.time()
                    if current_time - last_print_time >= 5: 
                        current_pos = f_raw.tell()
                        pct = (current_pos / total_bytes) * 100
                        ts_now = datetime.now().strftime('%H:%M:%S')
                        print(f"[{ts_now}] ⏳ {pct:.2f}% | Righe: {new_lines_written} | Skip: {skipped_lines}", flush=True)
                        last_print_time = current_time

    print(f"\n✅ ESTRAZIONE COMPLETATA. Righe scritte: {new_lines_written}")
    
    print("⏳ Riordinamento temporale...", flush=True)
    try:
        data = []
        with gzip.open(OUTPUT_FILE, 'rt', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            data = list(reader)
        
        data.sort(key=lambda x: float(x[0]))
        
        with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(data)
        print("✅ File ordinato e salvato.")
        
    except Exception as e:
        print(f"⚠️ Errore durante l'ordinamento: {e}")

if __name__ == "__main__":
    main()