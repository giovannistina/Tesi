# Tesi / experiments / equazion_differenziale / step_1 / a_preprocessing.py








import gzip
import json
import csv
import os
import sys
import time
from datetime import datetime

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "../../../data_collection/data/dataset_definitivo_6mesi.jsonl.gz"))
OUTPUT_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_log.csv.gz"))

def parse_record(line):
    try:
        data = json.loads(line)

        

        # 1. TIMESTAMP POST
        created_at = data.get('record', {}).get('created_at') or data.get('created_at')
        if not created_at: return None
        try:
            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        except ValueError:
        # Alcuni formati ISO vecchi o leggermente diversi potrebbero fallire
            return None
        ts = dt.timestamp()

        # 2. DID (User ID)
        did = data.get('author', {}).get('did') or data.get('did')
        if not did: return None

        # 3. METRICHE SEPARATE
        likes = data.get('like_count', 0) or 0
        reposts = data.get('repost_count', 0) or 0
        replies = data.get('reply_count', 0) or 0
        
        # 4. CALCOLO TOTAL ENGAGEMENT (v_i)
        # Manteniamo il +1 per evitare log(0) nel modello matematico
        v_i = 1 + likes + reposts + replies
        
        # 5. DATA CREAZIONE PROFILO
        user_meta = data.get('author_meta', {})
        user_created_str = user_meta.get('created_at')
        user_created_ts = -1.0
        if user_created_str:
            try:
                dt_u = datetime.fromisoformat(user_created_str.replace('Z', '+00:00'))
                user_created_ts = dt_u.timestamp()
            except: pass 

        # RESTITUISCE TUTTE LE COLONNE
        return [ts, did, likes, replies, reposts, v_i, user_created_ts]

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
    print(f"--- Modulo A: Preprocessing (Detailed Columns) ---", flush=True)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Errore: Input {INPUT_FILE} non trovato.", flush=True)
        sys.exit(1)

    total_bytes = os.path.getsize(INPUT_FILE)
    print(f"📦 Dimensione Input: {total_bytes / (1024**3):.2f} GB", flush=True)

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    lines_already_done = count_existing_lines(OUTPUT_FILE)
    mode = 'at' if lines_already_done > 0 else 'wt'
    
    HEADER = ['ts', 'did', 'likes', 'replies', 'reposts', 'v_i', 'user_created_ts']

    if lines_already_done > 0:
        print(f"⏩ Riprendo dalla riga {lines_already_done}...", flush=True)
    else:
        print(f"🆕 Inizio da zero...", flush=True)

    input_line_counter = 0
    new_lines_written = 0
    skipped_lines = 0  # <--- NUOVO CONTATORE
    
    with open(INPUT_FILE, 'rb') as f_raw:
        with gzip.open(f_raw, 'rt', encoding='utf-8') as f_in, \
             gzip.open(OUTPUT_FILE, mode, encoding='utf-8', newline='') as f_out:
            
            writer = csv.writer(f_out)
            if lines_already_done == 0:
                writer.writerow(HEADER)
            
            start_time = time.time()
            last_print_time = start_time

            for line in f_in:
                if input_line_counter < lines_already_done:
                    input_line_counter += 1
                    continue
                
                row = parse_record(line)
                if row:
                    writer.writerow(row)
                    new_lines_written += 1
                else:
                    skipped_lines += 1 # <--- INCREMENTO SE IL PARSE FALLISCE
                
                input_line_counter += 1
                
                if new_lines_written > 0 and new_lines_written % 50000 == 0:
                    current_time = time.time()
                    if current_time - last_print_time >= 10: 
                        current_pos = f_raw.tell()
                        pct = (current_pos / total_bytes) * 100
                        ts_now = datetime.now().strftime('%H:%M:%S')
                        total_lines = lines_already_done + new_lines_written
                        print(f"[{ts_now}] ⏳ {pct:.2f}% | Righe OK: {total_lines} | Scartate: {skipped_lines}", flush=True)
                        last_print_time = current_time

    # --- REPORT FINALE ESTRAZIONE ---
    print(f"\n" + "="*40)
    print(f"✅ ESTRAZIONE COMPLETATA")
    print(f"Totalizzatore finale:")
    print(f"  - Eventi validi scritti: {new_lines_written}")
    print(f"  - Righe scartate (errori/vuote): {skipped_lines}") # <--- STAMPA FINALE
    if skipped_lines > 0:
        pct_discarded = (skipped_lines / (new_lines_written + skipped_lines)) * 100
        print(f"  - Percentuale scarto: {pct_discarded:.2f}%")
    print("="*40 + "\n")

    
    # ORDINAMENTO FINALE
    print("⏳ Ricarico per ordinamento finale...", flush=True)
    data = []
    header = []
    with gzip.open(OUTPUT_FILE, 'rt', encoding='utf-8') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
            data = list(reader)
        except StopIteration: pass

    if data:
        print(f"   Ordinamento di {len(data)} righe...", flush=True)
        # Ordina sempre per 'ts' (che è alla posizione 0)
        data.sort(key=lambda x: float(x[0]))
        
        print("💾 Sovrascrittura file ordinato...", flush=True)
        with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(data)
        print("✅ File riordinato e salvato.", flush=True)

if __name__ == "__main__":
    main()