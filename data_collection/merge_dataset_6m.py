# Tesi/data_collection/merge_dataset_6m.py
# Unisce timelines-6m-chunk... e dati_profili_completi_6_mesi in un unico file finale.




import gzip
import json
import os
import time
import glob
from datetime import datetime

# --- CONFIGURAZIONE ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# 1. Nome esatto del file profili appena creato
PROFILE_FILE = os.path.join(DATA_DIR, 'dati_profili_completi_6_mesi.jsonl.gz')

# 2. Output finale
OUTPUT_FILE = os.path.join(DATA_DIR, 'dataset_definitivo_6mesi.jsonl.gz')

# 3. Pattern per trovare i chunk (timelines-6m-chunk_0, _1, etc.)
TIMELINE_PATTERN = os.path.join(DATA_DIR, 'timelines-6m-*.jsonl.gz')
# ----------------------

def get_job_info():
    """Trova automaticamente tutti i chunk scaricati."""
    total = 0
    files = []
    
    # Usa glob per trovare tutti i file che corrispondono al pattern
    found_files = glob.glob(TIMELINE_PATTERN)
    
    for path in found_files:
        size = os.path.getsize(path)
        total += size
        # Estraiamo un ID dal nome file per riferimento (es. chunk_0)
        chunk_id = os.path.basename(path).split('.')[0]
        files.append((chunk_id, path, size))
        
    return total, files

def load_profiles():
    """Carica i profili stampando aggiornamenti."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 📂 Inizio caricamento profili in RAM...", flush=True)
    
    if not os.path.exists(PROFILE_FILE):
        print(f"❌ File profili non trovato: {PROFILE_FILE}", flush=True)
        return {}
        
    data = {}
    count = 0
    try:
        with gzip.open(PROFILE_FILE, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    r = json.loads(line)
                    if 'did' in r:
                        data[r['did']] = r
                        count += 1
                        
                        if count % 10000 == 0:
                            print(f"   ...caricati {count} profili...", end='\r', flush=True)
                except: continue
                
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] ✅ Profili pronti: {len(data)}", flush=True)
        return data
    except Exception as e:
        print(f"❌ Errore profili: {e}", flush=True)
        return {}

def main():
    # 1. Cerca i file
    total_job_bytes, files_list = get_job_info()
    
    if not files_list:
        print(f"❌ Nessun file timeline trovato in {TIMELINE_PATTERN}")
        return

    total_gb = total_job_bytes / (1024**3)
    
    print("="*60, flush=True)
    print(f"📊 LAVORO TROVATO: {len(files_list)} file | {total_gb:.2f} GB Totali", flush=True)
    print("="*60, flush=True)

    # 2. Carica profili
    profiles_map = load_profiles()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 AVVIO MERGE...", flush=True)
    
    total_posts = 0
    enriched_posts = 0
    bytes_done_prev = 0
    last_log_time = time.time() - 60 
    
    # Apre il file finale in scrittura
    with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as fout:
        
        for chunk_id, input_path, file_size in files_list:
            filename = os.path.basename(input_path)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⤵️  Apro: {filename}", flush=True)
            
            try:
                # Legge il file di input
                with open(input_path, 'rb') as f_raw: # Apro binario per .tell()
                    with gzip.open(f_raw, 'rt', encoding='utf-8') as fin:
                        
                        for line in fin:
                            try:
                                post = json.loads(line)
                                total_posts += 1
                                
                                # ARRICCHIMENTO
                                did = post.get('author', {}).get('did')
                                if did and did in profiles_map:
                                    post['author_meta'] = profiles_map[did]
                                    enriched_posts += 1
                                
                                # Scrittura
                                fout.write(json.dumps(post) + '\n')

                                # LOGICA LOG
                                if total_posts % 5000 == 0:
                                    now = time.time()
                                    if now - last_log_time >= 30:
                                        cur_pos = f_raw.tell()
                                        done = bytes_done_prev + cur_pos
                                        pct = (done / total_job_bytes) * 100
                                        
                                        ts = datetime.now().strftime('%H:%M:%S')
                                        print(f"[{ts}] {filename} | Post: {total_posts} | ⏳ Totale: {pct:.2f}%", flush=True)
                                        last_log_time = now

                            except json.JSONDecodeError:
                                continue
            
            except Exception as e:
                print(f"❌ Errore {filename}: {e}", flush=True)
            
            bytes_done_prev += file_size

    print("="*60, flush=True)
    print(f"🎉 FINE. Dataset salvato in: {OUTPUT_FILE}")
    print(f"   Post totali: {total_posts}")
    print(f"   Arricchiti: {enriched_posts}")

if __name__ == "__main__":
    main()