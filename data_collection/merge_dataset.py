# Tesi/data_collection/merged_dataset.py
# salva in Tesi/data_collection/data/chunk_1
# Ha bisogno in input di: dati_profili_completi.json.gz e timelines-50000.jsonl.gz presenti in Tesi/data_collection/data/chunk_1




import gzip
import json
import os
from tqdm import tqdm

# --- CONFIGURAZIONE PERCORSI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Cartella dove sono i dati (chunk_1)
DATA_DIR = os.path.join(BASE_DIR, 'data', 'chunk_1')

# File Profili (COMPRESSO .gz)
PROFILE_FILE = os.path.join(DATA_DIR, 'dati_profili_completi.json.gz')

# Suffisso output
OUTPUT_SUFFIX = '_merged' 
# -------------------------------

def load_profiles():
    print(f"📂 Carico profili da '{PROFILE_FILE}'...")
    if not os.path.exists(PROFILE_FILE):
        print("❌ Errore: File profili non trovato. Esegui prima 'scarica_followers.py'.")
        return {}
    
    try:
        # Modifica fondamentale: Apre con GZIP
        with gzip.open(PROFILE_FILE, 'rt', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✅ Caricati {len(data)} profili in memoria.")
        return data
    except Exception as e:
        print(f"❌ Errore lettura GZ profili: {e}")
        return {}

def process_file(file_name, profiles_map):
    input_path = os.path.join(DATA_DIR, file_name)
    
    base_name = file_name.replace('.jsonl.gz', '')
    output_name = f"{base_name}{OUTPUT_SUFFIX}.jsonl.gz"
    output_path = os.path.join(DATA_DIR, output_name)
    
    file_size = os.path.getsize(input_path)
    print(f"🔄 Elaborazione: {file_name} -> {output_name}")
    
    matches = 0
    total_lines = 0
    
    try:
        # Usiamo tqdm semplice sui byte
        with tqdm.wrapattr(open(input_path, "rb"), "read", total=file_size, unit="B", unit_scale=True, desc="Merging") as f_raw:
            with gzip.open(f_raw, 'rt', encoding='utf-8') as fin, \
                 gzip.open(output_path, 'wt', encoding='utf-8') as fout:
                
                for line in fin:
                    try:
                        post_data = json.loads(line)
                        total_lines += 1
                        
                        user_did = post_data.get('user') or post_data.get('author', {}).get('did')
                        
                        if user_did and user_did in profiles_map:
                            post_data['author_enriched'] = profiles_map[user_did]
                            matches += 1
                        
                        fout.write(json.dumps(post_data) + '\n')
                            
                    except json.JSONDecodeError:
                        continue
                
        print(f"✅ Completato. {matches}/{total_lines} arricchiti.")
        print(f"💾 File salvato in: {output_path}\n")
        
    except Exception as e:
        print(f"❌ Errore: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)

def main():
    print(f"📂 Cartella dati: {DATA_DIR}")
    
    # 1. Carica Profili
    profiles_map = load_profiles()
    if not profiles_map: return

    # 2. Trova file Timeline (timelines-XXXX.jsonl.gz)
    # Escludiamo file già mergiati e il file dei profili
    files = [f for f in os.listdir(DATA_DIR) 
             if f.endswith('.jsonl.gz') 
             and OUTPUT_SUFFIX not in f
             and 'dati_profili' not in f] # Esclude il file profili stesso
    
    if not files:
        print("❌ Nessun file timelines trovato in chunk_1.")
        return
    
    print(f"files trovati: {len(files)}")
    print("-" * 60)

    for f in files:
        process_file(f, profiles_map)

if __name__ == "__main__":
    main()