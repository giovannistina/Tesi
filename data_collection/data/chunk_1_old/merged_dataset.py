import gzip
import json
import os
from tqdm import tqdm

# --- CONFIGURAZIONE ---
INPUT_DIR = '.'                     
PROFILE_FILE = 'dati_profili_completi.json' 
OUTPUT_SUFFIX = '_merged'           
# ----------------------

def load_profiles():
    print(f"📂 Carico dati profili da '{PROFILE_FILE}'...")
    if not os.path.exists(PROFILE_FILE):
        print("❌ Errore: File profili non trovato.")
        return {}
    
    with open(PROFILE_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"✅ Caricati {len(data)} profili in memoria.")
    return data

def process_file(file_name, profiles_map):
    input_path = os.path.join(INPUT_DIR, file_name)
    base_name = file_name.replace('.jsonl.gz', '')
    output_name = f"{base_name}{OUTPUT_SUFFIX}.jsonl.gz"
    output_path = os.path.join(INPUT_DIR, output_name)
    
    file_size = os.path.getsize(input_path)
    
    print(f"🔄 Elaborazione: {file_name} -> {output_name}")
    
    matches = 0
    total_lines = 0
    
    try:
        # 1. Apriamo il file RAW (binario)
        with open(input_path, 'rb') as f_in_raw:
            
            # 2. Avvolgiamo il file raw con TQDM per monitorare i byte letti
            with tqdm.wrapattr(f_in_raw, "read", total=file_size, unit="B", unit_scale=True, desc="Merging") as f_in_wrapped:
                
                # 3. Passiamo il file "monitorato" a GZIP
                with gzip.open(f_in_wrapped, 'rt', encoding='utf-8') as fin, \
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
                
        print(f"✅ Completato. {matches}/{total_lines} post sono stati arricchiti.")
        print(f"💾 File salvato: {output_name}\n")
        
    except Exception as e:
        print(f"❌ Errore processando il file {file_name}: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)

def main():
    profiles_map = load_profiles()
    if not profiles_map:
        return

    files = [f for f in os.listdir(INPUT_DIR) 
             if f.endswith('.jsonl.gz') 
             and OUTPUT_SUFFIX not in f]
    
    if not files:
        print("❌ Nessun file .jsonl.gz trovato da processare.")
        return
    
    print(f"files trovati: {len(files)}")
    print("-" * 60)

    for f in files:
        process_file(f, profiles_map)

if __name__ == "__main__":
    main()