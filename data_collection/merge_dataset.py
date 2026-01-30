# Tesi/data_collection/merge_dataset.py
# Unisce timelines-1...4 e dati_profili in un unico file finale.

import gzip
import json
import os
from tqdm import tqdm

# --- CONFIGURAZIONE PERCORSI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# File Profili (Metadati da unire)
PROFILE_FILE = os.path.join(DATA_DIR, 'dati_profili_completi.json.gz')

# File Output Unico
OUTPUT_FILE = os.path.join(DATA_DIR, 'dataset_definitivo.jsonl.gz')

# File Input da cercare (1, 2, 3, 4)
NUM_FILES_TO_PROCESS = 4
# -------------------------------

def load_profiles():
    print(f"📂 Carico mappa profili da '{PROFILE_FILE}'...")
    if not os.path.exists(PROFILE_FILE):
        print("❌ Errore: File profili non trovato. Esegui prima 'scarica_followers.py'.")
        return {}
    
    try:
        with gzip.open(PROFILE_FILE, 'rt', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✅ Profili caricati in RAM: {len(data)}")
        return data
    except Exception as e:
        print(f"❌ Errore lettura profili: {e}")
        return {}

def main():
    print(f"working dir: {DATA_DIR}")
    
    # 1. Caricamento Profili
    profiles_map = load_profiles()
    if not profiles_map: return

    # 2. Preparazione Output
    print(f"🚀 Inizio fusione di {NUM_FILES_TO_PROCESS} file timeline...")
    print(f"💾 Output destinazione: {OUTPUT_FILE}")

    total_posts = 0
    enriched_posts = 0

    # Apre il file di output una volta sola in scrittura
    with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as fout:
        
        # Cicla sui file da 1 a 4
        for i in range(1, NUM_FILES_TO_PROCESS + 1):
            filename = f"timelines-{i}.jsonl.gz"
            input_path = os.path.join(DATA_DIR, filename)
            
            if not os.path.exists(input_path):
                print(f"⚠️  File {filename} non trovato, salto.")
                continue

            print(f"🔄 Processando: {filename}...")
            
            try:
                # Legge il file di input corrente
                with gzip.open(input_path, 'rt', encoding='utf-8') as fin:
                    for line in tqdm(fin, desc=f"Reading {filename}", unit="posts"):
                        try:
                            post_data = json.loads(line)
                            total_posts += 1
                            
                            # Recupera il DID dell'autore dal post
                            # Nel crawler avevamo salvato: "author": { "did": "..." }
                            author_did = post_data.get('author', {}).get('did')

                            # Se troviamo il DID nella mappa profili, aggiungiamo i dati
                            if author_did and author_did in profiles_map:
                                post_data['author_meta'] = profiles_map[author_did]
                                enriched_posts += 1
                            
                            # Scrive la riga arricchita nel file finale
                            fout.write(json.dumps(post_data) + '\n')
                            
                        except json.JSONDecodeError:
                            continue
                            
            except Exception as e:
                print(f"❌ Errore processando {filename}: {e}")

    print("\n" + "="*50)
    print("🎉 FUSIONE COMPLETATA")
    print(f"📄 File creato: dataset_definitivo.jsonl.gz")
    print(f"📊 Totale Post: {total_posts}")
    print(f"✨ Post Arricchiti con Metadati: {enriched_posts}")
    print("="*50)

if __name__ == "__main__":
    main()