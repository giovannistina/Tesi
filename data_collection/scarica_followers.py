# Tesi/data_collection/scarica_followers.py
# scarica i followers degli utenti presenti in esi/data_collection/data/1.txt
# salva followers e follows e  bio in esi/data_collection/data/chunk_1/dati_profili_completi.jsno.gz
# dopo questo runnare Tesi/data_collection/merge_dataset.py per ottenere il db definitivo da dove far partire le analisi

import os
import json
import time
import gzip
from atproto import Client
from tqdm import tqdm

# --- CONFIGURAZIONE PERCORSI ---
# Cartella corrente dello script (.../data_collection)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Input: data/1.txt
INPUT_TXT = os.path.join(BASE_DIR, 'data', '1.txt')

# Output Cartella: data/chunk_1
OUTPUT_DIR = os.path.join(BASE_DIR, 'data', 'chunk_1')

# Output File: dati_profili_completi.json.gz (COMPRESSO)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'dati_profili_completi.json.gz')

# Sessione
SESSION_FILE = os.path.join(BASE_DIR, 'session.txt')
# -------------------------------

def get_client():
    client = Client()
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, 'r') as f:
                session_string = f.read().strip()
            client.login(session_string=session_string)
            print("✅ Login effettuato usando 'session.txt'.")
            return client
        except Exception as e:
            print(f"⚠️ Errore sessione: {e}")

    username = os.environ.get('USERNAME')
    password = os.environ.get('PASSWORD')
    if username and password:
        client.login(username, password)
        return client

    raise Exception(f"Login fallito. Controlla {SESSION_FILE} o le credenziali.")

def leggi_utenti_da_txt():
    if not os.path.exists(INPUT_TXT):
        print(f"❌ Errore: File input non trovato: {INPUT_TXT}")
        return []
    
    utenti = set()
    with open(INPUT_TXT, 'r', encoding='utf-8') as f:
        for line in f:
            u = line.strip()
            if u:
                utenti.add(u)
    return list(utenti)

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    print(f"📂 Script in: {BASE_DIR}")
    print(f"📄 Leggo da: {INPUT_TXT}")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_users = leggi_utenti_da_txt()
    if not all_users: return

    print(f"✅ Trovati {len(all_users)} utenti.")
    
    try:
        client = get_client()
    except Exception as e:
        print(f"❌ {e}")
        return

    profiles_map = {}
    batches = list(chunk_list(all_users, 25))
    
    print(f"⬇️ Scarico dati per {len(all_users)} profili...")
    
    for batch in tqdm(batches, desc="Fetching"):
        try:
            profiles = client.app.bsky.actor.get_profiles({'actors': batch})
            
            for profile in profiles.profiles:
                data_creazione = getattr(profile, 'indexed_at', None) or getattr(profile, 'created_at', None)
                
                dati_utente = {
                    "followers": profile.followers_count or 0,
                    "follows": profile.follows_count or 0,
                    "created_at": data_creazione,
                    "description": profile.description or "" 
                }
                profiles_map[profile.did] = dati_utente
                
        except Exception as e:
            time.sleep(1)

    # Salvataggio COMPRESSO (.gz)
    print(f"💾 Salvataggio (compresso) in: {OUTPUT_FILE}")
    try:
        with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as f:
            json.dump(profiles_map, f, indent=4, ensure_ascii=False)
        print(f"🎉 Fatto! File salvato.")
        
        # --- BLOCCO DI OUTPUT AGGIUNTO ---
        print("\n" + "="*60)
        print("⚠️  ATTENZIONE: STEP SUCCESSIVO NECESSARIO")
        print("Ora devi unire questi dati ai post grezzi per le analisi.")
        print("Esegui subito il seguente comando:")
        print("👉 python data_collection/merge_dataset.py")
        print("="*60 + "\n")

    except Exception as e:
        print(f"❌ Errore salvataggio: {e}")

if __name__ == "__main__":
    main()