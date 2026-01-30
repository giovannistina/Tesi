# Tesi/data_collection/scarica_followers.py
# Scarica i metadati (followers, follows, bio, data creaz.) per TUTTI gli utenti in all_users.txt
# Output: data/dati_profili_completi.json.gz

import os
import json
import time
import gzip
import sys
import getpass # Per inserimento password sicuro
from atproto import Client, SessionEvent
from tqdm import tqdm

# --- CONFIGURAZIONE PERCORSI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Input: File cumulativo creato da unisci_utenti.py
INPUT_TXT = os.path.join(DATA_DIR, 'all_users.txt')

# Output: File JSON compresso unico
OUTPUT_FILE = os.path.join(DATA_DIR, 'dati_profili_completi.json.gz')

# Sessione (Dinamica)
SESSION_FILE = os.path.join(BASE_DIR, 'session_meta.txt')
# -------------------------------

def get_session():
    try:
        with open(SESSION_FILE, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def save_session(session_string):
    with open(SESSION_FILE, 'w') as f:
        f.write(session_string)

def init_client_interactive():
    client = Client()
    
    # Callback per salvare la sessione se cambia
    def on_session_change(event, session):
        if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
            save_session(session.export())
    
    client.on_session_change(on_session_change)

    # 1. Prova a riusare la sessione salvata
    session_string = get_session()
    if session_string:
        print("🔄 Tentativo riutilizzo sessione esistente...")
        try:
            client.login(session_string=session_string)
            print("✅ Login via sessione riuscito.")
            return client
        except Exception:
            print("⚠️ Sessione scaduta.")

    # 2. Se fallisce, chiedi credenziali a video
    print("\n" + "="*40)
    print("🔑 LOGIN NECESSARIO PER SCARICARE I PROFILI")
    print("="*40)
    try:
        user = input("Inserisci Username Bluesky: [username.bsky.social] ").strip()
        pwd = getpass.getpass("Inserisci Password Bluesky (nascosta): ").strip()
    except KeyboardInterrupt:
        print("\nOperazione annullata.")
        sys.exit(0)
    
    print("⏳ Login in corso...")
    client.login(user, pwd)
    print("✅ Login effettuato con successo.")
    return client

def leggi_utenti_da_txt():
    if not os.path.exists(INPUT_TXT):
        print(f"❌ Errore: File input non trovato: {INPUT_TXT}")
        print("👉 Assicurati di aver eseguito 'unisci_utenti.py' prima.")
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
    print(f"📂 Script in esecuzione su: {BASE_DIR}")
    print(f"📄 Lettura utenti da: {INPUT_TXT}")
    
    # 1. Login Interattivo
    try:
        client = init_client_interactive()
    except Exception as e:
        print(f"❌ Errore Login: {e}")
        return

    # 2. Caricamento Utenti
    all_users = leggi_utenti_da_txt()
    if not all_users: 
        return

    print(f"✅ Trovati {len(all_users)} utenti unici da analizzare.")
    
    # 3. Download Dati (Batch di 25 utenti per volta)
    profiles_map = {}
    batches = list(chunk_list(all_users, 25))
    
    print(f"⬇️ Inizio download metadati...")
    
    for batch in tqdm(batches, desc="Fetching Profiles"):
        try:
            # Chiamata API per ottenere 25 profili in un colpo solo
            profiles = client.app.bsky.actor.get_profiles({'actors': batch})
            
            for profile in profiles.profiles:
                # Recupera data creazione (gestisce diverse versioni API)
                data_creazione = getattr(profile, 'indexed_at', None) or getattr(profile, 'created_at', None)
                
                dati_utente = {
                    "followers": profile.followers_count or 0,
                    "follows": profile.follows_count or 0,
                    "created_at": data_creazione,
                    "description": profile.description or "" 
                }
                profiles_map[profile.did] = dati_utente
                
        except Exception as e:
            # Se un batch fallisce (es. utente cancellato nel mezzo), aspettiamo un attimo e proseguiamo
            # (Per un codice più robusto si potrebbe riprovare il batch, ma rallenta)
            time.sleep(1)

    # 4. Salvataggio
    print(f"💾 Salvataggio dati in corso in: {OUTPUT_FILE}")
    try:
        with gzip.open(OUTPUT_FILE, 'wt', encoding='utf-8') as f:
            json.dump(profiles_map, f, indent=4, ensure_ascii=False)
        
        print(f"🎉 FILE SALVATO CORRETTAMENTE!")
        print(f"Totale profili scaricati: {len(profiles_map)} su {len(all_users)}")

    except Exception as e:
        print(f"❌ Errore durante il salvataggio del file: {e}")

if __name__ == "__main__":
    main()