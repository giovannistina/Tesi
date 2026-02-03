# Tesi/data_collection/scarica_followers.py
# Utilizzo: python scarica_followers.py <FILE_INPUT> <USER> <PASS>
# OUTPUT: JSON Lines compresso (.jsonl.gz)

import os
import json
import time
import datetime
import gzip
import sys
import subprocess

# --- AUTO-INSTALLAZIONE LIBRERIA ---
try:
    import atproto
except ImportError:
    print("⚠️ Libreria 'atproto' mancante. Installazione automatica in corso...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "atproto"])
    print("✅ Installazione completata.")
    import atproto
# -----------------------------------

from atproto import Client, SessionEvent

# --- CONFIGURAZIONE PERCORSI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Output: JSONL compresso (ottimo per appendere dati)
OUTPUT_FILE = os.path.join(DATA_DIR, 'dati_profili_completi_6_mesi.jsonl.gz')
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

def init_client_cli(username, password):
    client = Client()
    
    def on_session_change(event, session):
        if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
            save_session(session.export())
    client.on_session_change(on_session_change)

    session_string = get_session()
    if session_string:
        print("🔄 Tentativo riutilizzo sessione esistente...")
        try:
            client.login(session_string=session_string)
            print("✅ Login via sessione riuscito.")
            return client
        except Exception:
            print("⚠️ Sessione scaduta. Procedo con login standard.")

    print(f"⏳ Login con credenziali per {username}...")
    try:
        client.login(username, password)
        print("✅ Login effettuato con successo.")
        return client
    except Exception as e:
        print(f"❌ Login fallito: {e}")
        sys.exit(1)

def leggi_utenti_da_txt(filename):
    if not os.path.isabs(filename):
        path = os.path.join(DATA_DIR, filename)
    else:
        path = filename

    if not os.path.exists(path):
        print(f"❌ Errore: File input non trovato: {path}")
        return set()
    
    utenti = set()
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            u = line.strip()
            if u:
                utenti.add(u)
    return utenti

def get_processed_users():
    """Legge il file di output per vedere chi abbiamo già scaricato"""
    processed = set()
    if not os.path.exists(OUTPUT_FILE):
        return processed
    
    print("📂 Controllo file esistente per riprendere il lavoro...")
    try:
        with gzip.open(OUTPUT_FILE, 'rt', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if 'did' in data:
                        processed.add(data['did'])
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"⚠️ Errore lettura file esistente (potrebbe essere vuoto): {e}")
    
    return processed

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main():
    if len(sys.argv) != 4:
        print("Uso corretto: python scarica_followers.py <FILE_LISTA.txt> <USERNAME> <PASSWORD>")
        sys.exit(1)

    ARG_FILENAME = sys.argv[1]
    ARG_USER = sys.argv[2]
    ARG_PASS = sys.argv[3]

    print(f"📂 Script in esecuzione su: {BASE_DIR}")
    
    # 1. Login
    client = init_client_cli(ARG_USER, ARG_PASS)

    # 2. Caricamento Input e Controllo Già Fatti
    all_users_set = leggi_utenti_da_txt(ARG_FILENAME)
    if not all_users_set: 
        print("Nessun utente trovato nel file di input.")
        return

    already_done = get_processed_users()
    print(f"📊 Totale Input: {len(all_users_set)} | Già scaricati: {len(already_done)}")
    
    # Calcolo differenza (chi manca?)
    users_to_do = list(all_users_set - already_done)
    
    if not users_to_do:
        print("✅ Tutti gli utenti sono già stati scaricati. Fine.")
        return

    print(f"🚀 Da scaricare: {len(users_to_do)} utenti.")
    
    # 3. Download Incrementale
    BATCH_SIZE = 25
    batches = list(chunk_list(users_to_do, BATCH_SIZE))
    processed_count = len(already_done)
    total_target = len(all_users_set)
    
    # Apre il file in modalità APPEND ('at' = append text)
    # Se non esiste lo crea, se esiste aggiunge alla fine.
    try:
        with gzip.open(OUTPUT_FILE, 'at', encoding='utf-8') as f_out:
            
            for batch in batches:
                try:
                    profiles = client.app.bsky.actor.get_profiles({'actors': batch})
                    
                    for profile in profiles.profiles:
                        data_creazione = getattr(profile, 'indexed_at', None) or getattr(profile, 'created_at', None)
                        
                        record = {
                            "did": profile.did, # Importante: salviamo il DID dentro l'oggetto
                            "followers": profile.followers_count or 0,
                            "follows": profile.follows_count or 0,
                            "created_at": data_creazione,
                            "description": profile.description or "" 
                        }
                        
                        # Scrive subito la riga nel file compresso
                        f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    
                    # Forza la scrittura su disco
                    f_out.flush()
                    
                    # Aggiornamento contatore
                    processed_count += len(batch)
                    
                    # Log ogni 1000
                    if processed_count % 1000 == 0 or processed_count == total_target:
                        percent = (processed_count / total_target) * 100
                        now_str = datetime.datetime.now().strftime("%H:%M:%S")
                        print(f"[{now_str}] Avanzamento: {processed_count}/{total_target} ({percent:.2f}%) - Dati salvati.")
                        
                except Exception as e:
                    print(f"⚠️ ERRORE nel batch: {e}")
                    time.sleep(1)

        print(f"\n🎉 COMPLETATO. File salvato in: {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ ERRORE CRITICO APERTURA FILE: {e}")

if __name__ == "__main__":
    main()