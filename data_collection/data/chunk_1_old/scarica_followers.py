import os
import json
import time
from atproto import Client
from tqdm import tqdm

# --- CONFIGURAZIONE ---
INPUT_TXT = '../1.txt'            # Il file con la lista utenti
OUTPUT_JSON = 'dati_profili_completi.json' 
SESSION_FILE = 'session.txt'      # Il file creato dal tuo script di login
# ----------------------

def get_client():
    client = Client()
    
    # 1. Prova a usare session.txt (METODO PRIORITARIO)
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, 'r') as f:
                session_string = f.read().strip()
            
            client.login(session_string=session_string)
            print("✅ Login effettuato usando 'session.txt'.")
            return client
        except Exception as e:
            print(f"⚠️ Errore con session.txt (forse scaduto?): {e}")
            print("   Tento il login classico...")

    # 2. Fallback su Username/Password (se session.txt fallisce o non c'è)
    username = os.environ.get('USERNAME')
    password = os.environ.get('PASSWORD')
    
    if username and password:
        client.login(username, password)
        return client

    raise Exception(f"Login fallito. Assicurati di aver eseguito 'crea_sessione.py' per generare {SESSION_FILE}.")

def leggi_utenti_da_txt():
    if not os.path.exists(INPUT_TXT):
        print(f"❌ Errore: Non trovo il file '{INPUT_TXT}'.")
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
    print(f"📄 Leggo utenti da: {INPUT_TXT}...")
    all_users = leggi_utenti_da_txt()
    
    if not all_users:
        return

    print(f"✅ Trovati {len(all_users)} utenti nella lista.")
    
    # Login gestito dalla nuova funzione
    try:
        client = get_client()
    except Exception as e:
        print(f"❌ {e}")
        return

    profiles_map = {}
    batches = list(chunk_list(all_users, 25))
    
    print(f"⬇️ Scarico dati estesi per {len(all_users)} profili...")
    
    for batch in tqdm(batches, desc="Fetching Profiles"):
        try:
            profiles = client.app.bsky.actor.get_profiles({'actors': batch})
            
            for profile in profiles.profiles:
                # Recupero data creazione
                data_creazione = getattr(profile, 'indexed_at', None) or getattr(profile, 'created_at', None)

                dati_utente = {
                    "followers": profile.followers_count or 0,
                    "follows": profile.follows_count or 0,
                    "created_at": data_creazione,
                    "description": profile.description or "" 
                }
                
                profiles_map[profile.did] = dati_utente
                
        except Exception as e:
            # print(f"⚠️ Errore batch: {e}")
            time.sleep(1)

    # Salvataggio
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(profiles_map, f, indent=4, ensure_ascii=False)
        
    print(f"🎉 Fatto! Salvati {len(profiles_map)} profili in: {OUTPUT_JSON}")

if __name__ == "__main__":
    main()