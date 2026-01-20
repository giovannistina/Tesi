# .../Tesi/data_collection/enrich_feed_census_with_creator_bios.py

import pandas as pd
from atproto import Client
import time
import os

# --- CONFIGURAZIONE ---
INPUT_FILE = 'results/feed_stats/bluesky_feed_census_v2_with_lang.csv'
OUTPUT_FILE = 'results/feed_stats/bluesky_feed_census_v2_with_lang_and_bios.csv'
SESSION_FILE = 'session.txt'

def main():
    # 1. Inizializzazione Client e Caricamento Sessione
    client = Client()

    if not os.path.exists(SESSION_FILE):
        print(f"Errore: il file {SESSION_FILE} non esiste. Esegui prima lo script di login.")
        return

    try:
        with open(SESSION_FILE, "r") as f:
            session_string = f.read().strip()
        client.login(session_string=session_string)
        print("Accesso tramite sessione riuscito!")
    except Exception as e:
        print(f"Errore durante il caricamento della sessione: {e}")
        return

    # 2. Caricamento Dati
    if not os.path.exists(INPUT_FILE):
        print(f"Errore: il file {INPUT_FILE} non esiste.")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"File caricato: {len(df)} righe.")

    # 3. Identificazione Creator Univoci
    unique_dids = df['creator_did'].unique()
    total_unique = len(unique_dids)
    print(f"Creator univoci da analizzare: {total_unique}")

    # 4. Recupero Bio e Follower tramite API
    # Usiamo due dizionari per mappare i dati ai DID
    bios_map = {}
    followers_map = {}
    
    start_time = time.time()
    
    for i, did in enumerate(unique_dids, 1):
        try:
            # Recupero il profilo completo
            profile = client.get_profile(actor=did)
            
            # Estrazione dati
            bios_map[did] = profile.description if profile.description else ""
            followers_map[did] = profile.followers_count if profile.followers_count is not None else 0
            
        except Exception as e:
            print(f"Errore per il DID {did}: {e}")
            bios_map[did] = "N/A (Error or Deleted)"
            followers_map[did] = 0 # O un valore che indichi l'errore, es. -1

        # Feedback ogni 50 creator
        if i % 500 == 0:
            elapsed = time.time() - start_time
            avg_time = elapsed / i
            remaining_est = avg_time * (total_unique - i) / 60
            print(f"[{i}/{total_unique}] Processati. Tempo stimato rimanente: {remaining_est:.2f} minuti")
            # Pausa per rispettare i rate limits
            time.sleep(1.5)

    # 5. Mapping e Aggiornamento del DataFrame
    print("Mappatura dei dati sul dataframe originale...")
    
    # Aggiunge la nuova colonna delle descrizioni
    df['creator_description'] = df['creator_did'].map(bios_map)
    
    # Sovrascrive la vecchia colonna dei follower con il dato aggiornato e univoco
    df['creator_followers'] = df['creator_did'].map(followers_map)

    # 6. Salvataggio Output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    
    print("\n--- OPERAZIONE COMPLETATA ---")
    print(f"File salvato in: {OUTPUT_FILE}")
    print(f"Totale righe elaborate: {len(df)}")

if __name__ == "__main__":
    main()