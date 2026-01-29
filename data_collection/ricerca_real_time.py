# Tesi/data_collection/ricerca_real_time.py

import sys
import time
import os
import datetime
from atproto import FirehoseSubscribeReposClient, parse_subscribe_repos_message

# --- CONFIGURATION ---
DATA_FOLDER = "data"
FILE_TIMELINES = "1.txt"      # Database storico (Append)
# Il nome del batch viene generato dinamicamente nel main
# ----------------------

def main():
    print("--- BLUESKY FIREHOSE LISTENER ---")
    
    # 1. Setup Cartella
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    # 2. Setup Nomi File Dinamici
    timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    batch_filename = f"batch_{timestamp_str}.txt"
    
    path_timelines = os.path.join(DATA_FOLDER, FILE_TIMELINES)
    path_batch = os.path.join(DATA_FOLDER, batch_filename)

    # 3. Input Tempo (AUTOMATICO PER ORCHESTRATORE)
    # Se lo script riceve un argomento (es. dall'orchestrator), usa quello.
    if len(sys.argv) > 1:
        DURATION_MINUTES = float(sys.argv[1])
        print(f"Modalità Automatica: Durata impostata a {DURATION_MINUTES} minuti.")
    else:
        # Altrimenti chiede all'utente
        try:
            minutes_input = input("Durata acquisizione in MINUTI: ")
            DURATION_MINUTES = float(minutes_input)
        except ValueError:
            print("Valore non valido. Default: 30 minuti.")
            DURATION_MINUTES = 30.0

    # Calcolo Tempi
    start_time = time.time()
    end_time_limit = start_time + (DURATION_MINUTES * 60)
    start_dt_str = datetime.datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')

    # 4. Caricamento memoria storico
    unique_users = set()
    if os.path.exists(path_timelines):
        with open(path_timelines, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    unique_users.add(line)

    session_users = [] 

    # 5. Apertura File
    f1 = open(path_timelines, 'a', encoding='utf-8') # Storico (Append)
    f2 = open(path_batch, 'w', encoding='utf-8')     # Batch Corrente (Nuovo file)

    header = (
        f"# SESSION START: {start_dt_str}\n"
        f"# TARGET DURATION: {DURATION_MINUTES} min\n"
        f"# INFO: SCROLL TO BOTTOM FOR STATS\n"
        f"# ---------------------------------------------------\n"
    )
    f1.write(header)
    f2.write(header)
    
    print(f"Salvataggio batch corrente in: {batch_filename}")
    print("Acquisizione in corso...")

    def on_message_handler(message) -> None:
        if time.time() > end_time_limit:
            client.stop()
            return

        try:
            commit = parse_subscribe_repos_message(message)
            if not hasattr(commit, 'repo'):
                return

            user_did = commit.repo
            
            # Logica: Se non l'ho mai visto prima (nello storico)
            if user_did and user_did not in unique_users:
                unique_users.add(user_did)
                session_users.append(user_did)
                
                # Scriviamo
                f1.write(f"{user_did}\n")
                f2.write(f"{user_did}\n")
                
                if len(session_users) % 50 == 0:
                    f1.flush()
                    f2.flush()
                    elapsed = (time.time() - start_time) / 60
                    # Feedback a video
                    sys.stdout.write(f"\rMins: {elapsed:.1f}/{DURATION_MINUTES} | New Users: {len(session_users)}")
                    sys.stdout.flush()

        except Exception as e:
            print(f"Error: {e}")

    client = FirehoseSubscribeReposClient()
    
    try:
        client.start(on_message_handler)
    except KeyboardInterrupt:
        print("\nStop manuale ricevuto...")
    finally:
        # 6. Scrittura FOOTER Corretto
        end_time = time.time()
        end_dt_str = datetime.datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
        actual_duration = (end_time - start_time) / 60
        
        footer = (
            f"# ---------------------------------------------------\n"
            f"# SESSION END: {end_dt_str}\n"
            f"# ACTUAL DURATION: {actual_duration:.2f} min\n"
            f"# NEW USERS (THIS SESSION): {len(session_users)}\n"
            f"# TOTAL UNIQUE USERS (DB): {len(unique_users)}\n" 
        )
        
        f1.write(footer)
        f2.write(footer)
        
        f1.close()
        f2.close()
        
        print("\n" + "="*50)
        print(f"COMPLETATO.")
        print(f"Storico aggiornato: {FILE_TIMELINES}")
        print(f"Batch salvato come: {batch_filename}")
        print("="*50)

if __name__ == '__main__':
    main()