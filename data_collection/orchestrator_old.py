# Tesi/data_collection/orchestrator_old.py

import subprocess
import time
import sys
import datetime
import os

# --- CONFIGURAZIONE CICLI ---
MINUTI_LAVORO = 0.2       # Quanto dura la raccolta
MINUTI_PAUSA = 10        # Quanto aspetta tra una raccolta e l'altra
DURATA_TOTALE_ORE = 0.05 # Copertura totale in ore
DATA_FOLDER = "data"    # Cartella di output
DB_FILE = "1.txt"       # Nome del file database principale
# ----------------------------

def get_db_count(folder, filename):
    """Conta le righe di utenti nel file storico, ignorando i commenti."""
    path = os.path.join(folder, filename)
    if not os.path.exists(path):
        return 0
    
    count = 0
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Conta solo se c'è testo e NON è un commento
                if line and not line.startswith("#"):
                    count += 1
    except Exception:
        return 0
    return count

def add_to_log(log_list, message):
    """Aggiunge un messaggio con timestamp alla lista in memoria."""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_list.append(f"[{timestamp}] {message}")

def main():
    print(f"--- ORCHESTRATORE AUTOMATICO (Con conteggio utenti) ---")
    
    # 1. Setup Cartella
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
        
    session_start_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"session_log_{session_start_str}.txt"
    log_path = os.path.join(DATA_FOLDER, log_filename)

    session_log_buffer = []

    # 2. Header Log
    header = (
        f"--- SESSIONE DI RACCOLTA DATI ---\n"
        f"DATA AVVIO: {session_start_str}\n"
        f"IMPOSTAZIONI:\n"
        f"- Durata Raccolta: {MINUTI_LAVORO} min\n"
        f"- Durata Pausa:    {MINUTI_PAUSA} min\n"
        f"- Target Totale:   {DURATA_TOTALE_ORE} ore\n"
        f"-----------------------------------"
    )
    session_log_buffer.append(header)
    
    start_time_global = time.time()
    end_time_global = start_time_global + (DURATA_TOTALE_ORE * 3600)
    cycle_count = 1

    # Conteggio iniziale (prima di partire)
    initial_total_users = get_db_count(DATA_FOLDER, DB_FILE)
    add_to_log(session_log_buffer, f"START: Utenti già presenti nel DB: {initial_total_users}")

    try:
        while time.time() < end_time_global:
            current_time = datetime.datetime.now().strftime("%H:%M:%S")
            
            # Conta utenti PRIMA del ciclo
            count_before = get_db_count(DATA_FOLDER, DB_FILE)
            
            msg_start = f"CICLO {cycle_count}: Avvio raccolta ({MINUTI_LAVORO} min)..."
            print(f"\n[{current_time}] {msg_start}")
            add_to_log(session_log_buffer, msg_start)
            
            # 3. ESECUZIONE SCRIPT
            try:
                subprocess.run([sys.executable, "ricerca_real_time.py", str(MINUTI_LAVORO)], check=True)
                
                # Conta utenti DOPO il ciclo
                count_after = get_db_count(DATA_FOLDER, DB_FILE)
                new_users_found = count_after - count_before
                
                success_msg = (
                    f"CICLO {cycle_count}: Terminato. "
                    f"Nuovi utenti: +{new_users_found} | Totale DB: {count_after}"
                )
                add_to_log(session_log_buffer, success_msg)
                
            except subprocess.CalledProcessError as e:
                err_msg = f"ERRORE CRITICO ciclo {cycle_count}: {e}"
                print(err_msg)
                add_to_log(session_log_buffer, err_msg)

            # Check Stop
            if time.time() + (MINUTI_PAUSA * 60) > end_time_global:
                stop_msg = "Tempo totale esaurito. Stop sequenza."
                print(stop_msg)
                add_to_log(session_log_buffer, stop_msg)
                break
                
            # 4. PAUSA
            next_run = datetime.datetime.now() + datetime.timedelta(minutes=MINUTI_PAUSA)
            pause_msg = f"CICLO {cycle_count}: Pausa {MINUTI_PAUSA} min. Restart: {next_run.strftime('%H:%M:%S')}"
            print(f"{pause_msg}")
            add_to_log(session_log_buffer, pause_msg)
            
            time.sleep(MINUTI_PAUSA * 60)
            cycle_count += 1

        final_count = get_db_count(DATA_FOLDER, DB_FILE)
        total_session_gain = final_count - initial_total_users
        
        summary = (
            f"--- RIEPILOGO SESSIONE ---\n"
            f"Utenti inizio sessione: {initial_total_users}\n"
            f"Utenti fine sessione:   {final_count}\n"
            f"Totale raccolti oggi:   +{total_session_gain}\n"
            f"--- FINE PROGRAMMA ---"
        )
        add_to_log(session_log_buffer, summary)
        print("\n--- SESSIONE COMPLETATA ---")

    except KeyboardInterrupt:
        print("\n!!! INTERRUZIONE MANUALE !!!")
        add_to_log(session_log_buffer, "!!! INTERRUZIONE MANUALE (Ctrl+C) !!!")
        
    finally:
        # 5. SALVATAGGIO LOG
        print(f"\nSalvataggio log in: {log_path} ...")
        try:
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("\n".join(session_log_buffer))
            print("Log salvato.")
        except Exception as e:
            print(f"ERRORE salvataggio log: {e}")

if __name__ == "__main__":
    main()