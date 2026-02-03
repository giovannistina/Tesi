# Tesi/data_collection/cleaner.py
# python cleaner.py 12.txt

import sys
import os

# Configurazione cartella dati
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

# !!! MODIFICA QUI !!!
# Il numero del primo file da considerare come "storico".
# I file prima di questo numero (es. 1.txt ... 10.txt) verranno IGNORATI.
START_FROM_CHUNK = 11 

def clean_file(filename_arg):
    # Gestisce sia se passi "12.txt" sia se passi "data/12.txt"
    filename_only = os.path.basename(filename_arg) 
    target_path = os.path.join(DATA_DIR, filename_only)

    # 1. Deduce il numero
    try:
        current_num = int(filename_only.split('.')[0])
    except ValueError:
        print(f"ERRORE: '{filename_only}' non è nel formato 'numero.txt'.")
        sys.exit(1)

    # 2. Identifica file precedenti (MODIFICATO: parte da START_FROM_CHUNK)
    # Se current_num è 11, range(11, 11) è vuoto -> Corretto, perché 11 è il primo.
    # Se current_num è 12, range(11, 12) cerca solo 11.txt.
    previous_files = [os.path.join(DATA_DIR, f"{i}.txt") for i in range(START_FROM_CHUNK, current_num)]
    
    if not previous_files:
        print(f"Nessun file precedente trovato (nel range {START_FROM_CHUNK}-{current_num-1}). Nessuna pulizia necessaria.")
        return

    print(f"Target: {filename_only}")
    print(f"Confronto con: {[os.path.basename(f) for f in previous_files]}")

    # 3. Carica memoria storici (ignora le righe che iniziano con #)
    seen_users = set()
    for p_file in previous_files:
        if os.path.exists(p_file):
            with open(p_file, 'r', encoding='utf-8') as f:
                for line in f:
                    l = line.strip()
                    if l and not l.startswith("#"):
                        seen_users.add(l)

    # 4. Lettura Target e Filtraggio (PRESERVANDO I COMMENTI)
    if not os.path.exists(target_path):
        print(f"ERRORE: {filename_only} non esiste in {DATA_DIR}")
        sys.exit(1)

    lines_to_write = []
    users_checked_count = 0
    removed_count = 0

    with open(target_path, 'r', encoding='utf-8') as f:
        for line in f:
            stripped = line.strip()
            
            # Se è vuota, saltiamo
            if not stripped:
                continue

            # CASO A: È un commento -> MANTENERE ASSOLUTAMENTE
            if stripped.startswith("#"):
                lines_to_write.append(stripped)
            
            # CASO B: È un utente -> CONTROLLARE
            else:
                users_checked_count += 1
                if stripped in seen_users:
                    # Duplicato: lo scartiamo
                    removed_count += 1
                else:
                    # Utente valido: lo teniamo
                    lines_to_write.append(stripped)

    # 5. Sovrascrittura file
    with open(target_path, 'w', encoding='utf-8') as f:
        for line in lines_to_write:
            f.write(f"{line}\n")
        
        # Aggiunta Report Finale
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"# ------------------------------------------------\n")
        f.write(f"# CHECK ESEGUITO IL: {timestamp}\n")
        f.write(f"# RANGE CONTROLLO: {START_FROM_CHUNK} -> {current_num-1}\n")
        f.write(f"# UTENTI ANALIZZATI: {users_checked_count}\n")
        f.write(f"# RIMOSSI DUPLICATI: {removed_count}\n")

    print(f"✅ FATTO. Rimossi {removed_count} duplicati.")
    print(f"Report aggiunto in coda a {filename_only}.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python cleaner.py <numero.txt>")
        sys.exit(1)
    
    clean_file(sys.argv[1])