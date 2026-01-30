# Tesi/data_collection/cleaner.py
# lo lancio per controllare che il file n.txt non contenga nomi utenti presenti già negli altri n.txt. in questo modo non perdo tempo con i bot che ho già salvato
# devo lanciare così: ######## python cleaner.py n.txt ########


import sys
import os

def clean_file(target_file):
    # 1. Deduce il numero del file corrente (es. "3.txt" -> 3)
    try:
        base_name = os.path.basename(target_file)
        current_num = int(base_name.split('.')[0])
    except ValueError:
        print(f"ERRORE: Il file '{target_file}' non segue il formato 'numero.txt' (es. 1.txt, 2.txt).")
        sys.exit(1)

    # 2. Identifica i file precedenti (da 1 a N-1)
    previous_files = [f"{i}.txt" for i in range(1, current_num)]
    
    if not previous_files:
        print(f"Nessun file precedente da controllare per {target_file}. Nessuna pulizia necessaria.")
        return

    print(f"Target: {target_file}. Controllo duplicati rispetto a: {previous_files}")

    # 3. Carica la memoria dei file precedenti
    seen_users = set()
    for p_file in previous_files:
        if os.path.exists(p_file):
            with open(p_file, 'r') as f:
                seen_users.update(line.strip() for line in f if line.strip())
        else:
            print(f"Avviso: Il file precedente {p_file} non esiste. Verrà ignorato.")

    # 4. Legge il target, filtra e sovrascrive
    if not os.path.exists(target_file):
        print(f"ERRORE: Il file target {target_file} non esiste.")
        sys.exit(1)

    with open(target_file, 'r') as f:
        target_users = [line.strip() for line in f if line.strip()]

    unique_users = [u for u in target_users if u not in seen_users]

    with open(target_file, 'w') as f:
        for u in unique_users:
            f.write(f"{u}\n")

    print(f"OPERAZIONE COMPLETATA: {target_file} sovrascritto.")
    print(f"Utenti totali: {len(target_users)} -> Utenti univoci salvati: {len(unique_users)}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python cleaner.py <file_da_pulire.txt>")
        sys.exit(1)
    
    clean_file(sys.argv[1])