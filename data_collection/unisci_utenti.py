# Tesi/data_collection/unisci_utenti.py
# unisce i file 1.txt 2.txt, 3.txt, 4.txt in modo da avere un solo fine finale con tutti gli utenti chiamato "all_users.txt"


import os

# --- CONFIGURAZIONE PERCORSI ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "all_users.txt")

def unisci_files():
    print(f"--- UNIONE FILE 1.txt ... 4.txt ---")
    print(f"📂 Cartella dati: {DATA_DIR}")
    
    # Contatore per statistica
    total_users = 0

    # Apre il file di output in scrittura (sovrascrive se esiste)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        
        # Cicla i file da 1 a 4
        for i in range(11, 15):
            filename = f"{i}.txt"
            filepath = os.path.join(DATA_DIR, filename)
            
            if os.path.exists(filepath):
                print(f"🔄 Processando {filename}...")
                
                count_local = 0
                with open(filepath, 'r', encoding='utf-8') as infile:
                    for line in infile:
                        # Scrive la riga nel file finale
                        outfile.write(line)
                        count_local += 1
                
                # Assicuriamoci che ci sia un accapo tra un file e l'altro
                # (per evitare che l'ultima riga di 1.txt si attacchi alla prima di 2.txt)
                # Nota: outfile.write('\n') qui potrebbe aggiungere righe vuote extra se i file 
                # hanno già l'accapo finale, ma è più sicuro che rischiare di unire due user.
                
                total_users += count_local
                print(f"   -> Aggiunti {count_local} utenti.")
            else:
                print(f"⚠️  ATTENZIONE: {filename} non trovato, salto.")

    print(f"\n✅ Completato! File creato: {OUTPUT_FILE}")
    print(f"📊 Totale righe scritte: {total_users}")

if __name__ == "__main__":
    unisci_files()


    