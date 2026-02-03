import gzip
import json
import os

# Percorso al file
INPUT_FILE = os.path.expanduser("~/Tesi/data_collection/data/dataset_definitivo_6mesi.jsonl.gz")

def main():
    print(f"🔍 Ispezione Struttura Completa JSON in: {INPUT_FILE}")
    
    if not os.path.exists(INPUT_FILE):
        print("❌ File non trovato! Controlla il percorso.")
        return

    try:
        with gzip.open(INPUT_FILE, 'rt', encoding='utf-8') as f:
            found = False
            for i, line in enumerate(f):
                # Saltiamo eventuali righe vuote
                if not line.strip():
                    continue

                try:
                    data = json.loads(line)
                    
                    # Se il caricamento va a buon fine, stampiamo tutto il primo oggetto
                    print(f"\n✅ RECORD TROVATO alla riga {i+1}!")
                    
                    print("\n--- 1. Elenco Chiavi di primo livello ---")
                    print(list(data.keys()))
                    
                    print("\n--- 2. Struttura Completa (JSON Dump) ---")
                    # Stampiamo tutto per vedere dove sono annidati i dati (es. dentro 'record' o 'commit')
                    print(json.dumps(data, indent=4))
                    
                    found = True
                    break # Ci fermiamo subito dopo il primo, ci serve solo la struttura
                
                except json.JSONDecodeError:
                    print(f"⚠️ Errore di decodifica JSON alla riga {i+1}, provo la prossima...")
                    continue
            
            if not found:
                print("⚠️ Letto il file ma non ho trovato oggetti JSON validi.")

    except Exception as e:
        print(f"❌ Errore durante l'apertura del file: {e}")

if __name__ == "__main__":
    main()