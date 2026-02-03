# /Tesi/data_collection/mescola_utenti.py
# è utile prima del crawl 6 mesi per parallelizzare meglioimport pandas as pd




import pandas as pd
import os

# Percorsi
input_txt = '/home/students/s332085/Tesi/data_collection/data/lista_did_30k.txt'
output_dir = '/home/students/s332085/Tesi/data_collection/data/'

# 1. Carica solo la lista dei DID
if not os.path.exists(input_txt):
    print(f"❌ Errore: Il file {input_txt} non esiste.")
else:
    # Leggiamo il file come un DataFrame a colonna singola
    df = pd.read_csv(input_txt, header=None, names=['did'])

    # 2. Mescolamento casuale iniziale
    # Fondamentale per rompere l'ordine Low/Medium/High del filtro
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)

    print(f"🚀 Inizio distribuzione Round Robin su 4 chunk per {len(df)} utenti...")

    # 3. Distribuzione intercalata (Pettine) su 4 file
    for i in range(4):
        # Logica 'uno ogni 4': parte dall'indice i e salta di 4 in 4
        chunk = df.iloc[i::4]
        
        output_path = os.path.join(output_dir, f'chunk_{i}.txt')
        
        # Salvataggio solo della colonna DID senza intestazione
        chunk.to_csv(output_path, index=False, header=False)
        
        print(f"✅ Chunk {i} creato: {len(chunk)} utenti salvati in {output_path}")

    print("\n🎉 Distribuzione completata. I 4 file sono pronti per la parallelizzazione.")