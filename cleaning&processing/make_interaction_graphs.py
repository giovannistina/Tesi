import gzip
import os

# --- CONFIGURATION ---
INPUT_FILE = 'results/interactions.csv.gz'
OUT_REPLIES = 'results/replies.csv.gz'
OUT_REPOSTS = 'results/reposts.csv.gz'
OUT_QUOTES  = 'results/quotes.csv.gz'
# ---------------------

if __name__ == '__main__':
    
    print(f"Reading from {INPUT_FILE}...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Run interactions.py first.")
        exit(1)

    # Apre il file di input in lettura
    with gzip.open(INPUT_FILE, 'rb') as f:
        # Apre i 3 file di output in scrittura contemporaneamente
        with gzip.open(OUT_REPLIES, 'w') as f_replies:
            with gzip.open(OUT_REPOSTS, 'w') as f_reposts:
                with gzip.open(OUT_QUOTES, 'w') as f_quotes:
                    
                    count = 0
                    for line in f:
                        count += 1
                        line = line.decode('utf-8').rstrip()
                        
                        # Divide la riga CSV
                        fields = line.split(',')
                        
                        # Pulisce i campi (gestisce 'None' come stringa e converte numeri)
                        clean_fields = []
                        for field in fields:
                            if field and field != 'None' and field.isdigit():
                                clean_fields.append(int(field))
                            else:
                                clean_fields.append(None)
                        
                        # Assicuriamoci di avere i 6 campi previsti
                        if len(clean_fields) < 6: continue
                        
                        # Estrae i dati: Chi (author), A chi (reply, repost, quote), Quando (date)
                        author, reply, root, repost, quote, date = clean_fields
                        
                        # Formatta la data (YYYYMMDD) per risparmiare spazio
                        date_str = str(date)[:8] if date else "00000000"
                        
                        # 1. GRAFO RISPOSTE (Replies)
                        if reply is not None:
                            # Scrive: Autore, Destinatario, Data
                            f_replies.write(f'{author},{reply},{date_str}\n'.encode('utf-8'))
                        
                        # 2. GRAFO REPOST (Viralità)
                        if repost is not None:
                            f_reposts.write(f'{author},{repost},{date_str}\n'.encode('utf-8'))
                        
                        # 3. GRAFO CITAZIONI (Quotes)
                        if quote is not None:
                            f_quotes.write(f'{author},{quote},{date_str}\n'.encode('utf-8'))

    print(f"Done! Processed {count} interaction lines.")
    print(f"Created graph files in 'results/':")
    print(f" -> replies.csv.gz")
    print(f" -> reposts.csv.gz")
    print(f" -> quotes.csv.gz")