import gzip
import os
import datetime
import json
import sys
from tqdm import tqdm

# --- CONFIGURAZIONE ---
BASE = 'results/clean'
OUT = 'results/textdata.jsonl.gz'
# ----------------------

def valid_time(t):
    try:
        # Adattamento per i tuoi dati: converte int YYYYMMDDHHMM in stringa YYYYMMDD
        d_str = str(t)[:8] 
        T = datetime.datetime.strptime(d_str, '%Y%m%d')
        
        # Filtro date (Sbloccato per 2025)
        if T.date() < datetime.datetime(2023, 2, 17).date():
            return False
        return True
    except: return False

def gzip_iterator(BASE):
    if not os.path.exists(BASE): return
    # Ordine numerico per processare i file in sequenza
    files = sorted([f for f in os.listdir(BASE) if f.endswith('.gz')], 
                   key=lambda x: int(x.split('.')[0]) if x[0].isdigit() else x)
    for f in files:
        yield os.path.join(BASE, f)
            
if __name__ == '__main__':
    start = datetime.datetime.now()
    
    # Argomenti CLI
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    bad_lines = 0
    good_lines = 0
    
    print(f'Processing {BASE} -> {OUT}')
    
    # Crea cartella output se non esiste
    if not os.path.exists(os.path.dirname(OUT)): os.makedirs(os.path.dirname(OUT))

    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        for path in gzip_iterator(BASE):
            try:
                # Prova a estrarre ID file dal nome
                file_id = int(os.path.basename(path).split('.')[0])
            except: file_id = 0

            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    try:
                        data = json.loads(line.rstrip())
                        
                        # Controllo Data
                        if not valid_time(data.get('date')): continue

                        # Controllo Lingua (Accetta 'en' o 'eng')
                        langs = data.get('langs')
                        # Verifica se c'è almeno un tag inglese nella lista
                        is_eng = False
                        if langs and isinstance(langs, list):
                            if 'en' in langs or 'eng' in langs: is_eng = True
                        
                        if is_eng:
                            text = data.get('text', '')
                            if text:
                                # Pulizia testo per CSV/JSONL
                                text = text.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                                
                                row = json.dumps({
                                    'file_id': file_id, 
                                    'post_id': data['post_id'],
                                    'date': str(data.get('date'))[:8],
                                    'text': text
                                }) + '\n'
                                outf.write(row)
                                good_lines += 1
                             
                    except Exception:
                        bad_lines += 1
            
    print(f'Done in {datetime.datetime.now() - start}')
    print(f'Bad lines: {bad_lines}')
    print(f'Good lines (English): {good_lines}')