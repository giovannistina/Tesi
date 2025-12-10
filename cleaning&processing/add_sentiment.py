import gzip
import os
import json
import sys
import time
from tqdm import tqdm
from collections import defaultdict

# Configurazione
BASE_DIR = 'results/clean'
OUT_DIR = 'results/final_posts'
SENT_FILE = 'results/sentiment.jsonl.gz'

def gzip_iterator(BASE):
    # Itera sui file ordinati alfabeticamente (come l'originale)
    for f in sorted(os.listdir(BASE)):
        f_path = os.path.join(BASE, f)
        if f.endswith('.gz'):   
            yield f_path

def sentiment_dict():
    res = defaultdict(dict)
    # Aggiunto encoding utf-8
    if not os.path.exists(SENT_FILE): return res
    with gzip.open(SENT_FILE, 'rt', encoding='utf-8') as f:
        for line in tqdm(f, desc="Loading Sentiment"):
            try:
                line = json.loads(line.strip())
            except: continue
            
            file_id = line.get('file_id')
            post_id = line.get('post_id')
            # Salva label e score
            res[file_id][post_id] = (line.get('sent_label'), line.get('sent_score'))
    return res

if __name__ == '__main__':
    
    tick = time.time()
    BASE = BASE_DIR
    OUT = OUT_DIR

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    sent_dict = sentiment_dict()
    print('Sentiment dict loaded.')
    
    if not os.path.exists(OUT):
        os.makedirs(OUT)
        print('Created', OUT)
    
    bad_lines = 0
    with_sentiment = 0
    print('Processing files in', BASE, 'and saving to', OUT)
    
    for path in gzip_iterator(BASE):
        joined = os.path.join(OUT, os.path.basename(path))
        
        # Estrae ID file dal nome (es. 0.jsonl.gz -> 0)
        try: file_id = int(os.path.basename(path).split('.')[0])
        except: file_id = 0
        
        sentiment = sent_dict.get(file_id, {})

        # Scrittura: encoding utf-8
        with gzip.open(joined, 'wt', encoding='utf-8') as out:
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"File {file_id}"):
                    try:
                        d = json.loads(line.strip())
                    except:
                        bad_lines += 1
                        continue
                    
                    post_id = d.get('post_id')
                    
                    # Logica originale fedele: controlla se il post ha sentiment
                    if post_id in sentiment:
                        d['sent_label'] = sentiment[post_id][0]
                        d['sent_score'] = sentiment[post_id][1]
                        with_sentiment += 1
                    else:
                        d['sent_label'] = None
                        d['sent_score'] = None
                    
                    row = json.dumps(d) + '\n'
                    out.write(row)
                    
    tock = time.time()
    print('Done.', int(tock-tick), 's')
    print('Bad lines:', bad_lines)
    print('With sentiment:', with_sentiment)