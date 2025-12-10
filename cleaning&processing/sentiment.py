import gzip
import datetime
import sys
from tqdm import tqdm
import json
import os
import torch
from transformers import pipeline

# --- CONFIGURAZIONE ---
BASE_DEFAULT = 'results/textdata.jsonl.gz'
OUT_DEFAULT = 'results/sentiment.jsonl.gz'
MODEL = "cardiffnlp/twitter-roberta-base-sentiment-latest"
START_AT = 0 

# ### MODIFICA TEST: Metti un numero (es. 100) per testare. Metti None per l'analisi completa. ###
TEST_LIMIT = None 
# ##############################################################################################

def load_model(model_name, device):
    pipe = pipeline("sentiment-analysis", model=model_name, tokenizer=model_name, 
                    max_length=512, truncation=True, device=device, batch_size=32)
    return pipe

def preprocess(text):
    new_text = []
    if not text: return ""
    for t in text.split():
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

def save_batch(batch, pipe, outf):
    texts = [preprocess(d.get('text', '')) for d in batch]
    try:
        outputs = pipe(texts)
    except: return [] 
    
    for d, o in zip(batch, outputs):
        d['sent_label'] = mapping[o['label']]
        d['sent_score'] = round(o['score'], 3) 
        row = json.dumps(d) + '\n'
        outf.write(row.encode('utf-8'))
    return []

if __name__ == '__main__':
    
    start = datetime.datetime.now()
    
    BASE = BASE_DEFAULT
    OUT = OUT_DEFAULT
    read_n = 2000 

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]
        if sys.argv[i] == '-s': read_n = int(sys.argv[i+1])

    device = 0 if torch.cuda.is_available() else -1
    print(f'Using device: {"GPU" if device == 0 else "CPU"}')
    print(f'Processing {BASE} -> {OUT}')
    
    if TEST_LIMIT:
        print(f"⚠️  MODALITÀ TEST ATTIVA: Mi fermerò dopo {TEST_LIMIT} righe.")

    pipe = load_model(MODEL, device) 
    mapping = {'positive': 2, 'negative': 0, 'neutral': 1}
    
    if not os.path.exists(os.path.dirname(OUT)): os.makedirs(os.path.dirname(OUT))

    with gzip.open(OUT, 'a') as outf:
        with gzip.open(BASE, 'rt', encoding='utf-8') as f:
            batch = []
            for i, line in enumerate(tqdm(f)):
                
                # ### BLOCCO CONTROLLO TEST ###
                if TEST_LIMIT and i >= TEST_LIMIT:
                    print(f"\nLimite test raggiunto ({TEST_LIMIT}). Interruzione volontaria.")
                    break
                # #############################

                if i < START_AT: continue
                
                try:
                    data = json.loads(line.rstrip())
                    batch.append(data)
                except: continue
                
                if len(batch) == read_n:
                    batch = save_batch(batch, pipe, outf)

            if batch:
                batch = save_batch(batch, pipe, outf)
 
    print('done, took', datetime.datetime.now() - start)