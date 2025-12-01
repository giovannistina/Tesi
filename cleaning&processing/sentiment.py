import gzip
import datetime
import sys
from tqdm import tqdm
import json

import torch

from transformers import pipeline

MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
#MODEL = f"austinmw/distilbert-base-uncased-finetuned-tweets-sentiment"
#MODEL = "finiteautomata/bertweet-base-sentiment-analysis"

START_AT = 0 # Start at this row in the file 

def load_model(model_name, device):
    
    pipe = pipeline("sentiment-analysis", model=model_name, tokenizer=model_name, 
                    max_length=500, truncation=True, device=device, batch_size=32)
    return pipe


def preprocess(text):
    new_text = []
    for t in text.split():
        t = '@user' if t.startswith('@') and len(t) > 1 else t
        t = 'http' if t.startswith('http') else t
        new_text.append(t)
    return " ".join(new_text)

def save_batch(batch, pipe, outf):
    texts = [preprocess(d['text']) for d in batch]
    outputs = pipe(texts)
    for d, o in zip(batch, outputs):
        d['sent_label'] = mapping[o['label']]
        d['sent_score'] = round(o['score'], 3) 
        row = json.dumps(d) + '\n'
        outf.write(row.encode('utf-8'))
    return []



if __name__ == '__main__':
    
    start = datetime.datetime.now()
    
    BASE = 'textdata.jsonl.gz'
    OUT = 'sentiment.jsonl.gz'
    read_n = 50000
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]
        if sys.argv[i] == '-s':
            read_n = int(sys.argv[i+1])


    # check if uses gpu
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print('using', device)
    print('reading', read_n, 'lines at a time')

    print('processing', BASE, 'and saving to', OUT)
    pipe = load_model(MODEL, device) 
    
    mapping = {
        'positive': 2,
        'negative': 0,
        'neutral': 1
    }
    
    with gzip.open(OUT, 'a') as outf:
        with gzip.open(BASE) as f:
            batch = []
            for i, line in enumerate(tqdm(f)):
                if i < START_AT:
                    continue
                data = json.loads(line.rstrip())
                batch.append(data)
                if len(batch) == read_n:
                    batch = save_batch(batch, pipe, outf)

            # check if there are any left
            if batch:
                batch = save_batch(batch, pipe, outf)
 
            
    elapsed = datetime.datetime.now() - start
    print('done, took', elapsed)
    