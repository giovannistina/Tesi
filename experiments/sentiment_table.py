import gzip
import os
import json
import sys
from tqdm import tqdm
import time
from collections import defaultdict

# --- CONFIGURATION ---
# Input: The final posts with sentiment added
# (Points to the sibling folder where add_sentiment.py saved the data)
BASE_DEFAULT = '../cleaning&processing/results/final_posts'
OUT_DEFAULT = 'results/sentiment_table.csv.gz'
# ---------------------

def gzip_iterator(BASE):
    if not os.path.exists(BASE): 
        print(f"Error: Directory {BASE} not found.")
        return
    # Sort numerically
    files = sorted([f for f in os.listdir(BASE) if f.endswith('.gz')], 
                   key=lambda x: int(x.split('.')[0]) if x[0].isdigit() else x)
    for f in files:
        f_path = os.path.join(BASE, f)
        print(f'processing {f_path}...')
        yield f_path

if __name__ == '__main__':
    
    tick = time.time()
    BASE = BASE_DEFAULT
    OUT = OUT_DEFAULT

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]
    
    bad_lines = 0
    with_sentiment = 0
    
    # Dictionary structure: {date_int: [list of sentiment labels]}
    result = defaultdict(list)
    
    print(f'Processing files in {BASE} -> {OUT}')
    
    if not os.path.exists(os.path.dirname(OUT)):
        os.makedirs(os.path.dirname(OUT))

    for path in gzip_iterator(BASE):
        # WINDOWS FIX: encoding='utf-8' and mode='rt'
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                try:
                    d = json.loads(line.strip())
                    
                    # Date Handling (fix for int/string mismatch)
                    date_raw = str(d.get('date'))
                    if len(date_raw) >= 8:
                        date_int = int(date_raw[:8]) # YYYYMMDD
                    else:
                        continue

                    # Sentiment Handling
                    sentiment = d.get('sent_label')
                    
                    # If sentiment is None (not analyzed), skip
                    if sentiment is None:
                        continue
                        
                    with_sentiment += 1
                    result[date_int].append(sentiment)
                    
                except Exception as e:
                    bad_lines += 1
                    continue

    # Create CSV: date, positive, negative, neutral, total
    print("Writing output CSV...")
    # WINDOWS FIX: encoding='utf-8' and mode='wt'
    with gzip.open(OUT, 'wt', encoding='utf-8') as out:
        out.write('date,positive,negative,neutral,total\n')
        
        for date in sorted(result.keys()):
            # Mapping from sentiment.py: 2=positive, 0=negative, 1=neutral
            pos = result[date].count(2)
            neg = result[date].count(0)
            neu = result[date].count(1)
            total = pos + neg + neu
            
            row = f"{date},{pos},{neg},{neu},{total}\n"
            out.write(row)

    tock = time.time()
    print(f'Done. {int(tock-tick)} s')
    print(f'Bad lines: {bad_lines}')
    print(f'Posts with sentiment used: {with_sentiment}')