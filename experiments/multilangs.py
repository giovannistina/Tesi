import gzip
import os
import json
import sys
from tqdm import tqdm
import time

# --- CONFIGURATION ---
# Use 'clean' data to analyze all posts immediately
BASE_DEFAULT = '../cleaning&processing/results/clean'
OUT_DEFAULT = 'results/multilangs.txt.gz'
# ---------------------

def gzip_iterator(BASE):
    if not os.path.exists(BASE): 
        print(f"Error: Directory {BASE} not found.")
        return
    # Sort files numerically
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

    # Command line arguments
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    print(f'Processing files in {BASE} -> {OUT}')
    
    # Create output directory
    if not os.path.exists(os.path.dirname(OUT)):
        os.makedirs(os.path.dirname(OUT))
    
    # WINDOWS FIX: encoding='utf-8' and mode='wt'
    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        
        for path in gzip_iterator(BASE):
            # WINDOWS FIX: encoding='utf-8' and mode='rt'
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    try:
                        d = json.loads(line.strip())
                        langs = d.get('langs')
                        
                        # Check if multiple languages are present
                        if langs and len(langs) > 1:
                            # Clean and sort languages
                            langs = [str(l) if l else 'none' for l in langs]
                            row = ' '.join(sorted(langs))
                            outf.write(f"{row}\n")
                    except: continue

    tock = time.time()
    print(f'done. {int(tock-tick)} s')