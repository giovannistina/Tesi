import gzip
import os
import json
import sys
from tqdm import tqdm
from collections import defaultdict
import time

# --- CONFIGURATION ---
# Path relative to 'experiments' folder
BASE_DEFAULT = '../cleaning&processing/results/clean'
OUT_DEFAULT = 'results/all_langs.txt.gz'
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
    result = defaultdict(int)

    # Command line arguments
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    print(f'Processing files in {BASE} -> {OUT}')
    
    # Create output directory if it doesn't exist
    if not os.path.exists(os.path.dirname(OUT)):
        os.makedirs(os.path.dirname(OUT))
    
    for path in gzip_iterator(BASE):
        # WINDOWS FIX: Use 'rt' (read text) and utf-8 encoding
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                try:
                    d = json.loads(line.strip())
                    langs = d.get('langs')
                    if langs:
                        for lang in langs:
                            if lang:
                                result[lang] += 1
                except: continue
                    
    # Write results sorted by frequency
    # WINDOWS FIX: Use 'wt' (write text) and utf-8 encoding
    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        for k, v in sorted(result.items(), key=lambda x: x[1], reverse=True):
            outf.write(f"{k} {v}\n")

    tock = time.time()
    print(f'done. {int(tock-tick)} s')