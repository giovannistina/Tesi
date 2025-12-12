import gzip
import os
import json
import sys
from tqdm import tqdm
import time

# --- CONFIGURATION ---
# Path to your clean data (Relative to 'experiments' folder)
BASE_DEFAULT = '../cleaning&processing/results/clean'
# Output file (will be saved inside experiments/results)
OUT_DEFAULT = 'results/post_stats.txt.gz'
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

    # Command line arguments override
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    print(f'Processing files in {BASE} -> {OUT}')
    
    # Create local results folder if it doesn't exist
    if not os.path.exists(os.path.dirname(OUT)):
        os.makedirs(os.path.dirname(OUT))
    
    # WINDOWS FIX: encoding='utf-8'
    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        for path in gzip_iterator(BASE):
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    try:
                        d = json.loads(line.strip())
                        post_id = d.get('post_id')
                        user_id = d.get('user_id')
                        
                        # DATE FIX: Convert to string first
                        t = str(d.get('date'))
                        if len(t) >= 8:
                            t_day = t[:8] # YYYYMMDD
                            
                            if post_id is not None and user_id is not None:
                                outf.write(f"{t_day} {post_id} {user_id}\n")
                    except: continue
                    
    tock = time.time()
    print(f'Done. {int(tock-tick)} s')