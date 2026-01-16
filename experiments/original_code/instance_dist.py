import gzip
import os
import json
import sys
from tqdm import tqdm
from collections import defaultdict, Counter
import time

# --- CONFIGURATION ---
BASE_DEFAULT = '../cleaning&processing/results/clean'
OUT_DEFAULT = 'results'
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

    user_to_instance = dict()
    instance_n_posts = defaultdict(int)

    print(f'Processing files in {BASE} -> {OUT}')
    
    # Create output directory
    if not os.path.exists(OUT):
        os.makedirs(OUT)
    
    for path in gzip_iterator(BASE):
        # WINDOWS FIX: encoding='utf-8' and mode='rt'
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                try:
                    d = json.loads(line.strip())
                    user = d.get('user_id')
                    instance = d.get('instance')
                    
                    if instance:
                        instance_n_posts[instance] += 1
                        if user is not None and user not in user_to_instance:
                            user_to_instance[user] = instance
                except: continue

    # Count users per instance
    result = list(user_to_instance.values())
    result = dict(Counter(result))

    # Save Instance -> User Count
    path_users = os.path.join(OUT, 'instance_users.csv.gz')
    # WINDOWS FIX: encoding='utf-8' and mode='wt'
    with gzip.open(path_users, 'wt', encoding='utf-8') as outf:
        for k, v in sorted(result.items(), key=lambda x: x[1], reverse=True):
            outf.write(f"{k},{v}\n")

    # Save Instance -> Post Count
    path_posts = os.path.join(OUT, 'instance_posts.csv.gz')
    with gzip.open(path_posts, 'wt', encoding='utf-8') as outf:
        for k, v in sorted(instance_n_posts.items(), key=lambda x: x[1], reverse=True):
            outf.write(f"{k},{v}\n")
            
    tock = time.time()
    print(f'done. {int(tock-tick)} s')