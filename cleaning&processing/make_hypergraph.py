import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm

# --- CONFIGURATION ---
BASE_DEFAULT = 'results/clean'
OUT_DEFAULT = 'results/hypergraph.csv.gz'
# ---------------------

def load_enc_users():
    res = dict()
    # WINDOWS MODIFICATION: Correct path and utf-8 encoding
    map_file = 'results/enc_users.txt'
    
    if not os.path.exists(map_file):
        print(f"Error: {map_file} not found.")
        return res

    with open(map_file, 'r', encoding='utf-8') as f:
        while line := f.readline():
            parts = line.rstrip().split()
            if len(parts) >= 2:
                i, k = parts[0], parts[1]
                res[k] = int(i)
    return res

def gzip_iterator(BASE):
    if not os.path.exists(BASE):
        return

    # Original logic to find files
    files = sorted(os.listdir(BASE))
    # Small fix to sort numerically if possible, otherwise alphabetical
    try:
        files.sort(key=lambda x: int(x.split('.')[0]))
    except:
        pass

    for f in files:
        if f.endswith('.gz'):
            full_path = os.path.join(BASE, f)
            print(f'processing {full_path}...')
            yield full_path

if __name__ == '__main__':
    
    start = datetime.datetime.now()
    
    BASE = BASE_DEFAULT
    OUT = OUT_DEFAULT
    
    # Command line arguments (original)
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]
    
    # Load user map (Restored for fidelity)
    print("Loading user map...")
    user_map = load_enc_users()
    print(f"Loaded {len(user_map)} users.")

    print(f"Reading from {BASE} and writing to {OUT}")
    
    # Line counter
    count = 0

    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        
        for path in gzip_iterator(BASE):
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    try:
                        d = json.loads(line)
                    except:
                        continue
                    
                    # Extract fields like in the original code
                    user_id = d.get('user_id')
                    thread_root = d.get('thread_root')
                    date = d.get('date')
                    
                    # Original logic: if there is a root and a user, it's a hypergraph interaction
                    if user_id is not None and thread_root is not None:
                        # Date format YYYYMMDD
                        date_str = str(date)[:8] if date else "00000000"
                        
                        # Write: THREAD_ID, PARTICIPANT, DATE
                        outf.write(f"{thread_root},{user_id},{date_str}\n")
                        count += 1
    
    print(f"Done in {datetime.datetime.now() - start}")
    print(f"Wrote {count} lines to {OUT}")

    # WINDOWS MODIFICATION: Commented out because 'sort -u' does not work on Windows
    # print("Sorting and removing duplicates...")
    # os.system(f"sort -u {OUT} -o {OUT}")
    # print("Done sorting.")