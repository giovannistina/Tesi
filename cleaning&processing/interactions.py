import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
from datetime import datetime

# --- CONFIGURATION ---
BASE_DEFAULT = 'results/clean'
OUT_DEFAULT = 'results/interactions.csv.gz'
# ---------------------

def gzip_iterator(BASE):
    """Iterates through cleaned .jsonl.gz files."""
    if not os.path.exists(BASE):
        print(f"Error: {BASE} does not exist.")
        return

    # Filter and sort files numerically
    files = [f for f in os.listdir(BASE) if f.endswith('.gz')]
    try:
        files.sort(key=lambda x: int(x.split('.')[0]))
    except ValueError:
        files.sort()

    for f in files:
        full_path = os.path.join(BASE, f)
        print(f'Processing {full_path}...')
        yield full_path

if __name__ == '__main__':
    
    start = datetime.now()
    
    BASE = BASE_DEFAULT
    OUT = OUT_DEFAULT

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    # Ensure output directory exists
    out_dir = os.path.dirname(OUT)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    badlines = 0
    print(f'Reading cleaned files from: {BASE}')
    print(f'Saving interactions to: {OUT}')
    
    with gzip.open(OUT, 'a') as outf:

        for path in gzip_iterator(BASE):
            with gzip.open(path, 'rb') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    try:
                        post = json.loads(line.decode('utf-8').strip())
                    except Exception:
                        badlines += 1
                        continue
                    
                    # 1. Source User
                    u = post.get('user_id')
                    if u is None: continue
                   
                    # 2. Target Users
                    replied_author = post.get('replied_author') 
                    thread_root_author = post.get('thread_root_author')
                    reposted_author = post.get('reposted_author') 
                    quoted_author = post.get('quoted_author') 

                    # 3. Write Interaction if exists
                    if replied_author or thread_root_author or reposted_author or quoted_author:
                        t = post.get('date', 0)
                        # Format: Source, ReplyTo, ThreadRoot, RepostOf, QuoteOf, Date
                        row = f"{u},{replied_author},{thread_root_author},{reposted_author},{quoted_author},{t}\n"
                        outf.write(row.encode('utf-8'))

    print(f'Done. Took {datetime.now() - start}')
    print(f'Bad lines skipped: {badlines}')