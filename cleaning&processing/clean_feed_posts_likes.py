import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm

# --- CONFIGURATION ---
BASE_DEFAULT = '../data_collection/data'
OUT_DEFAULT = 'results/clean_feed_likes.csv.gz'
USER_MAP_FILE = 'results/enc_users.txt'
FEED_MAP_FILE = 'results/enc_feeds.txt'
# ---------------------

def load_map(path):
    res = dict()
    if not os.path.exists(path): return res
    # MODIFICA: encoding utf-8
    with open(path, encoding='utf-8') as f:
        while line := f.readline():
            parts = line.rstrip().split()
            if len(parts) >= 2:
                # Key is URI/DID, Value is ID
                res[parts[1]] = int(parts[0])
    return res

def gzip_iterator(BASE):
    if not os.path.exists(BASE): return
    files = sorted(os.listdir(BASE))
    try: files.sort(key=lambda x: int(x.split('.')[0]))
    except: pass

    for f in files:
        full_path = os.path.join(BASE, f)
        if os.path.isdir(full_path) and 'chunk' in f:
            for sub in sorted(os.listdir(full_path)):
                if sub.endswith('.gz'):
                    yield os.path.join(full_path, sub)
        elif f.endswith('.gz'):
            yield full_path

if __name__ == '__main__':
    
    start = datetime.datetime.now()
    BASE, OUT = BASE_DEFAULT, OUT_DEFAULT
    
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    print("Loading maps...")
    user_map = load_map(USER_MAP_FILE)
    feed_map = load_map(FEED_MAP_FILE)
    
    # --- VERBOSE COUNTERS ---
    total_lines = 0
    bad_lines = 0
    kept_likes = 0
    
    null_uri = 0 # Not used in original for likes, but good practice
    null_user = 0
    null_date = 0
    
    print(f"Processing Feed Likes from {BASE} -> {OUT}")
    
    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        # CSV Header
        outf.write("user_id,feed_id,date\n")

        for path in gzip_iterator(BASE):
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    total_lines += 1
                    try: d = json.loads(line)
                    except: 
                        bad_lines += 1
                        continue
                    
                    # Logic: Look for 'app.bsky.feed.like' records
                    record = d.get('record')
                    # Fallback for nested structures
                    if not record: record = d.get('post', {}).get('record')
                    if not record: continue

                    # Check type in URI or Record Type (if available)
                    # In timeline dumps, we usually look at the record content
                    # A like record has a 'subject'
                    subject = record.get('subject')
                    if not subject: continue
                    
                    target_uri = subject.get('uri', '')
                    
                    # Filter: Must be a like on a GENERATOR (Feed), not a post
                    if 'app.bsky.feed.generator' not in target_uri:
                        continue 

                    # Extract User
                    user_did = d.get('user') or d.get('author', {}).get('did')
                    if not user_did:
                        null_user += 1
                        continue

                    # Extract Date
                    t = record.get('createdAt')
                    if not t: 
                        null_date += 1
                        continue
                    
                    # Resolve IDs
                    u_id = user_map.get(user_did)
                    f_id = feed_map.get(target_uri)

                    if u_id is not None and f_id is not None:
                        try:
                            t = t.replace('Z', '+00:00')
                            dt = datetime.datetime.fromisoformat(t)
                            date_str = dt.strftime('%Y%m%d')
                            
                            outf.write(f"{u_id},{f_id},{date_str}\n")
                            kept_likes += 1
                        except: pass
                    else:
                        # Count how many we miss because of missing maps
                        # (Expected to be high if feed_map is empty)
                        pass

    print(f'\nDone in {datetime.datetime.now() - start}')
    print(f'Total lines scanned: {total_lines}')
    print(f'Kept Feed Likes: {kept_likes}')
    print(f' (Note: If this is 0, it means no likes matched the known feeds list)')