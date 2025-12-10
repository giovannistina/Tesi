import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm

# --- CONFIGURATION (Adapted for Windows/Folders) ---
BASE_DEFAULT = '../data_collection/data'
OUT_DEFAULT = 'results/clean_feeds.jsonl.gz'
USER_MAP_FILE = 'results/enc_users.txt'
LANG_MAP_FILE = 'results/language_mapping.json'
# ---------------------------------------------------

def load_langmap():
    if not os.path.exists(LANG_MAP_FILE):
        return dict()
    try:
        # MODIFICATION: encoding utf-8
        with open(LANG_MAP_FILE, encoding='utf-8') as f:
            content = f.read().strip()
            if not content: return dict()
            return json.loads(content)
    except: return dict()

def load_enc_users():
    res = dict()
    if not os.path.exists(USER_MAP_FILE):
        print(f"Error: {USER_MAP_FILE} not found.")
        return res 

    # MODIFICATION: encoding utf-8
    with open(USER_MAP_FILE, encoding='utf-8') as f:
        while line := f.readline():
            parts = line.rstrip().split()
            if len(parts) >= 2:
                i, k = parts[0], parts[1]
                res[k] = int(i)
    return res

def gzip_iterator(BASE):
    if not os.path.exists(BASE): return
    files = sorted(os.listdir(BASE))
    
    # MODIFICATION: Try/Except to avoid crash if filenames are not numbers
    try: files.sort(key=lambda x: int(x.split('.')[0]))
    except: pass

    for f in files:
        full_path = os.path.join(BASE, f)
        # Support for chunk structure (if present) or direct files
        if os.path.isdir(full_path) and 'chunk' in f:
            for sub in sorted(os.listdir(full_path)):
                if sub.endswith('.gz'):
                    yield os.path.join(full_path, sub)
        elif f.endswith('.gz'):
            yield full_path

def valid_time(t):
    try:
        # MODIFICATION: Fix ISO format
        t = t.replace('Z', '+00:00')
        T = datetime.datetime.fromisoformat(t)
        
        # MODIFICATION: Removed upper limit (2024) to accept 2025 data
        # if T.date() > datetime.datetime(2024, 3, 18).date(): return False
        
        if T.date() < datetime.datetime(2023, 2, 17).date(): return False
        return True
    except: return False

if __name__ == '__main__':
    
    start = datetime.datetime.now()
    BASE, OUT = BASE_DEFAULT, OUT_DEFAULT
    
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    print("Loading maps...")
    language_map = load_langmap()
    user_map = load_enc_users()
    
    # Local map for Feeds (URI -> ID)
    feed_ids = dict()

    # --- ORIGINAL VERBOSE COUNTERS ---
    total_lines = 0
    bad_lines = 0
    kept_feeds = 0
    
    null_uri = 0
    null_cid = 0
    null_creator = 0
    null_name = 0
    null_description = 0
    null_description_facets = 0
    null_avatar = 0
    null_like_count = 0
    null_viewer = 0
    null_indexed_at = 0

    print(f"Processing feeds from {BASE} -> {OUT}")
    
    # MODIFICATION: encoding utf-8
    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        
        for path in gzip_iterator(BASE):
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    total_lines += 1
                    try:
                        d = json.loads(line)
                    except:
                        bad_lines += 1
                        continue
                    
                    # Original Logic: Look for generators
                    uri = d.get('uri')
                    if not uri or 'app.bsky.feed.generator' not in uri:
                        continue 

                    # Look for record (variable structure)
                    record = d.get('record')
                    if not record: record = d.get('value') # Fallback for some dumps
                    if not record: continue

                    kept_feeds += 1

                    # Extract Fields
                    cid = d.get('cid')
                    creator_did = d.get('creator', {}).get('did')
                    
                    # Fallback for creator from URI if missing in dict
                    if not creator_did and uri.startswith('at://'):
                        try: creator_did = uri.split('/')[2]
                        except: pass

                    name = record.get('displayName')
                    description = record.get('description')
                    description_facets = record.get('descriptionFacets')
                    avatar = record.get('avatar')
                    like_count = d.get('likeCount')
                    viewer = d.get('viewer')
                    indexed_at = d.get('indexedAt')
                    
                    # Count Nulls
                    if not uri: null_uri += 1
                    if not cid: null_cid += 1
                    if not creator_did: null_creator += 1
                    if not name: null_name += 1
                    if not description: null_description += 1
                    if not description_facets: null_description_facets += 1
                    if not avatar: null_avatar += 1
                    if like_count is None: null_like_count += 1
                    if not viewer: null_viewer += 1
                    if not indexed_at: null_indexed_at += 1

                    # Mapping
                    creator_id = None
                    if creator_did:
                        if creator_did not in user_map:
                            user_map[creator_did] = len(user_map)
                        creator_id = user_map[creator_did]
                    
                    feed_num_id = None
                    if uri:
                        if uri not in feed_ids:
                            feed_ids[uri] = len(feed_ids)
                        feed_num_id = feed_ids[uri]

                    # Write Output
                    clean_obj = {
                        'feed_id': feed_num_id,
                        'uri': uri,
                        'cid': cid,
                        'creator_id': creator_id,
                        'name': name,
                        'description': description,
                        'like_count': like_count,
                        'indexed_at': indexed_at
                    }
                    
                    outf.write(json.dumps(clean_obj) + '\n')

    # Save Maps (Crucial for next steps)
    print("Saving updated maps...")
    with open('results/enc_users_updated.txt', 'w', encoding='utf-8') as f:
        for u, i in user_map.items(): f.write(f'{i} {u}\n')
        
    with open('results/enc_feeds.txt', 'w', encoding='utf-8') as f:
        for u, i in feed_ids.items(): f.write(f'{i} {u}\n')

    # Final Report
    print(f'\nDone in {datetime.datetime.now() - start}')
    print(f'Total lines scanned: {total_lines}')
    print(f'Bad lines: {bad_lines}')
    print(f'Kept Feeds: {kept_feeds}')
    print()
    print(f'null uri: {null_uri}')
    print(f'null cid: {null_cid}')
    print(f'null creator: {null_creator}')
    print(f'null name: {null_name}')
    print(f'null description: {null_description}')
    print(f'null description_facets: {null_description_facets}')
    print(f'null avatar: {null_avatar}')
    print(f'null like_count: {null_like_count}')
    print(f'null viewer: {null_viewer}')
    print(f'null indexed_at: {null_indexed_at}')
    print()
    
    # WINDOWS MODIFICATION: Sort removed
    # os.system(...)