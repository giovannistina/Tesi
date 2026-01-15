import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm

# --- CONFIGURAZIONE ---
BASE_DEFAULT = '../data_collection/data'
OUT_DEFAULT = 'results/clean_feed_bookmarks.csv.gz'
USER_MAP_FILE = 'results/enc_users.txt'
FEED_MAP_FILE = 'results/enc_feeds.txt'
# ---------------------

def load_map(path):
    res = dict()
    if not os.path.exists(path): return res
    with open(path, encoding='utf-8') as f:
        while line := f.readline():
            parts = line.rstrip().split()
            if len(parts) >= 2:
                res[parts[1]] = int(parts[0])
    return res

def gzip_iterator(BASE):
    if not os.path.exists(BASE): return
    for f in sorted(os.listdir(BASE)):
        path = os.path.join(BASE, f)
        if os.path.isdir(path) and 'chunk' in f:
            for sub in sorted(os.listdir(path)):
                if sub.endswith('.gz'): yield os.path.join(path, sub)
        elif f.endswith('.gz'): yield path

if __name__ == '__main__':
    start = datetime.datetime.now()
    BASE, OUT = BASE_DEFAULT, OUT_DEFAULT
    
    # Argomenti CLI
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    print("Loading maps...")
    user_map = load_map(USER_MAP_FILE)
    feed_map = load_map(FEED_MAP_FILE)
    
    count = 0
    print(f"Scanning {BASE} -> {OUT}")

    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        outf.write("user_id,feed_id,date\n") # Header

        for path in gzip_iterator(BASE):
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    try:
                        d = json.loads(line)
                    except: continue

                    # Logica originale semplice: Cerca uri generatori
                    uri = d.get('uri', '')
                    
                    # Se non è un generatore o se mancano i dati chiave, salta
                    # (Nel codice originale si assumeva che certi record fossero bookmark)
                    if 'app.bsky.feed.generator' not in uri: continue
                    
                    user_did = d.get('user')
                    if not user_did: continue
                    
                    # Risoluzione ID veloce
                    u_id = user_map.get(user_did)
                    f_id = feed_map.get(uri)

                    if u_id is not None and f_id is not None:
                        t = d.get('createdAt') or d.get('indexedAt')
                        date_str = "00000000"
                        if t:
                            try: date_str = t.replace('Z', '').replace('-', '')[:8]
                            except: pass
                        
                        outf.write(f"{u_id},{f_id},{date_str}\n")
                        count += 1

    print(f"Done. Extracted {count} bookmarks.")