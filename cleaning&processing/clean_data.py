import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
import re
from collections import defaultdict

# --- CONFIGURATION ---
BASE_DEFAULT = '../data_collection/data'
OUT_DEFAULT = 'results/clean'
USER_MAP_FILE = 'results/enc_users.txt'
LANG_MAP_FILE = 'results/language_mapping.json'
# ---------------------

def load_langmap():
    if not os.path.exists(LANG_MAP_FILE):
        print(f"WARNING: {LANG_MAP_FILE} not found. Languages will not be mapped.")
        return defaultdict(lambda: None)
        
    try:
        with open(LANG_MAP_FILE, encoding='utf-8') as f:
            content = f.read().strip()
            if not content: return defaultdict(lambda: None)
            m = json.loads(content)
        res = defaultdict(lambda: None)
        for k, v in m.items():
            res[k] = v
        return res
    except Exception as e:
        print(f"Error loading lang map: {e}")
        return defaultdict(lambda: None)

def load_enc_users():
    res = dict()
    # Se il file non c'è, non importa! Restituiamo un dizionario vuoto
    # e lo riempiremo man mano che troviamo nuovi utenti.
    if not os.path.exists(USER_MAP_FILE):
        print(f"⚠️ {USER_MAP_FILE} not found. Building new map from scratch...")
        return res

    with open(USER_MAP_FILE, encoding='utf-8') as f:
        while line := f.readline():
            parts = line.rstrip().split()
            if len(parts) >= 2:
                i, k = parts[0], parts[1]
                res[k] = int(i)
    return res

def gzip_iterator(BASE):
    if not os.path.exists(BASE):
        print(f"Error: {BASE} does not exist.")
        return

    for f in sorted(os.listdir(BASE)):
        f = os.path.join(BASE, f)
        if os.path.isdir(f) and 'chunk' in f:
            for file in sorted(os.listdir(f)):
                if file.endswith('.gz'):
                    yield os.path.join(f, file)

def valid_time(t):
    try:
        t = t.replace('Z', '+00:00')
        T = datetime.datetime.fromisoformat(t)
        if T.date() < datetime.datetime(2023, 2, 17).date():
            return False
        return True
    except ValueError:
        return False
    except Exception:
        return None

def extract_did_from_uri(uri):
    """Extracts DID from AT-URI like at://did:plc:abc/app.bsky..."""
    if not uri or 'at://' not in uri: return None
    try:
        parts = uri.split('/')
        if len(parts) >= 3:
            did = parts[2]
            if did.startswith('did:'):
                return did
    except: pass
    return None

if __name__ == '__main__':
    
    start = datetime.datetime.now()
    
    BASE = BASE_DEFAULT
    OUT = OUT_DEFAULT
    
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]
    
    language_map = load_langmap()
    user_map = load_enc_users()
    ids = dict()
    matcher = re.compile(r'(?!\b)@[\w.-]+\w+')

    if not os.path.exists(OUT):
        os.makedirs(OUT)
        print('Created new folder:', OUT)
        
    print(f'Processing files in {BASE} and saving to {OUT}')

    # --- VERBOSE COUNTERS ---
    bad_lines = 0
    total_lines = 0
    kept_lines = 0
    null_post_id = 0
    null_user_id = 0
    null_instance = 0
    null_date = 0
    null_text = 0
    null_langs = 0
    null_like_count = 0
    null_reply_count = 0
    null_repost_count = 0
    null_reply_to = 0
    null_replied_author = 0
    null_thread_root = 0
    null_thread_root_author = 0
    null_repost_from = 0
    null_reposted_author = 0
    null_quotes = 0
    null_quoted_author = 0
    null_labels = 0

    for i, path in enumerate(gzip_iterator(BASE)):
        out_filename = f"{i}.jsonl.gz"
        out_path = os.path.join(OUT, out_filename)
        
        with gzip.open(path, 'rb') as f_in:
            with gzip.open(out_path, 'wb') as f_out: 
                for line in tqdm(f_in, desc=f"File {i}"):
                    total_lines += 1
                    
                    post_id, user_id, instance = None, None, None
                    date, text, langs = None, None, None
                    like_count, reply_count, repost_count = None, None, None
                    reply_to, replied_author = None, None
                    thread_root, thread_root_author = None, None
                    repost_from, reposted_author = None, None
                    quotes, quoted_author = None, None
                    labels = None

                    try:
                        d = json.loads(line.decode('utf-8'))
                    except Exception:
                        bad_lines += 1
                        continue
                    
                    if 'record' in d: post_data = d 
                    elif 'post' in d: post_data = d['post']
                    else: 
                        bad_lines += 1
                        continue

                    record = post_data.get('record', {})
                    t = record.get('createdAt', record.get('created_at'))
                    
                    if t is not None and valid_time(t):
                        kept_lines += 1

                        # USER
                        handle = post_data.get('author', {}).get('did')
                        if not handle: handle = d.get('user')

                        if handle:
                            if handle not in user_map:
                                user_map[handle] = len(user_map)
                            user_id = user_map[handle]
                            if '.' in handle:
                                instance = '.'.join(handle.split('.')[1:])
                        
                        # POST ID
                        uri = post_data.get('uri')
                        if uri:
                            uri_key = uri + str(handle)
                            if uri_key not in ids:
                                ids[uri_key] = len(ids)
                            post_id = ids[uri_key]
                        
                        # DATE
                        t = t.replace('Z', '+00:00')
                        date = int(datetime.datetime.fromisoformat(t).strftime('%Y%m%d%H%M'))

                        # TEXT
                        text = record.get('text', '')
                        if text:
                            for m in matcher.findall(text):
                                m_clean = m[1:] 
                                if m_clean not in user_map:
                                    user_map[m_clean] = len(user_map)
                                text = re.sub(pattern=m, repl=f"@{user_map[m_clean]}", string=text)
                        else:
                            text = None 

                        # LANGS
                        raw_langs = record.get('langs', [])
                        if raw_langs:
                            if isinstance(raw_langs, list):
                                langs = [language_map[l.lower()] or l for l in raw_langs]
                            else:
                                langs = [language_map[raw_langs.lower()] or raw_langs]
                        
                        # METRICS
                        like_count = post_data.get('like_count', 0)
                        reply_count = post_data.get('reply_count', 0)
                        repost_count = post_data.get('repost_count', 0)

                        # --- INTERACTIONS ---
                        reply_ref = d.get('reply') or record.get('reply')
                        if reply_ref:
                            # Reply
                            parent = reply_ref.get('parent')
                            if parent:
                                p_uri = parent.get('uri')
                                if p_uri:
                                    if p_uri not in ids: ids[p_uri] = len(ids)
                                    reply_to = ids[p_uri]
                                    
                                    # ESTRAZIONE DID DA URI (New Logic)
                                    p_auth = extract_did_from_uri(p_uri)
                                    if not p_auth: 
                                        p_auth = parent.get('author', {}).get('did') # Try normal way

                                    if p_auth:
                                        if p_auth not in user_map: user_map[p_auth] = len(user_map)
                                        replied_author = user_map[p_auth]
                            
                            # Root
                            root = reply_ref.get('root')
                            if root:
                                r_uri = root.get('uri')
                                if r_uri:
                                    if r_uri not in ids: ids[r_uri] = len(ids)
                                    thread_root = ids[r_uri]
                                    
                                    # ESTRAZIONE DID DA URI
                                    r_auth = extract_did_from_uri(r_uri)
                                    if not r_auth:
                                        r_auth = root.get('author', {}).get('did')

                                    if r_auth:
                                        if r_auth not in user_map: user_map[r_auth] = len(user_map)
                                        thread_root_author = user_map[r_auth]

                        # Repost
                        post_author_did = post_data.get('author', {}).get('did')
                        current_user_did = d.get('user')
                        if current_user_did and post_author_did and current_user_did != post_author_did:
                            if post_author_did not in user_map: user_map[post_author_did] = len(user_map)
                            reposted_author = user_map[post_author_did]
                            if uri: repost_from = post_id
                        
                        # Quote
                        embed = post_data.get('embed') 
                        if embed and 'record' in embed:
                            quoted = embed['record']
                            if 'record' in quoted: quoted = quoted['record']
                            
                            if quoted and 'uri' in quoted:
                                try:
                                    q_uri = quoted.get('uri')
                                    # ESTRAZIONE DID DA URI
                                    q_auth = extract_did_from_uri(q_uri)
                                    if not q_auth: q_auth = quoted.get('author', {}).get('did')
                                    
                                    if q_uri:
                                        if q_uri not in ids: ids[q_uri] = len(ids)
                                        quotes = ids[q_uri]
                                    if q_auth:
                                        if q_auth not in user_map: user_map[q_auth] = len(user_map)
                                        quoted_author = user_map[q_auth]
                                except KeyError: pass
                                
                        # Labels
                        lbs = post_data.get('labels')
                        if lbs is not None:
                            try:
                                if isinstance(lbs, list) and len(lbs) > 0 and isinstance(lbs[0], dict):
                                    labels = [l.get('val') for l in lbs]
                                else: labels = lbs
                            except: labels = lbs

                        # --- EXPLICIT NULL COUNTING ---
                        if post_id is None: null_post_id += 1
                        if user_id is None: null_user_id += 1
                        if instance is None: null_instance += 1
                        if date is None: null_date += 1
                        if text is None: null_text += 1
                        if langs is None: null_langs += 1
                        if like_count is None: null_like_count += 1
                        if reply_count is None: null_reply_count += 1
                        if repost_count is None: null_repost_count += 1
                        if reply_to is None: null_reply_to += 1
                        if replied_author is None: null_replied_author += 1
                        if thread_root is None: null_thread_root += 1
                        if thread_root_author is None: null_thread_root_author += 1
                        if repost_from is None: null_repost_from += 1
                        if reposted_author is None: null_reposted_author += 1
                        if quotes is None: null_quotes += 1
                        if quoted_author is None: null_quoted_author += 1
                        if labels is None: null_labels += 1
                                
                        # Construct Object
                        clean_obj = {
                            'post_id': post_id,
                            'user_id': user_id,
                            'instance': instance,
                            'date': date,
                            'text': text,
                            'langs': langs,
                            'like_count': like_count,
                            'reply_count': reply_count,
                            'repost_count': repost_count,
                            'reply_to': reply_to,
                            'replied_author': replied_author,
                            'thread_root': thread_root,
                            'thread_root_author': thread_root_author,
                            'repost_from': repost_from,
                            'reposted_author': reposted_author,
                            'quotes': quotes,
                            'quoted_author': quoted_author,
                            'labels': labels
                        }
                        
                        f_out.write((json.dumps(clean_obj) + '\n').encode('utf-8'))

    # Save maps
    print("\nSaving maps...")
    with open(USER_MAP_FILE, 'w', encoding='utf-8') as f:  # <--- MODIFICA QUI
        for u, i in user_map.items(): f.write(f'{i} {u}\n')
    with open('results/enc_uris.txt', 'w', encoding='utf-8') as f:
        for u, i in ids.items(): f.write(f'{i} {u}\n')

    # --- VERBOSE REPORT ---
    print(f'\ndone in {datetime.datetime.now() - start}')
    print(f'total lines: {total_lines}')
    print(f'bad lines: {bad_lines}')
    print(f'kept lines: {kept_lines}')
    print()
    print(f'null post_id: {null_post_id}')
    print(f'null user_id: {null_user_id}')
    print(f'null instance: {null_instance}')
    print(f'null date: {null_date}')
    print(f'null text: {null_text}')
    print(f'null langs: {null_langs}')
    print(f'null like_count: {null_like_count}')
    print(f'null reply_count: {null_reply_count}')
    print(f'null repost_count: {null_repost_count}')
    print(f'null reply_to: {null_reply_to}')
    print(f'null replied_author: {null_replied_author}')
    print(f'null thread_root: {null_thread_root}')
    print(f'null thread_root_author: {null_thread_root_author}')
    print(f'null repost_from: {null_repost_from}')
    print(f'null reposted_author: {null_reposted_author}')
    print(f'null quotes: {null_quotes}')
    print(f'null quoted_author: {null_quoted_author}')
    print(f'null labels: {null_labels}')
    print()