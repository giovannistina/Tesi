# Tesi/data_collection/crawltimelines_limited.py
# Utilizzo: python crawltimelines_limited.py <NUMERO_FILE>
# Esempio: python crawltimelines_limited.py 1

from atproto_client import Client, SessionEvent
from atproto.exceptions import RequestException, BadRequestError
from dateutil import parser
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import datetime, time
import gzip
import os
import sys
import json
import getpass 

# --- CONFIGURAZIONE FISSA ---
SAVE_EVERY_N_USERS = 100
MAX_WORKERS = 10  
MAX_USER_ERRORS = 10

# !!! MODIFICATO: LIMITE AUMENTATO A 720 !!!
MAX_POSTS_LIMIT = 720 
# ----------------------

#### SESSION MANAGEMENT (Dinamico)

def get_session(session_file):
    try:
        with open(session_file) as f:
            return f.read()
    except FileNotFoundError:
        return None

def save_session(session_string, session_file):
    with open(session_file, 'w') as f:
        f.write(session_string)

def init_client_dynamic(username, password, session_file):
    client = Client()
    
    def on_session_change(event, session):
        if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
            save_session(session.export(), session_file)

    client.on_session_change(on_session_change)

    session_string = get_session(session_file)
    if session_string:
        print(f'[{session_file}] Tentativo riutilizzo sessione...')
        try:
            client.login(session_string=session_string)
            print(f'[{session_file}] Login via sessione OK.')
        except Exception:
            print(f"[{session_file}] Sessione scaduta. Login con credenziali...")
            client.login(username, password)
    else:
        print(f'[{session_file}] Creazione nuova sessione per {username}...')
        client.login(username, password)

    return client

#### EXCP HANDLING

def sleep_until(when):
    now = datetime.datetime.now()
    when = datetime.datetime.fromtimestamp(when, datetime.UTC)
    if now.timestamp() < when.timestamp():
        wait_time = (when - now).total_seconds()
        print(f"Rate limit hit. Waiting {wait_time:.1f}s...")
        time.sleep(wait_time)

def _handle_requests_exceptions(e):
    if not hasattr(e, 'response') or not e.response:
        return
        
    status = e.response.status_code
    if status == 429: 
        if 'RateLimit-Reset' in e.response.headers:
            when = int(e.response.headers['RateLimit-Reset'])
            sleep_until(when)
        else:
            time.sleep(60)
    elif status in {409, 413, 502}: 
        time.sleep(10)

#### IO

def _save(posts, processed_users, i, chunk_id):
    # Assicura che la cartella 'data' esista
    os.makedirs('data', exist_ok=True)
    
    # MODIFICATO: Salvataggio diretto in data/timelines-X.jsonl.gz
    filename = f'data/timelines-{chunk_id}.jsonl.gz'
    
    with gzip.open(filename, 'a') as f:
        for post in posts:
            rec = post.record 
            embed_type = None
            if post.embed:
                embed_type = getattr(post.embed, 'py_type', 'unknown')

            post_lite = {
                "uri": post.uri,
                "cid": post.cid,                      
                "user": getattr(post, 'user', None),  
                "author": { "did": post.author.did },
                "record": {
                    "text": getattr(rec, 'text', ""),
                    "created_at": getattr(rec, 'created_at', ""),
                    "langs": getattr(rec, 'langs', []),
                    "reply": getattr(rec, 'reply', None) 
                },
                "embed_type": embed_type,             
                "labels": post.labels,                
                "like_count": post.like_count or 0,
                "repost_count": post.repost_count or 0,
                "reply_count": post.reply_count or 0
            }

            row = f"{json.dumps(post_lite, default=str)}\n"
            f.write(row.encode('utf8'))

    with open(f'processedT_{chunk_id}.txt', 'a') as f:
        for u in processed_users:
            f.write(f'{u}\t{i}\n')
            
def _read_list(path):
    if not os.path.exists(path):
        return []
    res = []
    with open(path) as f:
        for l in f.readlines():
            if l.strip():
                res.append(l.strip())
    return res

def collect_timeline(client, handle):
    count_user_errors = 0 
    cursor = None
    old_cursor = None
    posts = []
    
    TIME_LIMIT = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
    stop_download = False

    while True:
        if count_user_errors > MAX_USER_ERRORS: break
        if stop_download: break
        if len(posts) >= MAX_POSTS_LIMIT: break

        try:
            fetched = client.get_author_feed(handle, limit=100, cursor=cursor)
            
            for post_view in fetched.feed:
                if len(posts) >= MAX_POSTS_LIMIT:
                    stop_download = True
                    break 

                try:
                    post_date_str = post_view.post.record.created_at
                    post_date = parser.parse(post_date_str)
                    if post_date.tzinfo is None:
                         post_date = post_date.replace(tzinfo=datetime.timezone.utc)
                    
                    if post_date < TIME_LIMIT:
                        stop_download = True 
                    else:
                        if not stop_download:
                            posts.append(post_view.post)
                except Exception:
                    continue 

        except RequestException as e:
            count_user_errors +=1
            _handle_requests_exceptions(e)
            cursor = old_cursor
            continue
        except BadRequestError:
            return [] 
        except Exception as e:
            error_msg = str(e).lower()
            if "validation error" in error_msg and "aspectratio" in error_msg:
                count_user_errors +=1
                cursor = old_cursor
                continue
            count_user_errors +=1
            cursor = old_cursor
            continue
        
        if not fetched.cursor or stop_download: break
        old_cursor = cursor
        cursor = fetched.cursor
    
    return posts

def process_user_wrapper(client, user):
    try:
        posts = collect_timeline(client, user)
        for post in posts:
            post.user = user
        return user, posts
    except Exception:
        return user, []

if __name__ == '__main__':
    start_run_time = time.time()
    
    if len(sys.argv) < 2:
        print("Uso: python crawltimelines_limited.py <CHUNK_NUMBER>")
        sys.exit(1)

    CHUNK = int(sys.argv[1])
    
    print("\n" + "="*40)
    print(f"🔑 CONFIGURAZIONE CREDENZIALI PER CHUNK {CHUNK}")
    print("="*40)
    
    try:
        CLI_USER = input("Inserisci Username Bluesky: ").strip()
        CLI_PASS = getpass.getpass("Inserisci Password Bluesky (non visibile): ").strip()
    except KeyboardInterrupt:
        print("\nOperazione annullata.")
        sys.exit(0)

    SESSION_FILE = f"session_{CHUNK}.txt"
    
    try:
        client = init_client_dynamic(CLI_USER, CLI_PASS, SESSION_FILE)
    except Exception as e:
        print(f"Login failed: {e}")
        sys.exit(1)

    user_list = _read_list(f'data/{CHUNK}.txt')
    processed_raw = _read_list(f'processedT_{CHUNK}.txt')
    
    processed_set = set()
    for p in processed_raw:
        parts = p.split('\t')
        if parts:
            processed_set.add(parts[0])

    n_processed = len(processed_set)
    if n_processed > 0:
        original_count = len(user_list)
        user_list = [u for u in user_list if u not in processed_set]
        print(f'Resuming: {original_count} total, {len(user_list)} left.')
    
    users_to_process_count = len(user_list)
    
    all_posts_buffer = []
    processed_buffer = []

    print(f"Starting collection on CHUNK {CHUNK} with User {CLI_USER}...")

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_user = {executor.submit(process_user_wrapper, client, user): user for user in user_list}
            
            for i, future in enumerate(tqdm(as_completed(future_to_user), total=len(user_list), desc=f"Chunk {CHUNK}", unit="user")):
                
                user, posts = future.result()
                
                if posts:
                    all_posts_buffer.extend(posts)
                processed_buffer.append(user)
                
                total_done = n_processed + i + 1

                if total_done % SAVE_EVERY_N_USERS == 0:
                    _save(all_posts_buffer, processed_buffer, total_done, CHUNK)
                    all_posts_buffer = []
                    processed_buffer = []

            if all_posts_buffer or processed_buffer:
                total_done = n_processed + len(user_list)
                _save(all_posts_buffer, processed_buffer, total_done, CHUNK)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    
    end_run_time = time.time()
    total_duration = end_run_time - start_run_time
    minutes = total_duration / 60

    print("\n" + "="*50)
    print("                FINAL REPORT")
    print("="*50)
    print(f"USERS PROCESSED:      {users_to_process_count}")
    print(f"TOTAL TIME:           {total_duration:.2f}s ({minutes:.2f} min)")
    print("="*50 + "\n")