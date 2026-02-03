# Tesi/data_collection/crawltimelines_limited.py
# Utilizzo: python crawltimelines_limited.py <NUMERO_FILE>
# Esempio: python crawl_timelines_limited.py 1

import subprocess
import sys

# Tenta di importare atproto. Se non c'è, lo installa e poi prosegue.
try:
    import atproto
except ImportError:
    print("⚠️ Libreria 'atproto' mancante. Installazione automatica in corso...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "atproto"])
    print("✅ Installazione completata.")
    import atproto
    
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
SAVE_EVERY_N_USERS = 20
MAX_WORKERS = 3  
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

# --- INIZIO NUOVO BLOCCO MAIN ---
if __name__ == '__main__':
    # Importiamo il Garbage Collector per forzare la pulizia RAM
    import gc 
    start_run_time = time.time()
    
    # --- GESTIONE ARGOMENTI ---
    if len(sys.argv) == 4:
        CHUNK = int(sys.argv[1])
        CLI_USER = sys.argv[2]
        CLI_PASS = sys.argv[3]
        print(f"✅ Login automatico via argomenti per: {CLI_USER}")
    elif len(sys.argv) == 2:
        CHUNK = int(sys.argv[1])
        print(f"🔑 CONFIGURAZIONE CREDENZIALI PER CHUNK {CHUNK}")
        try:
            CLI_USER = input("Inserisci Username Bluesky: ").strip()
            CLI_PASS = getpass.getpass("Inserisci Password Bluesky: ").strip()
        except KeyboardInterrupt:
            sys.exit(0)
    else:
        print("Uso: python crawltimelines_limited.py <CHUNK> [USER] [PASS]")
        sys.exit(1)
    # --------------------------

    SESSION_FILE = f"session_{CHUNK}.txt"
    try:
        client = init_client_dynamic(CLI_USER, CLI_PASS, SESSION_FILE)
    except Exception as e:
        print(f"Login failed: {e}")
        sys.exit(1)

    # 1. Carichiamo le liste
    all_users = _read_list(f'data/{CHUNK}.txt')
    processed_raw = _read_list(f'processedT_{CHUNK}.txt')
    
    processed_set = set()
    for p in processed_raw:
        parts = p.split('\t')
        if parts: processed_set.add(parts[0])

    # 2. Filtriamo chi manca
    users_to_do = [u for u in all_users if u not in processed_set]
    original_count = len(all_users)
    count_left = len(users_to_do)
    
    if len(processed_set) > 0:
        print(f'Resuming: {original_count} total, {count_left} left.')

    print(f"Starting collection on CHUNK {CHUNK} with User {CLI_USER}...")
    
    # --- MODIFICA FONDAMENTALE: BATCH PROCESSING ---
    # Invece di caricare 60.000 future insieme, processiamo a blocchi
    # Questo mantiene la RAM piatta e costante.
    
    batch_size = SAVE_EVERY_N_USERS  # Usiamo 20 (o quello che hai impostato)
    total_processed_session = 0

    # Creiamo un Executor che vivrà per tutto il tempo (così non lo ricreiamo sempre)
    # Nota: max_workers qui definisce il parallelismo reale
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        
        # Iteriamo sulla lista a blocchi (es. 0-20, 20-40, 40-60...)
        for i in range(0, len(users_to_do), batch_size):
            
            # 1. Prendiamo solo un pezzetto di lista
            batch_users = users_to_do[i : i + batch_size]
            
            # 2. Sottomettiamo SOLO questi al ThreadPool
            future_to_user = {executor.submit(process_user_wrapper, client, user): user for user in batch_users}
            
            batch_posts = []
            batch_processed_users = []
            
            # 3. Attendiamo i risultati di questo piccolo batch
            for future in as_completed(future_to_user):
                user, posts = future.result()
                if posts:
                    batch_posts.extend(posts)
                batch_processed_users.append(user)
            
            # 4. Salviamo subito
            current_total_done = len(processed_set) + total_processed_session + len(batch_processed_users)
            _save(batch_posts, batch_processed_users, current_total_done, CHUNK)
            
            # Aggiorniamo contatori per logica
            total_processed_session += len(batch_processed_users)
            
            # 5. PULIZIA RAM AGGRESSIVA
            del future_to_user
            del batch_posts
            del batch_processed_users
            gc.collect()  # <--- Forza Python a liberare la memoria ORA
            
            # Log di progresso manuale (visto che non usiamo più tqdm globale per evitare memory leak della bar)
            percent = (current_total_done / original_count) * 100
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Chunk {CHUNK}: {current_total_done}/{original_count} ({percent:.2f}%) - RAM Pulita")

    end_run_time = time.time()
    total_duration = end_run_time - start_run_time
    minutes = total_duration / 60

    print("\n" + "="*50)
    print("                FINAL REPORT")
    print("="*50)
    print(f"USERS PROCESSED:      {count_left}")
    print(f"TOTAL TIME:           {total_duration:.2f}s ({minutes:.2f} min)")
    print("="*50 + "\n")