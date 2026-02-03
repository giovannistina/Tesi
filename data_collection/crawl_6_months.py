# Tesi/data_collection/crawl_6_months.py

import subprocess
import sys
import os
import json
import gzip
import time
import datetime
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from dateutil import parser

# Tenta di importare atproto
try:
    import atproto
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "atproto"])
    import atproto

from atproto_client import Client, SessionEvent
from atproto.exceptions import RequestException, BadRequestError

# --- CONFIGURAZIONE FISSA ---
SAVE_EVERY_N_USERS = 20
MAX_WORKERS = 3  
MAX_USER_ERRORS = 5
MAX_POSTS_LIMIT = 4320 
# ----------------------

#### SESSION MANAGEMENT
def get_session(session_file):
    if os.path.exists(session_file):
        with open(session_file, 'r') as f: return f.read()
    return None

def save_session(session_string, session_file):
    with open(session_file, 'w') as f: f.write(session_string)

def init_client_dynamic(username, password, session_file):
    client = Client()
    def on_session_change(event, session):
        if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
            save_session(session.export(), session_file)
    client.on_session_change(on_session_change)
    
    session_string = get_session(session_file)
    if session_string:
        try:
            client.login(session_string=session_string)
            print(f"[{session_file}] Sessione ripristinata.")
        except Exception:
            client.login(username, password)
    else:
        client.login(username, password)
    return client

#### RATE LIMITS
def sleep_until(when):
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    if now < when:
        wait_time = (when - now) + 5
        print(f"Rate limit hit. Attesa di {wait_time:.1f}s...")
        time.sleep(wait_time)

def _handle_requests_exceptions(e):
    if hasattr(e, 'response') and e.response:
        if e.response.status_code == 429:
            reset = e.response.headers.get('RateLimit-Reset')
            if reset: sleep_until(int(reset))
            else: time.sleep(60)
        elif e.response.status_code in {502, 503, 504}:
            time.sleep(15)

#### IO (MODIFICATO PER GESTIRE I REPOST)
def _save(feed_items, processed_users, chunk_id):
    """
    feed_items: Lista di oggetti FeedViewPost (che contengono .post e .reason)
    """
    os.makedirs('data', exist_ok=True)
    filename = f'data/timelines-6m-{chunk_id}.jsonl.gz'
    
    with gzip.open(filename, 'ab') as f:
        for item in feed_items:
            post = item.post
            reason = item.reason # Qui c'è l'info sul Repost!
            rec = post.record

            # LOGICA CHIAVE PER DISTINGUERE LUCA DA CARLO
            if reason:
                # È UN REPOST (Carlo reposta Luca)
                # L'autore dell'azione è Carlo (reason.by.did)
                action_did = reason.by.did
                # La data dell'azione è quando Carlo ha repostato
                action_date = reason.indexed_at 
                is_repost = True
                original_author = post.author.did
            else:
                # È UN POST NORMALE (Luca scrive)
                action_did = post.author.did
                action_date = post.indexed_at # O rec.created_at
                is_repost = False
                original_author = post.author.did

            # Struttura Dati Arricchita
            post_lite = {
                # Info sull'Azione (Chi e Quando nel feed)
                "did": action_did,            # Chi ha fatto l'azione (Carlo)
                "created_at": str(action_date), # Quando l'ha fatta
                "is_repost": is_repost,
                
                # Info sul Contenuto Originale
                "uri": post.uri,              # URI del contenuto originale
                "text": getattr(rec, 'text', ""),
                
                # Dettagli Autoriali
                "original_author_did": original_author, # Chi ha scritto il testo (Luca)
                
                # Metriche del contenuto originale
                "like_count": post.like_count or 0,
                "repost_count": post.repost_count or 0,
                "reply_count": post.reply_count or 0,
                
                # Metadata extra
                "langs": getattr(rec, 'langs', []),
                "reply_ref": getattr(rec, 'reply', None) # Se è una risposta
            }
            
            f.write((json.dumps(post_lite, default=str) + "\n").encode('utf8'))
    
    # Aggiorna lista utenti fatti
    with open(f'processed_6m_{CHUNK_ID}.txt', 'a') as f:
        for u in processed_users: f.write(f"{u}\n")

#### COLLECTION
def collect_timeline(client, handle):
    items = [] # Qui salviamo i FeedViewPost completi
    cursor = None
    TIME_LIMIT = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=180)
    
    while len(items) < MAX_POSTS_LIMIT:
        try:
            fetched = client.get_author_feed(handle, limit=100, cursor=cursor)
            if not fetched.feed: break
            
            for feed_view in fetched.feed:
                # --- CONTROLLO TEMPORALE ---
                # Dobbiamo controllare la data dell'AZIONE (repost o post), non solo del contenuto.
                # Se Carlo reposta oggi un post di Luca del 2020, per noi conta come attività di oggi.
                
                if feed_view.reason:
                    # Data del Repost
                    check_date_str = feed_view.reason.indexed_at
                else:
                    # Data del Post
                    check_date_str = feed_view.post.record.created_at
                
                try:
                    p_date = parser.parse(check_date_str)
                    if p_date.tzinfo is None: p_date = p_date.replace(tzinfo=datetime.timezone.utc)
                    
                    if p_date < TIME_LIMIT: 
                        return items # Siamo andati troppo indietro nel tempo
                except:
                    pass # Se la data è illeggibile, lo prendiamo per sicurezza o lo saltiamo (qui proseguiamo)

                # --- SALVATAGGIO ---
                # Salviamo TUTTO l'oggetto (post + reason)
                items.append(feed_view)
                
                if len(items) >= MAX_POSTS_LIMIT: return items

            if not fetched.cursor: break
            cursor = fetched.cursor
        except RequestException as e:
            _handle_requests_exceptions(e)
        except Exception: break
    return items

def process_user_wrapper(client, user):
    try:
        # Ritorna user e LISTA items
        items = collect_timeline(client, user)
        return user, items
    except Exception:
        return user, []

# --- MAIN ENGINE ---
if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Uso: python crawl_6_months.py <FILE_CHUNK> <USER> <PASS>")
        sys.exit(1)

    CHUNK_PATH = sys.argv[1]
    CLI_USER = sys.argv[2]
    CLI_PASS = sys.argv[3]
    CHUNK_ID = os.path.basename(CHUNK_PATH).split('.')[0]
    
    # Variabile globale per il chunk id usata in _save (non bellissimo ma efficace qui)
    # Meglio passarla come argomento, fatto sopra nella chiamata _save
    
    client = init_client_dynamic(CLI_USER, CLI_PASS, f"session_{CLI_USER}.txt")

    with open(CHUNK_PATH, 'r') as f:
        all_users = [line.strip() for line in f if line.strip()]
    
    processed_path = f'processed_6m_{CHUNK_ID}.txt'
    processed_set = set()
    if os.path.exists(processed_path):
        with open(processed_path, 'r') as f:
            processed_set = {line.strip() for line in f}

    users_to_do = [u for u in all_users if u not in processed_set]
    print(f"Account: {CLI_USER} | Chunk: {CHUNK_ID} | Da fare: {len(users_to_do)}/{len(all_users)}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for i in range(0, len(users_to_do), SAVE_EVERY_N_USERS):
            batch_users = users_to_do[i : i + SAVE_EVERY_N_USERS]
            future_to_user = {executor.submit(process_user_wrapper, client, u): u for u in batch_users}
            
            batch_items = []
            batch_done = []
            
            for future in as_completed(future_to_user):
                u, items = future.result()
                
                # MODIFICA: Salva solo se ha attività, ma segna comunque come fatto
                if items:
                    batch_items.extend(items)
                batch_done.append(u)
            
            if batch_items:
                _save(batch_items, batch_done, CHUNK_ID)
            else:
                # Se il batch è vuoto di post, salviamo comunque che abbiamo processato gli utenti
                with open(f'processed_6m_{CHUNK_ID}.txt', 'a') as f:
                    for u in batch_done: f.write(f"{u}\n")

            del batch_items
            gc.collect()
            
            total_done = len(processed_set) + i + len(batch_done)
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {CHUNK_ID}: {total_done}/{len(all_users)}")