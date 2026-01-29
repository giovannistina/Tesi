# Tesi/data_collection/crawl_timelines_limited.py


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

# --- CONFIGURAZIONE ---
USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')
USERS_PER_FILE = 50000
SAVE_EVERY_N_USERS = 100
MAX_WORKERS = 10  
MAX_USER_ERRORS = 10

# !!! NUOVO LIMITE NUMERICO !!!
MAX_POSTS_LIMIT = 530 
# ----------------------

#### SESSION

def get_session():
    try:
        with open('session.txt') as f:
            return f.read()
    except FileNotFoundError:
        return None

def save_session(session_string):
    with open('session.txt', 'w') as f:
        f.write(session_string)

def on_session_change(event, session):
    if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
        save_session(session.export())

def init_client(USERNAME, PASSWORD):
    client = Client()
    client.on_session_change(on_session_change)

    session_string = get_session()
    if session_string:
        print('Reusing session')
        try:
            client.login(session_string=session_string)
        except Exception:
            print("Session expired/invalid. Creating new session...")
            client.login(USERNAME, PASSWORD)
    else:
        print('Creating new session')
        client.login(USERNAME, PASSWORD)

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
    if status == 429:  # Rate Limit
        if 'RateLimit-Reset' in e.response.headers:
            when = int(e.response.headers['RateLimit-Reset'])
            sleep_until(when)
        else:
            time.sleep(60)
    elif status in {409, 413, 502}:  # Network/Server errors
        time.sleep(10)

#### IO

def _save(posts, processed_users, i, file_id, chunk_id):
    os.makedirs(f'data/chunk_{chunk_id}', exist_ok=True)
    
    with gzip.open(f'data/chunk_{chunk_id}/timelines-{file_id}.jsonl.gz', 'a') as f:
        for post in posts:
            row = f"{post.model_dump_json()}\n"
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
    
    # --- TIME CONFIGURATION ---
    # Se vuoi scaricare 530 post anche se sono vecchi di 2 anni, 
    # commenta queste righe relative a TIME_LIMIT
    TIME_LIMIT = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
    stop_download = False
    # ----------------------------

    while True:
        # Stop di sicurezza errori
        if count_user_errors > MAX_USER_ERRORS:
            break
        
        # Stop logico (tempo o numero)
        if stop_download:
            break

        # Stop Numero Max Raggiunto (Controllo preventivo)
        if len(posts) >= MAX_POSTS_LIMIT:
            break

        try:
            fetched = client.get_author_feed(handle, limit=100, cursor=cursor)
            
            for post_view in fetched.feed:
                
                # 1. CONTROLLO LIMITE NUMERICO (Durante il ciclo)
                if len(posts) >= MAX_POSTS_LIMIT:
                    stop_download = True
                    break # Esce dal ciclo for, poi uscirà dal while

                try:
                    # 2. CONTROLLO LIMITE TEMPORALE
                    post_date_str = post_view.post.record.created_at
                    post_date = parser.parse(post_date_str)
                    if post_date.tzinfo is None:
                         post_date = post_date.replace(tzinfo=datetime.timezone.utc)
                    
                    if post_date < TIME_LIMIT:
                        stop_download = True 
                        # Non facciamo break subito perché potremmo voler salvare questo post o i precedenti nel batch
                        # ma il flag fermerà il prossimo batch
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
            return [] # User deleted/not found
        except Exception as e:
            error_msg = str(e).lower()
            if "validation error" in error_msg and "aspectratio" in error_msg:
                count_user_errors +=1
                cursor = old_cursor
                continue
            
            count_user_errors +=1
            # print(f"Err {handle}: {e}")
            cursor = old_cursor
            continue
        
        if not fetched.cursor or stop_download:
            break
        
        old_cursor = cursor
        cursor = fetched.cursor
    
    return posts

# Wrapper per il multithreading
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
        print("Error: please specify the chunk number (e.g., python crawl_timelines.py 1)")
        sys.exit(1)

    CHUNK = int(sys.argv[1])
    
    try:
        client = init_client(USERNAME, PASSWORD)
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
    current_file_id = USERS_PER_FILE + int(n_processed/USERS_PER_FILE) * USERS_PER_FILE
    
    all_posts_buffer = []
    processed_buffer = []

    print(f"Starting collection with {MAX_WORKERS} threads (Limit: {MAX_POSTS_LIMIT} posts/user)...")

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_user = {executor.submit(process_user_wrapper, client, user): user for user in user_list}
            
            for i, future in enumerate(tqdm(as_completed(future_to_user), total=len(user_list), desc="Processing", unit="user")):
                
                user, posts = future.result()
                
                if posts:
                    all_posts_buffer.extend(posts)
                processed_buffer.append(user)
                
                total_done = n_processed + i + 1

                if total_done % (USERS_PER_FILE + SAVE_EVERY_N_USERS) == 0:
                    current_file_id += USERS_PER_FILE
                    _save(all_posts_buffer, processed_buffer, total_done, current_file_id, CHUNK)
                    all_posts_buffer = []
                    processed_buffer = []
                elif total_done % SAVE_EVERY_N_USERS == 0:
                    _save(all_posts_buffer, processed_buffer, total_done, current_file_id, CHUNK)
                    all_posts_buffer = []
                    processed_buffer = []

            if all_posts_buffer or processed_buffer:
                total_done = n_processed + len(user_list)
                _save(all_posts_buffer, processed_buffer, total_done, current_file_id, CHUNK)

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
    if users_to_process_count > 0:
        print(f"AVG TIME (Parallel):  {total_duration / users_to_process_count:.2f} s/user")
    print("="*50 + "\n")