#Questo file è stato modificato per prendere le attività solamente degli ultimi 30 giorni. 
#Per il codice originale andare sul sito e riscaricare lo script
 
from atproto_client import Client, SessionEvent
from atproto.exceptions import RequestException, BadRequestError
from dateutil import parser
from datetime import datetime, timezone, timedelta

import datetime, time
import gzip
import os
import sys

import logging.handlers
log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')
USERS_PER_FILE = 50000
SAVE_EVERY_N_USERS = 100


count_user_errors = 0
MAX_USER_ERRORS = 10

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
    print('Session changed:', event, repr(session))
    if event in (SessionEvent.CREATE, SessionEvent.REFRESH):
        print('Saving changed session')
        save_session(session.export())


def init_client(USERNAME, PASSWORD):
    client = Client()
    client.on_session_change(on_session_change)

    session_string = get_session()
    if session_string:
        print('Reusing session')
        client.login(session_string=session_string)
    else:
        print('Creating new session')
        client.login(USERNAME, PASSWORD)

    return client

#### EXCP HANDLING

def sleep_until(when):
    now = datetime.datetime.now()
    when = datetime.datetime.fromtimestamp(when, datetime.UTC)
    if now.timestamp() > when.timestamp():
        pass
    else:
        print(f"waiting until {when}")
        time.sleep((when - now).total_seconds())


def _handle_requests_exceptions(e):
    status = e.response.status_code
    print(f"{datetime.datetime.now()}. error {status} {e.response.content.message}")
    if status == 429:  # too many
        when = int(e.response.headers['RateLimit-Reset'])
        sleep_until(when)

    elif status in {409, 413, 502}:  # net error
        time.sleep(50)
    else:
        pass


#### IO

def _save(posts, processed_users, i, file_id):

    with gzip.open(f'data/chunk_{CHUNK}/timelines-{file_id}.jsonl.gz', 'a') as f:
        for post in posts:
            row = f"{post.model_dump_json()}\n"
            f.write(row.encode('utf8'))

    with open(f'processed_{CHUNK}.txt', 'a') as f:
        for u in processed_users:
            f.write(f'{u}\t{i}\n')
    print(f'{datetime.datetime.now()} SAVED {i+1}')
            


def _read_list(path):
    if not os.path.exists(path):
        return []
    res = []
    with open(path) as f:
        for l in f.readlines():
            res.append(l.rstrip())
    return res


def collect_timeline(client, handle, cursor=None, posts=None):
    count_user_errors = 0 
    cursor = None
    old_cursor = None
    
    # --- CONFIGURAZIONE TEMPO ---
    # Scarica solo post degli ultimi 30 giorni
    TIME_LIMIT = datetime.now(timezone.utc) - timedelta(days=30)
    stop_download = False
    # ----------------------------

    if posts is None:
        posts = []
    
    while True:
        if count_user_errors > MAX_USER_ERRORS:
            return posts
        
        # Se abbiamo superato la data limite, fermiamoci
        if stop_download:
            break

        try:
            fetched = client.get_author_feed(handle, limit=100, cursor=cursor)
            
            # Controllo date nel blocco appena scaricato
            for post_view in fetched.feed:
                # Estraggo la data del post
                post_date_str = post_view.post.record.created_at
                post_date = parser.parse(post_date_str)
                
                # Se il post è troppo vecchio...
                if post_date < TIME_LIMIT:
                    stop_download = True # ...attiva il freno per il prossimo giro
                    # Non aggiungiamo questo post e usciamo dal for
                    continue 
                
                # Altrimenti aggiungi il post alla lista
                posts.append(post_view)

        except RequestException as e:
            count_user_errors +=1
            _handle_requests_exceptions(e)
            cursor = old_cursor
            continue
        except BadRequestError:
            return []
        except Exception as e:
            count_user_errors +=1
            print(f"{datetime.datetime.now()} {e}")
            cursor = old_cursor
            continue
        
        if not fetched.cursor or stop_download:
            break
        
        old_cursor = cursor
        cursor = fetched.cursor
    
    return posts


if __name__ == '__main__':
    CHUNK = int(sys.argv[1])

    if not os.path.exists(f'data/chunk_{CHUNK}'):
        os.makedirs(f'data/chunk_{CHUNK}')
    
    client = init_client(USERNAME, PASSWORD)
    user_list = _read_list(f'{CHUNK}.txt')
    all_posts = []
    processed = _read_list(f'processedT_{CHUNK}.txt')
    processed = set(processed)
    n_processed = len(processed)
    if n_processed > 0:
        user_list = list(set(user_list) - processed)
        print(f'remaining:', len(user_list))
    processed = []
    
    # cfid is USERS_PER_FILE if first file, else k*USERS_PER_FILE
    current_file_id = USERS_PER_FILE + int(n_processed/USERS_PER_FILE) * USERS_PER_FILE
    for i, user in enumerate(user_list):
        i += n_processed # resume idx
        posts = collect_timeline(client, user)
        for post in posts:
            post.user = user
        all_posts.extend(posts)
        processed.append(user)

        if (i+1) % (USERS_PER_FILE+SAVE_EVERY_N_USERS) == 0:
            current_file_id += USERS_PER_FILE
            _save(all_posts, processed, i,  current_file_id)
            all_posts = []
            processed = []
        elif (i+1) % SAVE_EVERY_N_USERS == 0: 
            _save(all_posts, processed, i,  current_file_id)
            all_posts = []
            processed = []
        
    if len(all_posts) > 0:
       _save(all_posts, processed, i,  current_file_id)
        

    
