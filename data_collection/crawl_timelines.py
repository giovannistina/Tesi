#Questo file è stato modificato per prendere le attività solamente degli ultimi 30 giorni. 
#Per il codice originale andare sul sito e riscaricare lo script
 
# This file has been modified to capture activity only from the last 30 days.
# It also includes a final timer to estimate performance.

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

    with open(f'processedT_{CHUNK}.txt', 'a') as f:
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
    
    # --- TIME CONFIGURATION ---
    # Using datetime.datetime and datetime.timedelta to avoid import errors
    # Download only posts from the last 30 days
    TIME_LIMIT = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)
    stop_download = False
    # ----------------------------

    if posts is None:
        posts = []
    
    while True:
        if count_user_errors > MAX_USER_ERRORS:
            return posts
        
        # If we passed the date limit, break the main loop
        if stop_download:
            break

        try:
            fetched = client.get_author_feed(handle, limit=100, cursor=cursor)
            
            # Check dates in the fetched block
            for post_view in fetched.feed:
                # Extract post date
                post_date_str = post_view.post.record.created_at
                
                # Safe date parsing
                try:
                    post_date = parser.parse(post_date_str)
                except:
                    continue # Skip post if date is unreadable

                # Normalize timezone to UTC to avoid comparison errors
                if post_date.tzinfo is None:
                     post_date = post_date.replace(tzinfo=datetime.timezone.utc)

                # If post is older than the limit...
                if post_date < TIME_LIMIT:
                    stop_download = True 
                    continue # Skip to next (which will trigger the break)
                
                # Otherwise add post to list
                posts.append(post_view.post) # Note: we save .post, not the whole view

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
    # START TIMER
    start_run_time = time.time()
    
    if len(sys.argv) < 2:
        print("Error: please specify the chunk number (e.g., python crawl_timelines.py 1)")
        sys.exit(1)

    CHUNK = int(sys.argv[1])

    if not os.path.exists(f'data/chunk_{CHUNK}'):
        os.makedirs(f'data/chunk_{CHUNK}')
    
    client = init_client(USERNAME, PASSWORD)
    user_list = _read_list(f'{CHUNK}.txt')
    all_posts = []
    processed = _read_list(f'processedT_{CHUNK}.txt')
    processed_set_loaded = set()
    
    # Load processed users into a set for faster lookup
    for p in processed:
        parts = p.split('\t')
        if parts:
            processed_set_loaded.add(parts[0])

    n_processed = len(processed_set_loaded)
    
    if n_processed > 0:
        # Filter the list removing already processed users
        original_count = len(user_list)
        user_list = [u for u in user_list if u not in processed_set_loaded]
        print(f'Resuming: {original_count} total users found, {len(user_list)} remaining to process.')
    
    processed = [] # Reset for current batch
    
    # Total number of users to process in this run
    users_to_process_count = len(user_list)
    
    # cfid is USERS_PER_FILE if first file, else k*USERS_PER_FILE
    current_file_id = USERS_PER_FILE + int(n_processed/USERS_PER_FILE) * USERS_PER_FILE
    
    last_idx = 0 

    try:
        for i, user in enumerate(user_list):
            last_idx = i
            
            posts = collect_timeline(client, user)
            for post in posts:
                post.user = user
            all_posts.extend(posts)
            processed.append(user)

            # Use total count (previously processed + current) for saving checkpoints
            total_count = n_processed + i + 1

            if total_count % (USERS_PER_FILE+SAVE_EVERY_N_USERS) == 0:
                current_file_id += USERS_PER_FILE
                _save(all_posts, processed, total_count,  current_file_id)
                all_posts = []
                processed = []
            elif total_count % SAVE_EVERY_N_USERS == 0: 
                _save(all_posts, processed, total_count,  current_file_id)
                all_posts = []
                processed = []
            
        if len(all_posts) > 0:
            total_count = n_processed + last_idx + 1
            _save(all_posts, processed, total_count,  current_file_id)

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    
    # END TIMER & SUMMARY
    end_run_time = time.time()
    total_duration = end_run_time - start_run_time
    minutes = total_duration / 60

    print("\n" + "="*50)
    print("                 FINAL REPORT")
    print("="*50)
    print(f"USERS PROCESSED IN THIS RUN:   {users_to_process_count}")
    print(f"TOTAL TIME ELAPSED:            {total_duration:.2f} seconds ({minutes:.2f} minutes)")
    if users_to_process_count > 0:
        avg_time = total_duration / users_to_process_count
        print(f"AVERAGE TIME PER USER:         {avg_time:.2f} seconds")
    print("="*50 + "\n")