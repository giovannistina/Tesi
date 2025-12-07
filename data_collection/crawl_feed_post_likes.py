# Per ora non lo ho utilizzato perchè va a cercare i like ad ogni post. per ora troppo specifico. magari in futuro


import gzip
import os
import datetime

from atproto_client import Client, SessionEvent
from atproto.exceptions import RequestException, BadRequestError

import datetime, time
import gzip
import os

USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')


import logging.handlers
log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


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

def _save(likes, file_id):

    with gzip.open(f'feed_likes/{file_id}.csv.gz', 'a') as f:
        for uri, post_author, ls in likes:
            for like in ls:
                user, created_at = clean_like(like)
                row = f'{user},{post_author},{uri},{created_at}\n'
                f.write(row.encode('utf8'))

            

def clean_like(like):
    like = like.dict()
    who = like['actor']['handle']
    when = like['created_at']
    return who, when


def collect_likes(client, uri, cursor=None, likes=None):
    cursor = None
    old_cursor = None

    if likes is None:
        likes = []
    
    
    while True:

        try:
            post_author = client.get_posts([uri]).posts[0].author.handle
            fetched = client.get_likes(uri, cursor=cursor, limit=100)
            likes = likes + fetched.likes

        except RequestException as e:
            _handle_requests_exceptions(e)
            cursor = old_cursor
            continue
        except BadRequestError:
            return []
        except Exception as e:
            print(f"{datetime.datetime.now()} {e}")
            cursor = old_cursor
            continue
        
        if not fetched.cursor:
            break
        
        old_cursor = cursor
        cursor = fetched.cursor
    
    return post_author, likes


if __name__ == '__main__':
    
    
    client = init_client(USERNAME, PASSWORD)

    
    for file in os.listdir('feed_uris'):
        if file.endswith('.gz'):
            uris = []
            
            with gzip.open(os.path.join('feed_uris', file)) as f:
                feed_name = file.replace('.txt.gz', '')
                print('processing', feed_name)
                all_likes = []
                for l in f.readlines():
                    uri = l.decode('utf8').rstrip()
                    uris.append(uri)
                    post_author, likes = collect_likes(client, uri)
                    all_likes.append((uri, post_author, likes))
                _save(all_likes, feed_name)
                

                
        
    
