from atproto_client import Client, SessionEvent
from atproto.exceptions import RequestException, BadRequestError

import datetime, time
import sys

import os

import logging.handlers

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

USERNAME = os.environ.get('USERNAME')
PASSWORD = os.environ.get('PASSWORD')
SAVE_EVERY_N_USERS = 1000

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

def _save(follows, processed_users):
    with open(f'data/follows_{CHUNK}.txt', 'a') as f:
        for user, flws in follows.items():
            f.write(f"{user}\t{' '.join(flws)}\n")

    with open(f'processedf_{CHUNK}.txt', 'a') as f:
        for user in processed_users:
            f.write(f"{user}\n")
    print(f'{datetime.datetime.now()} SAVED {i + 1}')


def _read_list(path):
    if not os.path.exists(path):
        return []
    res = []
    with open(path) as f:
        for l in f.readlines():
            res.append(l.rstrip())
    return res


def collect_follows(client, handle, follows=None):
    count_user_errors = 0  # init
    cursor = None
    old_cursor = None

    if follows is None:
        follows = []

    while True:
        if count_user_errors > MAX_USER_ERRORS:
            return follows
        try:
            fetched = client.get_follows(handle, limit=100, cursor=cursor)
            for user in fetched.follows:
                follows.append(user.handle)

        except RequestException as e:
            count_user_errors += 1
            _handle_requests_exceptions(e)
            cursor = old_cursor
            continue
        except BadRequestError:
            return []
        except Exception as e:
            count_user_errors += 1
            print(f"{datetime.datetime.now()} {e}")
            cursor = old_cursor
            continue

        if not fetched.cursor:
            break

        old_cursor = cursor
        cursor = fetched.cursor

    return follows




if __name__ == '__main__':
    CHUNK = int(sys.argv[1])

    client = init_client(USERNAME, PASSWORD)
    user_list = _read_list(f'batch_{CHUNK}.txt')
    all_followers = dict()
    processed = _read_list(f'processedf_{CHUNK}.txt')
    n_processed = len(processed)
    if n_processed > 0:
        print(f'resuming at {n_processed}')

    for i, user in enumerate(user_list[n_processed:]):
        i += n_processed  # resume idx
        followers = collect_follows(client, user)
        all_followers[user] = followers
        processed.append(user)

        if (i + 1) % SAVE_EVERY_N_USERS == 0:
            _save(all_followers, processed)
            all_followers = dict()
            processed = []

    if len(all_followers) > 0:
        _save(all_followers, processed)
