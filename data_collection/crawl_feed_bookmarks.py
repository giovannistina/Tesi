import pandas as pd
from otherfile import myfeeduris, init_client
from requests.exceptions import RequestException
from atproto import BadRequestError
import datetime


def collect_likes(client, uri, cursor=None, likes=None):
    cursor = None
    old_cursor = None

    if likes is None:
        likes = []
    
    
    while True:

        try:
            fetched = client.get_likes(uri, cursor=cursor, limit=100)
            likes = likes + fetched.likes

        except RequestException as e:
            print(e)
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
    
    return likes


def clean_like(like):
    like = like.dict()
    who = like['actor']['handle']
    when = like['created_at']
    return who, when
def valid_time(t):
    try:
        T = datetime.datetime.fromisoformat(t)
        if T.date() > datetime.datetime(2024, 3, 18).date():
            return False
        elif T.date() < datetime.datetime(2023, 2, 17).date():
            return False
        return True
    except ValueError: # invalid time
        return False
    except Exception as e:
        print(e)
        return None




if __name__ == '_:main__':

    client = init_client()

    feeds = client.app.bsky.unspecced.get_popular_feed_generators()
    
    data = []
    for f in feeds:
        if f.uri in myfeeduris.values():
            f = f.dict()
            data.append([f['display_name'], f['uri'], f['creator']['handle'], f['indexed_at'], f['description']])
    # feed statistics
    df = pd.DataFrame(data, columns=['display_name', 'uri', 'creator', 'indexed_at', 'description'])
    df.to_csv('feed_info.csv', index=False, sep=';')

    # who bookmarked a feed
    with open('feed_likes.csv', 'w') as f:
        for name, uri in myfeeduris.to_dict().items():
            print(datetime.datetime.now(), name)
            likes = collect_likes(client, uri)
            for l in likes:
                who, when = clean_like(l)
                if valid_time(when):
                    f.write(f"{name},{who},{when}\n")


            