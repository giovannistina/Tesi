# è necessario che sia stato runnato "get_top_feeds" altrimenti non riesce a leggere il file otherfile
# se invece si vuole analizzare dei feed specifici basta inserirlo direttamente in otherfile a mano

import pandas as pd
import datetime
import os
import csv
from atproto import Client
from atproto.exceptions import RequestException, BadRequestError

# Valid import because we created otherfile.py in Step 1
from otherfile import myfeeduris 

# --- CONFIGURATION ---
OUTPUT_CSV = 'feed_likes.csv'
INFO_CSV = 'feed_info.csv'
# ---------------------

def get_session():
    try:
        with open('session.txt', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def init_client():
    client = Client()
    session_string = get_session()
    if session_string:
        print('Reusing session from session.txt')
        client.login(session_string=session_string)
    else:
        raise Exception("Session file not found.")
    return client

def collect_likes(client, uri, cursor=None, likes=None):
    cursor = None
    old_cursor = None

    if likes is None:
        likes = []
    
    while True:
        try:
            # Fetch likes for the feed generator
            fetched = client.get_likes(uri, cursor=cursor, limit=100)
            likes = likes + fetched.likes

        except RequestException as e:
            print(f"Request error: {e}")
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
    # Convert object to dict model if needed, or access directly
    who = like.actor.handle
    when = like.created_at
    return who, when

def valid_time(t):
    # ORIGINAL LOGIC MODIFIED:
    # The original code filtered dates between 2023 and March 2024.
    # I removed the upper limit so it works for TODAY'S data.
    try:
        # Check if string or datetime object
        if isinstance(t, str):
            T = datetime.datetime.fromisoformat(t.replace('Z', '+00:00'))
        else:
            T = t
            
        # Example filter: only keep data after 2023
        if T.date() < datetime.datetime(2023, 2, 17).date():
            return False
            
        return True
    except ValueError: # invalid time
        return False
    except Exception as e:
        print(f"Time error: {e}")
        return None

def main():
    print("--- CRAWL FEED BOOKMARKS  ---")
    client = init_client()

    # 1. Save Feed Statistics (Info)
    print(f"Reading {len(myfeeduris)} feeds from otherfile.py...")
    
    # We fetch fresh info for the feeds in our list
    # Note: getting popular generators again just to extract metadata easily
    # In a strict scenario, we would fetch get_generator for each URI.
    # Here we simplify by listing what we have.
    
    data = []
    # Just creating a simple list based on our static file
    for name, uri in myfeeduris.items():
        data.append([name, uri])
        
    df = pd.DataFrame(data, columns=['display_name', 'uri'])
    df.to_csv(INFO_CSV, index=False, sep=';')
    print(f"Feed info saved to {INFO_CSV}")

    # 2. Who bookmarked a feed (The Core Loop)
    print(f"Starting collection of likes/bookmarks...")
    
    with open(OUTPUT_CSV, 'w', encoding='utf-8') as f:
        # Added header for clarity
        f.write("feed_name,user_handle,liked_at\n")
        
        for name, uri in myfeeduris.items():
            print(f"{datetime.datetime.now()} Processing: {name}")
            
            likes = collect_likes(client, uri)
            print(f"   -> Found {len(likes)} likes.")
            
            for l in likes:
                who, when = clean_like(l)
                if valid_time(when):
                    f.write(f"{name},{who},{when}\n")

    print(f"Done. Bookmarks saved to {OUTPUT_CSV}")

if __name__ == '__main__':
    main()