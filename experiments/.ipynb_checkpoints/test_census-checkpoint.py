"""
Script: test_census.py
Description: 
    TEST VERSION of the census script.
    It only scans letters 'a' and 'b' to verify the Direct API connection works.
"""

import os
import time
import requests 
import pandas as pd
from atproto import Client
from tqdm import tqdm

# --- CONFIGURATION ---
OUTPUT_CSV = "results/feed_stats/test_census_ab.csv"
SEARCH_CHARS = ['a', 'b'] # <--- ONLY TEST CHARS
API_ENDPOINT = "https://bsky.social/xrpc/app.bsky.feed.searchFeedGenerators"
# ---------------------

def get_session():
    paths = ['session.txt', '../data_collection/session.txt']
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                return f.read().strip()
    return None

def main():
    print("--- CENSUS TEST (LETTERS A, B ONLY) ---")
    
    session_str = get_session()
    if not session_str:
        print("❌ Session not found.")
        return

    client = Client()
    try:
        client.login(session_string=session_str)
        print("✅ Login successful.")
    except Exception as e:
        print(f"❌ Login failed: {e}")
        return

    try:
        jwt_token = client._session.access_jwt
    except AttributeError:
        jwt_token = client.session.access_jwt

    headers = {
        "Authorization": f"Bearer {jwt_token}"
    }

    unique_feeds = {} 
    
    # We lowered max_pages to 5 just to make the test super fast
    MAX_PAGES_TEST = 5 

    for char in tqdm(SEARCH_CHARS, desc="Scanning Test Chars"):
        cursor = None
        
        for page in range(MAX_PAGES_TEST):
            try:
                params = {
                    'q': char,
                    'limit': 100,
                    'cursor': cursor
                }
                
                resp = requests.get(API_ENDPOINT, headers=headers, params=params)
                
                if resp.status_code != 200:
                    time.sleep(1)
                    continue
                
                data = resp.json()
                feeds_list = data.get('feeds', [])
                
                if not feeds_list:
                    break
                
                for feed in feeds_list:
                    uri = feed.get('uri')
                    if uri not in unique_feeds:
                        creator = feed.get('creator', {})
                        unique_feeds[uri] = {
                            'name': feed.get('displayName', ''),
                            'creation_date': feed.get('indexedAt', ''),
                            'feed_likes': feed.get('likeCount', 0),
                            'creator_followers': creator.get('followersCount', 0) if creator else 0,
                            'creator_handle': creator.get('handle', ''),
                            'uri': uri
                        }
                
                cursor = data.get('cursor')
                if not cursor:
                    break
                
                time.sleep(0.2)
                
            except Exception as e:
                time.sleep(1)
                continue

    # --- SAVE ---
    total_found = len(unique_feeds)
    print(f"\n✅ TEST COMPLETE.")
    print(f"Total Unique Feeds Found: {total_found}")

    if total_found > 0:
        df = pd.DataFrame(list(unique_feeds.values()))
        df = df.sort_values('feed_likes', ascending=False)
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        print(f"File saved: {OUTPUT_CSV}")
    else:
        print("No feeds found. Direct API method failed.")

if __name__ == "__main__":
    main()