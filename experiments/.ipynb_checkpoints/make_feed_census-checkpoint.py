# .../experiments/make_feed_census.py
"""
Description: 
    Performs a Brute Force census of Bluesky Feed Generators by iterating through
    alphanumeric characters.
    
    Extracts specific metadata requested:
    1. Creation Date (indexed_at)
    2. Feed Likes (like_count)
    3. Creator Followers (followers_count of the user who made the feed)
    
    Output: results/feed_stats/bluesky_feed_census.csv
"""

import os
import time
import string
import pandas as pd
from atproto import Client
from tqdm import tqdm

# --- CONFIGURATION ---
OUTPUT_CSV = "results/feed_stats/bluesky_feed_census.csv"
# Search characters: a-z and 0-9
SEARCH_CHARS = list(string.ascii_lowercase) + list(string.digits)
# ----------------------

def get_session():
    """Retrieves session token from file."""
    paths = ['session.txt', '../data_collection/session.txt']
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                return f.read().strip()
    return None

def main():
    print("--- BLUESKY FEED CENSUS (EXTENDED METRICS) ---")
    
    session = get_session()
    if not session:
        print("❌ Session not found. Run create_session.py first.")
        return

    client = Client()
    try:
        client.login(session_string=session)
        print("✅ Login successful.")
    except Exception as e:
        print(f"❌ Login failed: {e}")
        return

    unique_feeds = {} # Key = URI (to handle duplicates)
    
    print(f"Starting census on {len(SEARCH_CHARS)} characters...")
    print("Extracting: Creation Date, Like Count, Creator Followers.\n")

    # Progress bar loop
    for char in tqdm(SEARCH_CHARS, desc="Scanning Alphabet"):
        cursor = None
        # Max pages to scan per letter 
        max_pages = 100 
        
        for page in range(max_pages):
            try:
                # NOTE: API limit is max 100 per request. 
                response = client.app.bsky.feed.search_feed_generators({
                    'q': char,
                    'limit': 100, 
                    'cursor': cursor
                })
                
                if not response.feeds:
                    break
                
                for feed in response.feeds:
                    if feed.uri not in unique_feeds:
                        # --- DATA EXTRACTION ---
                        # We handle potential None values with 'or 0' or empty strings
                        
                        # Extract creator followers safely
                        creator_followers = 0
                        if hasattr(feed, 'creator') and hasattr(feed.creator, 'followers_count'):
                            creator_followers = feed.creator.followers_count or 0

                        unique_feeds[feed.uri] = {
                            'name': feed.display_name,
                            'creation_date': feed.indexed_at,        # Requested: Data di creazione
                            'feed_likes': feed.like_count or 0,      # Requested: Numero Like Feed
                            'creator_followers': creator_followers,  # Requested: Numero Followers Autore
                            'creator_handle': feed.creator.handle,
                            'uri': feed.uri
                        }
                
                cursor = response.cursor
                if not cursor:
                    break
                
                # Rate limit politeness
                time.sleep(0.1)
                
            except Exception as e:
                # Log error but continue to next char/page
                # print(f"Error on '{char}': {e}")
                time.sleep(1)
                continue

    # --- SAVE RESULTS ---
    total_found = len(unique_feeds)
    print(f"\n✅ CENSUS COMPLETE.")
    print(f"Total Unique Feeds Found: {total_found}")

    if total_found > 0:
        print("Saving to CSV...")
        df = pd.DataFrame(list(unique_feeds.values()))
        
        # Sort by Feed Likes descending
        df = df.sort_values('feed_likes', ascending=False)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        
        # Save
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        
        print(f"File saved: {OUTPUT_CSV}")
        
        # --- QUICK STATS FOR THESIS ---
        print("\n--- DESCRIPTIVE STATISTICS ---")
        print(f"Average Feed Likes: {df['feed_likes'].mean():.2f}")
        print(f"Max Feed Likes: {df['feed_likes'].max()}")
        print(f"Average Creator Followers: {df['creator_followers'].mean():.2f}")
    else:
        print("No feeds found. Please check your connection or session.")

if __name__ == "__main__":
    main()