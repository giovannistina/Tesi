# .../experiments/make_feed_census_hybrid.py
"""
Description: 
    The most comprehensive census strategy possible given the API limitations (HTTP 501).
    It combines three discovery vectors to find Popular, Active, and SILENT feeds.
    
    STRATEGY:
    1. POPULAR: Get the official top charts.
    2. POSTS: Find users talking about feeds (Active Discovery).
    3. ACTORS (New): Find users with tech keywords in their BIO (Silent Discovery).
       (e.g. searching for users with "bot", "dev", "feed" in their profile).
    4. ENRICHMENT: Fetch accurate follower counts for all identified creators.
    
    This creates the largest possible dataset for the thesis.
"""

import os
import time
import pandas as pd
from atproto import Client
from tqdm import tqdm

# --- CONFIGURATION ---
OUTPUT_CSV = "results/feed_stats/bluesky_feed_census_hybrid.csv"

# 1. EXPANDED ACTOR KEYWORDS (Bio Search)
ACTOR_KEYWORDS = [
    # Tech / Dev roles
    "bot", "feed", "dev", "algorithm", "python", "scraper", "coder", 
    "engineer", "backend", "atproto", "maintainer", "admin",
    
    # Curation / Creator roles
    "curator", "maker", "archivist", "collection", "aggregator", 
    "tracker", "indexing", "directory", "experimental", "project", 
    "bluesky feed", "feed maker"
]

# 2. EXPANDED POST KEYWORDS (Active Search)
POST_KEYWORDS = [
    # Standard Announcements
    "created a feed", "new feed", "custom feed", "feed generator",
    "made a feed", "check out my feed", "subscribe to my feed",
    "feed update", "my first feed",
    
    # Low-Code Tools & Infrastructure
    "skyfeed.app", "skyfeed builder", "bluefeed", "goodfeeds",
    
    # Community Hashtags
    "#blueskyfeed", "#customfeed", "#feeddev", "#atproto"
]
# ---------------------

def get_session():
    paths = ['session.txt', '../data_collection/session.txt']
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                return f.read().strip()
    return None

def batch_get_profiles(client, dids):
    """Fetches profiles in batches of 25 to get accurate follower counts."""
    stats = {}
    # Chunk list into batches of 25 (API limit)
    chunks = [dids[i:i + 25] for i in range(0, len(dids), 25)]
    
    for batch in tqdm(chunks, desc="Enriching Creator Data"):
        try:
            res = client.app.bsky.actor.get_profiles({'actors': batch})
            for profile in res.profiles:
                stats[profile.did] = profile.followers_count or 0
            time.sleep(0.1) # Rate limit politeness
        except Exception:
            continue
    return stats

def main():
    print("--- BLUESKY FEED CENSUS (HYBRID STRATEGY + ENRICHMENT) ---")
    
    session = get_session()
    if not session:
        print("❌ Session not found.")
        return

    client = Client()
    try:
        client.login(session_string=session)
        print("✅ Login successful.")
    except Exception as e:
        print(f"❌ Login failed: {e}")
        return

    unique_feeds = {}        # URI -> Feed Data
    target_creators = set()  # DIDs of potential creators

    # --- STEP 1: POPULAR ---
    print("\n🚀 STEP 1: Harvesting 'Popular' seeds...")
    try:
        popular = client.app.bsky.unspecced.get_popular_feed_generators({'limit': 100})
        for feed in popular.feeds:
            unique_feeds[feed.uri] = extract_data(feed)
            target_creators.add(feed.creator.did)
        print(f"   Found {len(popular.feeds)} popular feeds.")
    except Exception as e:
        print(f"   ⚠️ Could not fetch popular: {e}")

    # --- STEP 2: BIO SEARCH ---
    print("\n🚀 STEP 2: Searching for 'Silent' Developers (Bio Search)...")
    for query in tqdm(ACTOR_KEYWORDS, desc="Scanning User Bios"):
        cursor = None
        for _ in range(3): 
            try:
                res = client.app.bsky.actor.search_actors({
                    'q': query,
                    'limit': 100,
                    'cursor': cursor
                })
                for actor in res.actors:
                    target_creators.add(actor.did)
                
                cursor = res.cursor
                if not cursor: break
                time.sleep(0.2)
            except Exception as e:
                break

    # --- STEP 3: POST SEARCH ---
    print("\n🚀 STEP 3: Searching for 'Active' Developers (Post Search)...")
    for query in tqdm(POST_KEYWORDS, desc="Scanning Posts"):
        cursor = None
        for _ in range(3):
            try:
                res = client.app.bsky.unspecced.search_posts_skeleton({
                    'q': query,
                    'limit': 100,
                    'cursor': cursor
                })
                if res.posts:
                    for post in res.posts:
                        if 'did:' in post.uri:
                            did = post.uri.split('/')[2]
                            target_creators.add(did)
                
                cursor = res.cursor
                if not cursor: break
                time.sleep(0.2)
            except Exception as e:
                break
    
    print(f"\n📊 TARGET LIST: Identified {len(target_creators)} potential creators.")
    print("   Now scanning their profiles for feeds...")


    # --- STEP 4: ENRICHMENT (Fixing Follower Counts) ---
    print("\n🚀 STEP 4: Enriching Data (Fetching Real Follower Counts)...")
    # Identify creators who actually have feeds in our dataset
    # (We don't need to scan creators who turned out to have 0 feeds)
    active_creators_dids = list(set(f['creator_did'] for f in unique_feeds.values()))
    print(f"   Updating stats for {len(active_creators_dids)} active creators...")
    
    # Get accurate map: DID -> Followers
    follower_map = batch_get_profiles(client, active_creators_dids)
    
    # Apply update to the collected feeds
    for uri, data in unique_feeds.items():
        did = data['creator_did']
        if did in follower_map:
            data['creator_followers'] = follower_map[did]

            
    # --- FINAL SWEEP: EXTRACT FEEDS ---
    creators_list = list(target_creators)
    for i, did in enumerate(tqdm(creators_list, desc="Extracting Feeds")):
        try:
            actor_feeds = client.app.bsky.feed.get_actor_feeds({
                'actor': did,
                'limit': 100
            })
            for feed in actor_feeds.feeds:
                if feed.uri not in unique_feeds:
                    unique_feeds[feed.uri] = extract_data(feed)
            time.sleep(0.05) 
        except Exception as e:
            continue
    # --- SAVE ---
    total = len(unique_feeds)
    print(f"\n✅ CENSUS COMPLETE.")
    print(f"Total Unique Feeds Found: {total}")

    if total > 0:
        df = pd.DataFrame(list(unique_feeds.values()))
        df = df.sort_values('feed_likes', ascending=False)
        
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        print(f"File saved: {OUTPUT_CSV}")
        
        # Thesis Stats
        print("\n--- DATA INSIGHTS ---")
        print(f"Mean Likes: {df['feed_likes'].mean():.2f}")
        print(f"Median Likes: {df['feed_likes'].median():.2f}")
        print(f"Mean Creator Followers: {df['creator_followers'].mean():.2f}")
    else:
        print("No feeds found.")

def extract_data(feed):
    """Standardizes data extraction"""
    # Initial follower count might be 0/None, fixed in Step 4
    creator_followers = 0
    if hasattr(feed, 'creator') and hasattr(feed.creator, 'followers_count'):
        creator_followers = feed.creator.followers_count or 0
        
    return {
        'name': feed.display_name,
        'creation_date': feed.indexed_at,
        'feed_likes': feed.like_count or 0,
        'creator_followers': creator_followers,
        'creator_handle': feed.creator.handle,
        'creator_did': feed.creator.did, # Crucial for Step 4
        'description': feed.description.replace('\n', ' ') if feed.description else '',
        'uri': feed.uri
    }

if __name__ == "__main__":
    main()