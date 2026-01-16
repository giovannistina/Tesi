# Questo file è stato modificato per chiedere all'utente il numero di feed da scaricare.

import os
import datetime
from atproto import Client

# --- CONFIGURATION ---
OUTPUT_FILE = "otherfile.py" # The file needed by the next script
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
        raise Exception("Session file not found. Run create_session.py first.")
    return client

def main():
    print("--- GENERATE TOP FEEDS LIST ---")

    # 1. ASK USER FOR LIMIT
    try:
        user_input = input("How many top feeds do you want to download? (e.g. 50): ")
        LIMIT = int(user_input)
    except ValueError:
        print("Invalid number entered. Defaulting to 10 feeds.")
        LIMIT = 10

    print(f"--- GENERATING {OUTPUT_FILE} WITH TOP {LIMIT} FEEDS ---")
    
    client = init_client()
    
    print("Fetching popular feeds from Bluesky API...")
    
    # Get popular generators using the user-provided LIMIT
    popular = client.app.bsky.unspecced.get_popular_feed_generators(params={'limit': LIMIT})
    
    print(f"Found {len(popular.feeds)} feeds.")
    
    # Generate the Python file 'otherfile.py'
    # It will contain a dictionary named 'myfeeduris'
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("# This file was automatically generated.\n")
        f.write(f"# Date: {datetime.datetime.now()}\n\n")
        f.write("myfeeduris = {\n")
        
        for feed in popular.feeds:
            # Clean the name to avoid syntax errors in Python string
            safe_name = feed.display_name.replace('"', "'").replace('\\', '')
            uri = feed.uri
            
            f.write(f'    "{safe_name}": "{uri}",\n')
            
        f.write("}\n")
        
        # Also export a helper function if needed by original code
        f.write("\ndef init_client():\n")
        f.write("    # Placeholder if the original script tries to import init_client from here\n")
        f.write("    pass\n")

    print(f"SUCCESS! '{OUTPUT_FILE}' created.")
    print("You can now run 'crawl_feed_bookmarks.py'.")

if __name__ == "__main__":
    main()