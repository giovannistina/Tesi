import sys
import os
import time
import datetime
from atproto import Client
from atproto.exceptions import RequestException, BadRequestError

# --- CONFIGURATION ---
SAVE_EVERY_N_USERS = 100
# ----------------------

def get_session():
    """Reads the session string from the local file."""
    try:
        with open('session.txt', 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        return None

def init_client():
    """Initializes the Bluesky client using the saved session."""
    client = Client()
    session_string = get_session()
    if session_string:
        print('Reusing session from session.txt')
        client.login(session_string=session_string)
    else:
        raise Exception("Session file not found. Run create_session.py first.")
    return client

def _save(follows_data, processed_users, chunk_id):
    """Saves the downloaded data to text files."""
    
    # Save relations: USER -> FOLLOWS_1 FOLLOWS_2 ...
    with open(f'follows_{chunk_id}.txt', 'a', encoding='utf-8') as f:
        for user, flws in follows_data.items():
            # Format: USER_DID [TAB] FOLLOWED_DID_1 FOLLOWED_DID_2 ...
            f.write(f"{user}\t{' '.join(flws)}\n")

    # Save processed users checkpoint
    with open(f'processedfollows_{chunk_id}.txt', 'a', encoding='utf-8') as f:
        for user in processed_users:
            f.write(f"{user}\n")
            
    # Discreet checkpoint message
    print(f'   [SAVED checkpoint at {datetime.datetime.now().strftime("%H:%M:%S")}]')

def collect_follows(client, handle):
    """Downloads the list of users that 'handle' follows (Outbound edges)."""
    follows = []
    cursor = None
    
    while True:
        try:
            # Download 100 follows at a time
            fetched = client.get_follows(actor=handle, limit=100, cursor=cursor)
            
            for user in fetched.follows:
                follows.append(user.did) # Always use DID for scientific consistency

            if not fetched.cursor:
                break
            cursor = fetched.cursor

        except Exception as e:
            # We catch errors to prevent the script from crashing on a single bad user
            # print(f"Error fetching follows for {handle}: {e}")
            break
            
    return follows

def main():
    # START TIMER
    start_run_time = time.time()

    if len(sys.argv) < 2:
        print("Usage: python crawl_follows.py <CHUNK_NUMBER>")
        return

    CHUNK = sys.argv[1]
    
    # Input file is expected to be batch_X.txt
    input_file = f'batch_{CHUNK}.txt'

    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found.")
        return

    client = init_client()
    
    # Read input users
    with open(input_file, 'r') as f:
        user_list = [l.strip() for l in f.readlines() if l.strip()]

    # Filter out users already processed
    processed_file = f'processedfollows_{CHUNK}.txt'
    processed = set()
    if os.path.exists(processed_file):
        with open(processed_file, 'r') as f:
            processed = set(l.strip() for l in f.readlines())
    
    user_list = [u for u in user_list if u not in processed]
    total_users = len(user_list)
    
    print(f"Processing {total_users} users for 'follows' collection...")
    print("-" * 30)

    current_batch_data = {}
    current_batch_users = []

    for i, user in enumerate(user_list):
        # CLEAN OUTPUT: User X/Total
        print(f"User {i+1}/{total_users}")
        
        flws = collect_follows(client, user)
        
        current_batch_data[user] = flws
        current_batch_users.append(user)

        # Periodic Save
        if (i + 1) % SAVE_EVERY_N_USERS == 0:
            _save(current_batch_data, current_batch_users, CHUNK)
            current_batch_data = {}
            current_batch_users = []

    # Final Save
    if current_batch_data:
        _save(current_batch_data, current_batch_users, CHUNK)
    
    # STOP TIMER & REPORT
    end_run_time = time.time()
    total_duration = end_run_time - start_run_time
    minutes = total_duration / 60

    print("\n" + "="*50)
    print("                 FINAL REPORT for follows")
    print("="*50)
    print(f"USERS PROCESSED:      {total_users}")
    print(f"TOTAL TIME:           {total_duration:.2f} seconds ({minutes:.2f} minutes)")
    if total_users > 0:
        avg = total_duration / total_users
        print(f"AVERAGE PER USER:     {avg:.2f} seconds")
    print("="*50 + "\n")

if __name__ == '__main__':
    main()