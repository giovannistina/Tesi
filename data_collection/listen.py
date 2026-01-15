import sys
import time
import os
import cbor2
from atproto import FirehoseSubscribeReposClient, models

# --- CONFIGURATION ---
# Ora definiamo DUE file di output
FILE_TIMELINES = "1.txt"         # Per crawl_timelines.py
FILE_NETWORK = "batch_1.txt"     # Per crawl_followers.py
MAX_TIME_MINUTES = 30
# ----------------------

def main():
    print("--- BLUESKY FIREHOSE LISTENER (Dual Write Mode) ---")
    
    try:
        user_input = input("How many unique active users do you want to collect? (e.g., 5000): ")
        TARGET_USERS = int(user_input)
    except ValueError:
        print("Invalid number. Defaulting to 1,000 users.")
        TARGET_USERS = 1000

    start_time_run = time.time()
    end_time_limit = start_time_run + (MAX_TIME_MINUTES * 60)
    
    unique_users = set()
    
    # Carichiamo la memoria dal file principale per non avere duplicati
    if os.path.exists(FILE_TIMELINES):
        with open(FILE_TIMELINES, 'r') as f:
            for line in f:
                unique_users.add(line.strip())
    
    print(f"\nStarting listener...")
    print(f"Target: {TARGET_USERS} users")
    print(f"Time Limit: {MAX_TIME_MINUTES} minutes")
    print(f"Saving simultaneously to: {FILE_TIMELINES} AND {FILE_NETWORK}")
    print("Press Ctrl+C to stop manually.\n")

    # APERTURA DOPPIA: Apriamo entrambi i file in modalità 'append'
    f1 = open(FILE_TIMELINES, 'a', encoding='utf-8')
    f2 = open(FILE_NETWORK, 'a', encoding='utf-8')

    def custom_tag_hook(decoder, tag, shareable_index=None):
        return tag.value

    def on_message_handler(message) -> None:
        # STOP CHECKS
        if len(unique_users) >= TARGET_USERS:
            client.stop()
            return

        if len(unique_users) % 100 == 0:
            if time.time() > end_time_limit:
                client.stop()
                return

        try:
            # UNIVERSAL DECODING
            if isinstance(message.body, dict):
                decoded_data = message.body
            else:
                decoded_data = cbor2.loads(message.body, tag_hook=custom_tag_hook)
            
            user_did = decoded_data.get('repo')
            
            if user_did:
                if user_did not in unique_users:
                    unique_users.add(user_did)
                    
                    # SCRITTURA DOPPIA
                    # Scriviamo lo stesso dato su entrambi i file
                    f1.write(f"{user_did}\n")
                    f2.write(f"{user_did}\n")
                    
                    # Flush ogni tanto per sicurezza (salva su disco)
                    if len(unique_users) % 10 == 0:
                        f1.flush()
                        f2.flush()
                    
                    if len(unique_users) % 50 == 0:
                        sys.stdout.write(f"\rCollected: {len(unique_users)} / {TARGET_USERS} users")
                        sys.stdout.flush()

        except Exception:
            pass

    client = FirehoseSubscribeReposClient()
    
    try:
        client.start(on_message_handler)
    except KeyboardInterrupt:
        print("\nStopping by user request...")
        client.stop()
    except Exception as e:
        print(f"\nError occurred: {e}")
        client.stop()
    finally:
        # CHIUSURA DOPPIA
        f1.close()
        f2.close()
    
    # --- SUMMARY ---
    total_duration = time.time() - start_time_run
    
    print("\n" + "="*40)
    print(f"STATUS: Process Finished.")
    print(f"FILES CREATED: {FILE_TIMELINES} & {FILE_NETWORK}")
    print(f"TOTAL TIME: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    print(f"TOTAL USERS SAVED: {len(unique_users)}")
    print("="*40 + "\n")
    print(f"Next steps:")
    print(f"1. For Posts: Run 'python crawl_timelines.py 1'")
    print(f"2. For Network: Run 'python crawl_followers.py 1'")

if __name__ == '__main__':
    main()