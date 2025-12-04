import sys
import time
import os
import cbor2
from atproto import FirehoseSubscribeReposClient, models

# --- CONFIGURATION ---
OUTPUT_FILENAME = "1.txt" 
MAX_TIME_MINUTES = 30
# ----------------------

def main():
    print("--- BLUESKY FIREHOSE LISTENER (Universal Fix) ---")
    
    try:
        user_input = input("How many unique active users do you want to collect? (e.g., 5000): ")
        TARGET_USERS = int(user_input)
    except ValueError:
        print("Invalid number. Defaulting to 1,000 users.")
        TARGET_USERS = 1000

    start_time_run = time.time()
    end_time_limit = start_time_run + (MAX_TIME_MINUTES * 60)
    
    unique_users = set()
    
    if os.path.exists(OUTPUT_FILENAME):
        with open(OUTPUT_FILENAME, 'r') as f:
            for line in f:
                unique_users.add(line.strip())
    
    print(f"\nStarting listener...")
    print(f"Target: {TARGET_USERS} users")
    print(f"Time Limit: {MAX_TIME_MINUTES} minutes")
    print("Press Ctrl+C to stop manually.\n")

    output_file = open(OUTPUT_FILENAME, 'a', encoding='utf-8')

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
            # --- UNIVERSAL DECODING ---
            # Controllo intelligente: i dati sono già un dizionario o sono byte?
            
            if isinstance(message.body, dict):
                # Caso A: La libreria ha già fatto il lavoro sporco (Nuove versioni)
                decoded_data = message.body
            else:
                # Caso B: I dati sono byte grezzi (Vecchie versioni o configurazioni diverse)
                decoded_data = cbor2.loads(message.body, tag_hook=custom_tag_hook)
            
            # Ora estraiamo il DID
            user_did = decoded_data.get('repo')
            
            if user_did:
                if user_did not in unique_users:
                    unique_users.add(user_did)
                    output_file.write(f"{user_did}\n")
                    
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
        output_file.close()
    
    # --- SUMMARY ---
    total_duration = time.time() - start_time_run
    
    print("\n" + "="*40)
    print(f"STATUS: Process Finished.")
    print(f"SAVED TO FILE: {OUTPUT_FILENAME}")
    print(f"TOTAL TIME: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    print(f"TOTAL USERS SAVED: {len(unique_users)}")
    print("="*40 + "\n")
    print(f"Next step: Run 'python crawl_timelines.py 1'")

if __name__ == '__main__':
    main()