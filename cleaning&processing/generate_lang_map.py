import gzip
import os
import json
import sys
from tqdm import tqdm

# --- CONFIGURATION ---
# Path to raw data
BASE_DIR = '../data_collection/data'
# Output file
OUT_FILE = 'results/language_mapping.json'
# ----------------------

def gzip_iterator(base_path):
    """Iterates through all .gz files in chunk folders."""
    if not os.path.exists(base_path):
        return
    for f in sorted(os.listdir(base_path)):
        full_path = os.path.join(base_path, f)
        if os.path.isdir(full_path) and 'chunk' in f:
            for file in sorted(os.listdir(full_path)):
                if file.endswith('.gz'):
                    yield os.path.join(full_path, file)

if __name__ == '__main__':
    
    print(f"--- LANGUAGE MAP GENERATOR ---")
    print(f"Scanning data in: {BASE_DIR}")
    
    # Set to collect all unique language codes (e.g., 'en', 'it', 'jp')
    unique_langs = set()
    
    # Create results folder if it doesn't exist
    if not os.path.exists('results'):
        os.makedirs('results')

    # 1. SCAN DATA
    file_count = 0
    for path in gzip_iterator(BASE_DIR):
        file_count += 1
        with gzip.open(path, 'rb') as f:
            # Use tqdm to show progress
            for line in tqdm(f, desc=f"Scanning file {os.path.basename(path)}"):
                try:
                    d = json.loads(line.decode('utf-8'))
                    
                    # Extract language list from record
                    record = d.get('post', {}).get('record', {})
                    langs = record.get('langs', [])
                    
                    if langs:
                        # Add each found language to the set
                        for lang in langs:
                            unique_langs.add(lang)
                            
                except Exception:
                    continue

    print(f"\nScan completed on {file_count} files.")
    print(f"Found {len(unique_langs)} unique languages.")

    # 2. CREATE DICTIONARY (Language -> Number)
    # Sort alphabetically for consistency
    sorted_langs = sorted(list(unique_langs))
    
    lang_map = {}
    for i, lang in enumerate(sorted_langs):
        # Assign a numeric ID (starting from 1)
        lang_map[lang] = i + 1
        
    # 3. SAVE JSON FILE
    print(f"Saving map to {OUT_FILE}...")
    with open(OUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(lang_map, f, indent=4)
        
    print("DONE! 'language_mapping.json' has been created.")