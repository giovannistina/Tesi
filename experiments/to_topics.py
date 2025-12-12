import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
import time

# --- CONFIGURATION ---
# Input: Your clean data
BASE_DEFAULT = '../cleaning&processing/results/clean'
# Output: Folder where text files for topic modeling will be saved
OUT_DEFAULT = 'results/topics'
# ---------------------

def gzip_iterator(BASE):
    if not os.path.exists(BASE): 
        print(f"Error: Directory {BASE} not found.")
        return
    # Sort files numerically
    files = sorted([f for f in os.listdir(BASE) if f.endswith('.gz')], 
                   key=lambda x: int(x.split('.')[0]) if x[0].isdigit() else x)
    for f in files:
        f_path = os.path.join(BASE, f)
        print(f'processing {f_path}...')
        yield f_path

if __name__ == '__main__':
    
    start = time.time()
    
    BASE = BASE_DEFAULT
    OUT = OUT_DEFAULT

    # Command line arguments
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    # Create output directory
    if not os.path.exists(OUT):
        os.makedirs(OUT)

    print(f'Processing files in {BASE} -> {OUT}')
    
    # Output file for all English posts
    out_file_path = os.path.join(OUT, 'english_posts.txt')
    
    count = 0
    badlines = 0

    # WINDOWS FIX: encoding='utf-8'
    with open(out_file_path, 'w', encoding='utf-8') as txt_file:
        
        for path in gzip_iterator(BASE):
            # WINDOWS FIX: encoding='utf-8' and mode='rt'
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    try:
                        post = json.loads(line.strip())
                        
                        # 1. Check Language (English only)
                        # We use 'langs' list from clean data
                        langs = post.get('langs')
                        is_eng = False
                        if langs and isinstance(langs, list):
                            # Check for 'en' or 'eng'
                            if 'en' in langs or 'eng' in langs:
                                is_eng = True
                        
                        if is_eng:
                            # 2. Extract Text
                            text = post.get('text', '')
                            if text:
                                # Clean newlines/tabs to keep 1 post per line in the txt file
                                text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                                txt_file.write(text + '\n')
                                count += 1
                                
                    except Exception as e:
                        badlines += 1
                        continue

    elapsed = time.time() - start
    print(f'Done in {int(elapsed)} s')
    print(f'Bad lines: {badlines}')
    print(f'Total English posts saved: {count}')
    print(f'File saved to: {out_file_path}')