import gzip
import os
import json
import sys
from tqdm import tqdm
import time
from datetime import datetime

# --- CONFIGURATION ---
BASE_DEFAULT = '../cleaning&processing/results/clean'
OUT_DEFAULT = 'results/inter-time.txt'
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
    
    tick = time.time()
    BASE = BASE_DEFAULT
    OUT = OUT_DEFAULT

    # Command line arguments
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    print(f'Processing files in {BASE} -> {OUT}')
    
    # Dictionary to store min and max date for each user
    # Structure: {user_id: [min_date, max_date]}
    user_dates = {}

    for path in gzip_iterator(BASE):
        # WINDOWS FIX: encoding='utf-8' and mode='rt'
        with gzip.open(path, 'rt', encoding='utf-8') as f:
            for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                try:
                    d = json.loads(line.strip())
                    user_id = d.get('user_id')
                    
                    # Parse date YYYYMMDD
                    date_str = str(d.get('date'))[:8] 
                    try:
                        dt = datetime.strptime(date_str, '%Y%m%d').date()
                    except: continue

                    if user_id is not None:
                        if user_id not in user_dates:
                            user_dates[user_id] = [dt, dt]
                        else:
                            # Update min and max
                            if dt < user_dates[user_id][0]:
                                user_dates[user_id][0] = dt
                            if dt > user_dates[user_id][1]:
                                user_dates[user_id][1] = dt
                except: continue

    print("Writing results...")
    
    # Create output directory
    if not os.path.exists(os.path.dirname(OUT)):
        os.makedirs(os.path.dirname(OUT))

    # Write results: DateOfFirstPost DaysDelta
    with open(OUT, 'w', encoding='utf-8') as outf:
        for uid, dates in user_dates.items():
            delta = (dates[1] - dates[0]).days
            
            # Format: YYYYMMDD DELTA
            dt_str = dates[0].strftime('%Y%m%d')
            outf.write(f'{dt_str} {delta}\n')

    tock = time.time()
    print(f'done. {int(tock-tick)} s')