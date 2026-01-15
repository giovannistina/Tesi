import sys, os
from tqdm import tqdm
import gzip
from collections import defaultdict

if __name__ == '__main__':
        
    # --- CONFIGURATION ---
    # Input: We look for the file in the current folder (created by join_follower_graph)
    BASE = 'edgelist.csv.gz'
    
    # Output: We save results in the 'results' folder to keep order
    OUT_DIR = 'results'
    if not os.path.exists(OUT_DIR):
        os.makedirs(OUT_DIR)
        
    OUT = os.path.join(OUT_DIR, 'enc_edgelist.csv')
    MAP_FILE = os.path.join(OUT_DIR, 'enc_users.txt')

    # Allow overriding via command line arguments
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]
    
    # Check if input file exists before starting
    if not os.path.exists(BASE):
        print(f"Error: Input file '{BASE}' not found in current directory.")
        sys.exit(1)

    enc = defaultdict(int)

    print(f"Processing {BASE}...")

    # Open output file with utf-8 encoding (Fix for Windows)
    with open(OUT, 'w', encoding='utf-8') as outf:
        # Open gzip in binary mode (default) as per original script logic
        with gzip.open(BASE) as f:
            for line in tqdm(f):
                # Decode bytes to string
                line = line.decode('utf-8').rstrip().split(',')
                if line and len(line) == 2:
                    u, v = line

                    if u not in enc:
                        enc[u] = len(enc)
                    if v not in enc:
                        enc[v] = len(enc)
                    outf.write(f'{enc[u]},{enc[v]}\n')
            
    print(f'Wrote {len(enc)} users to {OUT}')
    
    # Try to remove duplicates using system command
    # Note: 'sort -u' is a Unix command. On Windows this might not work as expected.
    try:
        os.system(f"sort -u {OUT} -o {OUT}")
        print(f'Removed duplicates from {OUT}')
    except Exception:
        print("Warning: Could not run system sort (normal on Windows). Skipping.")

    # Save the user mapping (The Rosetta Stone)
    with open(MAP_FILE, 'w', encoding='utf-8') as f:
        for u, i in enc.items():
            f.write(f'{i} {u}\n')
            
    print(f'Wrote {len(enc)} users to {MAP_FILE}')