import sys, os
from tqdm import tqdm
import gzip
from collections import defaultdict


if __name__ == '__main__':
        
    BASE = 'edgelist.csv.gz'
    OUT = 'enc_edgelist.csv'

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]
    
    enc = defaultdict(int)

    with open(OUT, 'w') as outf:
        with gzip.open(BASE) as f:
            for line in tqdm(f):
                line = line.decode('utf-8').rstrip().split(',')
                if line and len(line) == 2:
                    u, v = line

                    if u not in enc:
                        enc[u] = len(enc)
                    if v not in enc:
                        enc[v] = len(enc)
                    outf.write(f'{enc[u]},{enc[v]}\n')
            
    print(f'Wrote {len(enc)} users to {OUT}')
    
    # remove duplicates in output file
    os.system(f"sort -u {OUT} -o {OUT}")
    print(f'Removed duplicates from {OUT}')
    

    with open('enc_users.txt', 'w') as f:
        for u, i in enc.items():
            f.write(f'{i} {u}\n')
    print(f'Wrote {len(enc)} users to enc_users.txt')

    
                
