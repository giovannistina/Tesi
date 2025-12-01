import gzip
import os
import datetime
import json
import sys
from tqdm import tqdm
from collections import defaultdict
import time


def gzip_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        f = os.path.join(BASE, f)
        if f.endswith('.gz'):   
            print(f'processing {f}...')
            yield f
                    

if __name__ == '__main__':
    
    tick = time.time()
    BASE = 'final_posts'
    OUT = 'results/multilangs.txt.gz'

    

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    print('processing files in', BASE, 'and saving to', OUT)
    
    with gzip.open(OUT, 'w') as outf:
        
        for path in gzip_iterator(BASE):
            with gzip.open(path) as f:
                for line in tqdm(f):
                    d = json.loads(line.strip())
                    langs = d['langs']
                    if langs is not None and len(langs) > 1:
                        langs = [l if l else 'none' for l in langs]
                        row = ' '.join(sorted(langs))
                        outf.write(f"{row}\n".encode('utf-8'))

                    
                
    tock = time.time()
    print('done.', int(tock-tick), 's')