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
    BASE = 'clean'
    OUT = 'all_langs.txt.gz'

    result = defaultdict(int)

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    print('processing files in', BASE, 'and saving to', OUT)
    
    for path in gzip_iterator(BASE):
        with gzip.open(path) as f:
            for line in tqdm(f):
                d = json.loads(line.strip())
                langs = d['langs']
                if langs is not None:
                    for lang in langs:
                        if lang:
                            result[lang] += 1
                    
                    
    with gzip.open(OUT, 'w') as outf:
        for k, v in result.items():
            outf.write(f"{k} {v}\n".encode('utf-8'))

    tock = time.time()
    print('done.', int(tock-tick), 's')