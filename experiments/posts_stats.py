import gzip
import os
import json
import sys
from tqdm import tqdm
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
    OUT = 'post_stats.txt.gz'

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    print('processing files in', BASE, 'and saving to', OUT)
    
    with gzip.open(OUT, 'a') as outf:
        for path in gzip_iterator(BASE):
            with gzip.open(path) as f:
                for line in tqdm(f):
                    d = json.loads(line.strip())
                    post_id = d.get('post_id')
                    user_id = d.get('user_id')
                    
                    t = str(d.get('date'))
                    t = int(t[:8])
                    outf.write(f"{t} {post_id} {user_id}\n"\
                               .encode('utf-8'))
                    
                    
    tock = time.time()
    print('done.', int(tock-tick), 's')