import gzip
import os
import json
import sys
from tqdm import tqdm
from collections import defaultdict, Counter
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
    OUT = 'results'

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    
    user_to_instance = dict()
    instance_n_posts = defaultdict(int)


    print('processing files in', BASE, 'and saving to', OUT)
    result = defaultdict(int)
    for path in gzip_iterator(BASE):
        with gzip.open(path) as f:
            for line in tqdm(f):
                d = json.loads(line.strip())
                user = d['user_id']
                instance_n_posts[d['instance']] += 1
                if user not in user_to_instance:
                    instance = d['instance']
                    user_to_instance[user] = instance


    result = list(user_to_instance.values())
    result = dict(Counter(result))

    path = os.path.join(OUT, 'instance_users.csv.gz')
    with gzip.open(path, 'w') as outf:
        for k, v in sorted(result.items(), key=lambda x: x[1], reverse=True):
            row = f"{k},{v}\n".encode('utf-8')
            outf.write(row)

    path = os.path.join(OUT, 'instance_posts.csv.gz')
    with gzip.open(path, 'w') as outf:
        for k, v in sorted(instance_n_posts.items(), key=lambda x: x[1], reverse=True):
            row = f"{k},{v}\n".encode('utf-8')
            outf.write(row)
            

    
    tock = time.time()
    print('done.', int(tock-tick), 's')