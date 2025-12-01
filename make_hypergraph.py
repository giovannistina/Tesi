import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
from datetime import datetime
from collections import defaultdict


def yeld_all_lines(BASE):
    for f in sorted(os.listdir(BASE), key=lambda x: int(x.split('.')[0])):
        if f.endswith('.gz'):
            full_path = os.path.join(BASE, f)
                
            print(f'processing {full_path}...')
            with gzip.open(full_path) as f:
                for line in tqdm(f):
                    yield line

if __name__ == '__main__':
    
    start = datetime.now()
    
    BASE = 'clean'
    OUT = 'results/threads.txt.gz'
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]



    badlines = 0
    hyperedges = defaultdict(set)
    print('processing files in', BASE)
    print('collecting hyperedges and mapping posts to timestamps...')
    with open('timestamps.txt', 'w') as f:
        for line in yeld_all_lines(BASE):
            try:
                post = json.loads(line.strip())
            except (json.JSONDecodeError, UnicodeDecodeError, Exception) as e:
                print(e)
                badlines += 1
                continue

            post_id = post['post_id']
            date = post['date']
            f.write(f"{post_id}\t{date}\n")

            thread_root = post['thread_root']
            if thread_root is None:
                continue
            thread_root_author = post['thread_root_author']
            hyperedges[thread_root].add(thread_root_author)
            hyperedges[thread_root].add(post['user_id'])

    
    count =  len(hyperedges)
    print('found', count, 'hyperedges')
    
    
    static = OUT.replace('temporal_', '')
    print('writing hypergraph to', static)
    with gzip.open(static, 'w') as f:
        for k, v in hyperedges.items():
            participants = list(v)
            row = f"{k}\t{','.join([str(p) for p in participants])}\n"
            f.write(row.encode('utf-8'))
    # free memory by keeping only keys of hyperedges (root posts)
    hyperedges = set(hyperedges.keys())

    # load timestamps
    print('loading timestamps...')
    timestamps = {}
    with open('timestamps.txt') as f:
        for line in tqdm(f):
            post_id, date = line.strip().split('\t')
            post_id = int(post_id)
            if post_id in hyperedges:
                timestamps[post_id] = int(date)
    
    # free memory
    del hyperedges
    print(len(timestamps), 'roots with timestamps')
    print('loading hypergraph...')
    with gzip.open(static, 'r') as f:
        with gzip.open(OUT, 'w') as f2:
            for line in tqdm(f):
                root, *participants = line.decode('utf-8').strip().split('\t')
                root = int(root)
                if root in timestamps:
                    date = timestamps[root]
                    participants = ','.join(participants)
                    row = f"{root}\t{date}\t{participants}\n"
                    f2.write(row.encode('utf-8'))
                else:
                    print('no timestamp for', root)
    

    elapsed = datetime.now() - start
    print('done, took', elapsed)
    print('bad lines:', badlines)
 