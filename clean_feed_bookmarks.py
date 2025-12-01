import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
import re


def load_enc_users():
    res = dict()
    with open('results/enc_users.txt') as f:
        while line := f.readline():
            
            i, k = line.rstrip().split()
            res[k] = int(i)
    
    return res


def jsonl_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        if f.endswith('.jsonl.gz'):
            yield os.path.join(BASE, f)



if __name__ == '__main__':
    
    start = datetime.datetime.now()
    BASE = 'feed_bookmarks.csv'
    OUT = 'clean_feed_bookmarks.csv'
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]
        if sys.argv[i] == '-h':
            print('Usage: python clean_data.py -b <input_dir> -o <output_dir>')
            sys.exit(0)

    user_map = load_enc_users()
    total_lines = 0
    bad_lines = 0
    with open(BASE, 'r') as f, open(OUT, 'w') as out:
        for line in f:
            feed_name, user, t = line.rstrip().split(',')
            total_lines += 1
            if user in user_map:
                user_id = user_map[user]
            else:
                user_map[user] = len(user_map)
                user_id = user_map[user]
            date = int(datetime.datetime.fromisoformat(t).strftime('%Y%m%d%H%M'))
            out.write(f'{feed_name},{user_id},{date}\n')

                      

               

    with open(f'results/enc_users2.txt', 'w') as f:
        for u, i in user_map.items():
            f.write(f'{i} {u}\n')


    print('done in', datetime.datetime.now() - start)
    print('total lines:', total_lines)
    print('bad lines:', bad_lines)
