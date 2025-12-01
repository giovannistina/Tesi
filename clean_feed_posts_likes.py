import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
import re
from collections import defaultdict

def load_enc_users():
    res = dict()
    with open('results/enc_users.txt') as f:
        while line := f.readline():
            
            i, k = line.rstrip().split()
            res[k] = int(i)
    
    return res

def load_enc_uris():
    res = dict()
    bad = 0
    with open('results/enc_uris.txt') as f:
        while line := f.readline():
            try:
                i, k = line.rstrip().split()
                res[k] = int(i)
            except:
                bad += 1
                print('bad line:', line)
            
    print('bad lines in enc_uris:', bad)
    return res

def csv_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        if f.endswith('.csv.gz'):
            yield os.path.join(BASE, f)

        
def valid_time(t):
    try:
        T = datetime.datetime.fromisoformat(t)
        if T.date() > datetime.datetime(2024, 3, 18).date():
            return False
        elif T.date() < datetime.datetime(2023, 2, 17).date():
            return False
        return True
    except ValueError: # invalid time
        return False
    except Exception as e:
        print(e)
        return None

if __name__ == '__main__':
    
    start = datetime.datetime.now()
    BASE = 'feed_posts_likes'
    OUT = 'clean_feed_posts_likes'
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]
        if sys.argv[i] == '-h':
            print('Usage: python clean_data.py -b <input_dir> -o <output_dir>')
            sys.exit(0)

    user_map = load_enc_users()
    ids = load_enc_uris()

    if not os.path.exists(OUT):
        os.makedirs(OUT)
        print('created new folder:', OUT)
        
    print('processing files in', BASE, 'and saving to', OUT)


    total_lines = 0
    bad_lines = 0
    
    for i, path in enumerate(csv_iterator(BASE)):
        with gzip.open(path) as f:
            out_path = os.path.join(OUT, path.split('/')[-1])
            feed_name = os.path.basename(path)[:os.path.basename(path).find('.')]
            users = set()
            like_count = 0
            with gzip.open(out_path, 'w') as out:
                for line in tqdm(f):
                    total_lines += 1
                    
                    liker, liked_author, liked_uri, t = line.decode('utf8').rstrip().split(',')
                    uri = liked_uri + liked_author
                    if uri not in ids:
                        ids[uri] = len(ids)

                    if liker not in user_map:
                        user_map[liker] = len(user_map)

                    if liked_author not in user_map:
                        user_map[liked_author] = len(user_map)

                    date = int(datetime.datetime.fromisoformat(t).strftime('%Y%m%d%H%M'))

                    row = f'{user_map[liker]},{user_map[liked_author]},{ids[uri]},{date}\n'

                    out.write(row.encode('utf8'))
                    like_count += 1

        print(f'Processed {like_count} likes from {feed_name}.')
        

    with open(f'{OUT}/enc_users2.txt', 'w') as f:
        for u, i in user_map.items():
            f.write(f'{i} {u}\n')

    with open(f'{OUT}/enc_uris2.txt', 'w') as f:
        for u, i in ids.items():
            f.write(f'{i} {u}\n')
    

    print('done in', datetime.datetime.now() - start)
    print('total lines:', total_lines)
    print('bad lines:', bad_lines)