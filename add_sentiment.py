import gzip
import os
import json
import sys
from tqdm import tqdm
import time
from collections import defaultdict


def gzip_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        f = os.path.join(BASE, f)
        if f.endswith('.gz'):   
            print(f'processing {f}...')
            yield f

def sentiment_dict():
    res = defaultdict(dict) # {file_id: {post_id: {sent_label: label, sent_score: score}}}
    with gzip.open('sentiment.jsonl.gz') as f:
        for line in tqdm(f):
            try:
                line = json.loads(line.strip())
            except:
                print('error:', line)
                continue
            file_id = line.get('file_id')
            post_id = line.get('post_id')
            sent_label = line.get('sent_label')
            sent_score = line.get('sent_score')
            res[file_id][post_id] = (sent_label, sent_score)
    return res

                    

if __name__ == '__main__':
    
    tick = time.time()
    BASE = 'clean'
    OUT = 'final_posts'

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    
    sent_dict = sentiment_dict()
    print('sentiment dict loaded.')
    if not os.path.exists(OUT):
        os.makedirs(OUT)
        print('created', OUT)
    
    bad_lines = 0
    with_sentiment = 0
    print('processing files in', BASE, 'and saving to', OUT)
    for path in gzip_iterator(BASE):
        joined = os.path.join(OUT, os.path.basename(path))
    
        with gzip.open(joined, 'a') as out:

            file_id = int(os.path.basename(path).split('.')[0])
            sentiment = sent_dict[file_id]
            with gzip.open(path) as f:
                for line in tqdm(f):
                    try:
                        d = json.loads(line.strip())
                    except Exception as e:
                        print('error:', e)
                        bad_lines += 1
                        continue
                    post_id = d['post_id']
                    langs = d['langs']
                    
                    if langs is not None and len(langs) == 1 and post_id in sentiment:
                        d['sent_label'] = sentiment[post_id][0]
                        d['sent_score'] = sentiment[post_id][1]
                        with_sentiment += 1
                    else:
                        d['sent_label'] = None
                        d['sent_score'] = None
                    row = json.dumps(d) + '\n'
                    out.write(row.encode('utf-8'))
                    
    tock = time.time()
    print('done.', int(tock-tick), 's')
    print('bad lines:', bad_lines)
    print('with sentiment:', with_sentiment)