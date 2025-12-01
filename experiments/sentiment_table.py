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


                    

if __name__ == '__main__':
    
    tick = time.time()
    BASE = 'final_posts1'
    OUT = 'sentiment_table.csv.gz'

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    
    bad_lines = 0
    with_sentiment = 0
    result = defaultdict(list)
    print('processing files in', BASE, 'and saving to', OUT)
    for path in gzip_iterator(BASE):
        
        with gzip.open(path) as f:

            for line in tqdm(f):
                try:
                    d = json.loads(line.strip())
                except Exception as e:
                    bad_lines += 1
                    continue
                langs = d['langs']

                if langs is None:
                    continue
        
                if langs is not None and len(langs) > 1:
                    continue
                

                date = int(str(d['date'])[:8])
                sentiment = d['sent_label']
                if sentiment is None:
                    continue
                with_sentiment += 1
                result[date].append(sentiment)

    

    # create a csv file containing, for each day, the number of positive, negative, and neutral sentiments
    with gzip.open(OUT, 'w') as out:
        out.write('date,positive,negative,neutral,total\n'.encode('utf-8'))
        for date in sorted(result.keys()):
            pos = result[date].count(2)
            neg = result[date].count(0)
            neu = result[date].count(1)
            total = pos + neg + neu
            row = f"{date},{pos},{neg},{neu},{total}\n"
            out.write(row.encode('utf-8'))

    tock = time.time()
    print('done.', int(tock-tick), 's')
    print('bad lines:', bad_lines)
    print('with sentiment:', with_sentiment)