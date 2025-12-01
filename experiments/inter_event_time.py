import gzip
import os
import json
import sys
from tqdm import tqdm
import time
from datetime import datetime


def gzip_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        f = os.path.join(BASE, f)
        if f.endswith('.gz'):   
            print(f'processing {f}...')
            yield f
                    

if __name__ == '__main__':
    
    tick = time.time()
    BASE = 'clean'
    OUT = 'inter-time.txt'

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    print('processing files in', BASE, 'and saving to', OUT)
    
    # computes the inter-event time
    # for each user, we compute the time between the first and the last post (in days)

    current_user = None
    current_user_first = None
    current_user_last = None
    
    with open(OUT, 'a') as outf:
        for path in gzip_iterator(BASE):
            with gzip.open(path) as f:
                
                for line in tqdm(f):
                    d = json.loads(line.strip())
                    date = str(d['date'])[:8]
                    timestamp = datetime.strptime(date, '%Y%m%d').date()
                    if current_user is None:
                        # initialize
                        current_user = d['user_id']
                        current_user_first = timestamp
                        current_user_last = timestamp
                        n_user_posts = 1

                    
                    elif current_user != d['user_id'] and n_user_posts > 1:
                        # write the inter-event time for the current user
                        delta = (current_user_last - current_user_first).days
                        dt = current_user_first.strftime('%Y%m%d')
                        outf.write(f'{dt} {delta}\n')

                        # then update the current user
                        current_user = d['user_id']
                        current_user_first = timestamp
                        current_user_last = timestamp
                        n_user_posts = 1
                    else:
                        # update the first and last post of the current user
                        current_user_last = max(current_user_last, timestamp)
                        current_user_first = min(current_user_first, timestamp)
                        n_user_posts += 1

                        





                        

                    
                    




                    
                    
    tock = time.time()
    print('done.', int(tock-tick), 's')