import gzip
import os
import datetime
import json
import sys
from tqdm import tqdm

def valid_time(t):
    try:
        T = datetime.datetime.strptime(t, '%Y%m%d')
        if T.date() > datetime.datetime(2023, 6, 27).date():
            return False
        elif T.date() < datetime.datetime(2023, 2, 17).date():
            return False
        return True
    except ValueError: # invalid time
        return False
    except Exception as e:
        print(e)
        return None

def gzip_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        f = os.path.join(BASE, f)
        if f.endswith('.gz'):
            print(f'processing {f}...')
            yield f
            
if __name__ == '__main__':
    
    start = datetime.datetime.now()
    
    BASE = 'clean'
    OUT = 'textdata.jsonl.gz'

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    bad_lines = 0
    good_lines = 0
    print('processing files in', BASE, 'and saving to', OUT)
    with gzip.open(OUT, 'a') as outf:
        for path in gzip_iterator(BASE):
            with gzip.open(path, 'r') as f:
                file_id = int(os.path.basename(path).split('.')[0]) 

                for line in tqdm(f):
                    try:
                        data = json.loads(line.rstrip())
                        date = str(data.get('date')) # %Y%m%d%H%M eg 202309122149
                        date = date[:8]

                        if not valid_time(date):
                            continue

                        
                        langs = data.get('langs')
                        if langs is not None and len(langs) == 1 and 'eng' in langs:
                            text = data.get('text', '')
                            if text:
                                post_id = data['post_id']
                                text = text.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                                
                                row = json.dumps({'file_id': file_id, 
                                                  'post_id': post_id,
                                                    'date': date,
                                                    'text': text}) + '\n'
                                outf.write(row.encode('utf-8'))
                                good_lines += 1
                             
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        bad_lines += 1
                    
            
    elapsed = datetime.datetime.now() - start
    print('done, took', elapsed)
    print('bad lines', bad_lines)
    print('good lines', good_lines) # posts in english to be used for sentiment analysis
    