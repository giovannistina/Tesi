import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
from collections import defaultdict
from datetime import datetime


def gzip_iterator(BASE):
    for f in sorted(os.listdir(BASE), key=lambda x: int(x.split('.')[0])):
        if f.endswith('.gz'):
            full_path = os.path.join(BASE, f)
                
            print(f'processing {full_path}...')
            yield full_path

if __name__ == '__main__':
    
    start = datetime.now()
    
    BASE = 'clean'
    OUT = 'results/topics'
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    if not os.path.exists(OUT):
        os.makedirs(OUT)


    date_format = "%Y%m%d%H%M"
    badlines = 0
    july_count = 0
    feb_count = 0
    print('processing files in', BASE)

    with open(f'{OUT}/13-15jul.txt', 'a') as july:
        with open(f'{OUT}/6-8feb.txt', 'a') as feb:

            for path in gzip_iterator(BASE):
                with gzip.open(path) as f:
                    for line in tqdm(f):
                        try:
                            post = json.loads(line.strip())
                        except json.JSONDecodeError as e:
                            print(e)
                            badlines += 1
                            continue
                        except UnicodeDecodeError as e:
                            print(e)
                            badlines += 1
                            continue
                        except Exception as e:
                            print(e)
                            badlines += 1
                            continue
                        langs = post.get('langs')
                        sent_label = post.get('sent_label')
                        if sent_label != 0: # only negative sentiment
                            continue
                        if langs is not None and len(langs) == 1 and 'eng' in langs:

                            t = post.get('date')
                            t = datetime.strptime(str(t), date_format)
                            date = t.date() 
                            
                            if datetime(2024, 2, 6).date() <= date <= datetime(2024, 2, 8).date():
                                text = post.get('text')
                                text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                                feb.write(text+'\n')
                                feb_count += 1
                            
                            elif datetime(2023, 7, 13).date() <= date <= datetime(2023, 7, 15).date():
                                text = post.get('text')
                                text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
                                july.write(text+'\n')
                                july_count += 1

          
    elapsed = datetime.now() - start
    print('done, took', elapsed)
    print('bad lines:', badlines)
    print('feb:', feb_count)
    print('july:', july_count)

    