import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
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
    OUT = 'results/interactions.csv.gz'
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]



    badlines = 0
    print('processing files in', BASE)
    with gzip.open(OUT, 'a') as outf:

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
                    u = user_id = post['user_id']
                   
                    replied_author = post.get('replied_author') 
                    thread_root_author = post.get('thread_root_author')
                    reposted_author = post.get('reposted_author') 
                    quoted_author = post.get('quoted_author') 

                    # if any is not None, write to file
                    if replied_author or thread_root_author or reposted_author or quoted_author:
                        t = post['date']
                        row = f"{u},{replied_author},{thread_root_author},{reposted_author},{quoted_author},{t}\n"
                        outf.write(row.encode('utf-8'))

            
    elapsed = datetime.now() - start
    print('done, took', elapsed)
    print('bad lines:', badlines)
 
    