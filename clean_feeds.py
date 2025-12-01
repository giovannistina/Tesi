import gzip
import os
import json
import sys
import datetime
from tqdm import tqdm
import re
from collections import defaultdict

def load_langmap():
    with open('results/language_mapping.json') as f:
        m = json.loads(f.read())
    res = defaultdict(lambda: None)

    for k, v in m.items():
        res[k] = v
    return res

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

def jsonl_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        if f.endswith('.jsonl.gz'):
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
    BASE = 'feeds'
    OUT = 'clean_feeds'
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]
        if sys.argv[i] == '-h':
            print('Usage: python clean_data.py -b <input_dir> -o <output_dir>')
            sys.exit(0)

    language_map = load_langmap()
    user_map = load_enc_users()
    ids = load_enc_uris()

    user_pattern = r'(?!\b)@[\w.-]+\w+' # match @usernames
    matcher = re.compile(pattern=user_pattern)

    if not os.path.exists(OUT):
        os.makedirs(OUT)
        print('created new folder:', OUT)
        
    print('processing files in', BASE, 'and saving to', OUT)


    total_lines = 0
    bad_lines = 0
    not_already_covered = 0
    
    for i, path in enumerate(jsonl_iterator(BASE)):
        with gzip.open(path) as f:
            out_path = os.path.join(OUT, path.split('/')[-1])
            feed_name = os.path.basename(path)[:os.path.basename(path).find('.')]
            users = set()
            post_count = 0
            with gzip.open(out_path, 'w') as out:
                for line in tqdm(f):
                    total_lines += 1
                    post_id, user_id, instance = None, None, None
                    date, text, langs = None, None, None
                    like_count, reply_count, repost_count = None, None, None
                    reply_to, replied_author = None, None
                    thread_root, thread_root_author = None, None
                    quotes, quoted_author = None, None
                    labels =  None
                    
                    try:
                        d = json.loads(line.decode('utf8'))
                    except json.JSONDecodeError:
                        print('json decode error. Skipping...')
                        bad_lines += 1
                        continue
                    except UnicodeDecodeError:
                        print('Unicode decode error. Skipping...')
                        bad_lines += 1
                        continue
                    except Exception as e:
                        print('Unknown error:', e)
                        bad_lines += 1
                        continue
                    
                    t = d['post']['record'].get('createdAt', 
                                                    d['post']['record'].get('created_at'))
                    if t is not None and valid_time(t):
                        post_count += 1

                        handle = d['post']['author']['handle']
                        if handle in user_map:
                            user_id = user_map[handle]
                        else:
                            user_map[handle] = len(user_map)
                            user_id = user_map[handle]
                        users.add(user_id)
                        
                        
                        uri = d['post'].get('uri') + handle 
                        if uri not in ids:
                            ids[uri] = len(ids)
                            not_already_covered += 1
                        post_id = ids[uri]
                        
                            

                        # INSTANCE, the instance from which the user is from
                        # obtained from the last part of the handle
                        instance = '.'.join(handle.split('.')[1:])
                        
                        # DATE, the date of the post in the format YYYYMMDDHHMM
                        date = int(datetime.datetime.fromisoformat(t).strftime('%Y%m%d%H%M'))

                        # TEXT, the text of the post
                        # replace @usernames with their corresponding integer   
                        text = d['post']['record'].get('text', d['post']['record'].get('body'))
                        for m in matcher.findall(text):
                            m = m[1:] # remove @
                            if m not in user_map:
                                user_map[m] = len(user_map)
                            text = re.sub(pattern=m, repl=f"@{user_map[m]}", string=text)

                        # LANGS, the language(s) of the post
                        # standardized to the ISO 639-3 code
                        langs = d['post']['record'].get('langs', d['post']['record'].get('lang'))
                        if langs and isinstance(langs, list):
                            langs = [language_map[lang.lower()] for lang in langs]
                        elif isinstance(langs, str):
                            langs = [language_map[langs.lower()]]


                        # likes, shares, comments
                        like_count = d['post'].get('like_count', 0)
                        reply_count = d['post'].get('reply_count', 0)
                        repost_count = d['post'].get('repost_count', 0)

                        # REPLY_TO, the post_id of the post being replied to
                        # obtained from a mapping of post URIs+handles to integers
                        if d['reply'] is not None:
                            replied_author = d['reply']['parent']['author']['handle']
                            reply_to = d['reply']['parent'].get('uri') + replied_author
                            if reply_to not in ids:
                                ids[reply_to] = len(ids)
                            reply_to = ids[reply_to]
                            if replied_author not in user_map:
                                user_map[replied_author] = len(user_map)
                            replied_author = user_map[replied_author]
                            
                            # THREAD_ROOT, the post_id of the root post of the thread
                            # obtained from a mapping of post URIs+handles to integers
                            thread_root_author = d['reply']['root']['author']['handle']
                            if thread_root_author not in user_map:
                                user_map[thread_root_author] = len(user_map)
                            
                            thread_root = d['reply']['root'].get('uri') + thread_root_author
                            if thread_root not in ids:
                                ids[thread_root] = len(ids)
                            thread_root = ids[thread_root]
                            thread_root_author = user_map[thread_root_author]

                    
                        # QUOTES, the post_id of the post being quoted
                        # obtained from a mapping of post URIs+handles to integers
                        embed = d['post'].get('embed') 
                        if embed is not None and 'record' in embed:
                            quoted = embed['record']
                            if quoted is not None and 'author' in quoted:
                                try:
                                    quoted_author = quoted['author']['handle']
                                    
                                    if quoted_author not in user_map:
                                        user_map[quoted_author] = len(user_map)
                                    

                                    quotes = quoted.get('uri') + quoted_author
                                    if quotes not in ids:
                                        ids[quotes] = len(ids)
                                    quotes = ids[quotes]
                                    quoted_author = user_map[quoted_author]
                                except KeyError: # no author
                                    pass
                                
                        # LABELS, the labels (content warning) of the post
                        lbs = d['post']['record'].get('labels')
                        if lbs is not None:
                            lbs = lbs.get('values')
                            try:
                                labels = [l.get('val') for l in lbs]
                            except AttributeError:
                                labels = lbs
                        
                        
                        row = json.dumps({
                            'post_id': post_id,
                            'user_id': user_id,
                            'instance': instance,
                            'date': date,
                            'text': text,
                            'langs': langs,
                            'like_count': like_count,
                            'reply_count': reply_count,
                            'repost_count': repost_count,
                            'reply_to': reply_to,
                            'replied_author': replied_author,
                            'thread_root': thread_root,
                            'thread_root_author': thread_root_author,
                            'quotes': quotes,
                            'quoted_author': quoted_author,
                            'labels': labels

                        }) + '\n'
                        out.write(row.encode('utf8'))
        print(f'Processed {post_count} posts from {feed_name}.')
        print(f'Found {len(users)} unique users.')

    with open(f'{OUT}/enc_users2.txt', 'w') as f:
        for u, i in user_map.items():
            f.write(f'{i} {u}\n')

    with open(f'{OUT}/enc_uris2.txt', 'w') as f:
        for u, i in ids.items():
            f.write(f'{i} {u}\n')
    

    print('done in', datetime.datetime.now() - start)
    print('total lines:', total_lines)
    print('bad lines:', bad_lines)
    print('not already covered:', not_already_covered)