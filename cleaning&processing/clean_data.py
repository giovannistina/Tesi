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

def gzip_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        f = os.path.join(BASE, f)
        if os.path.isdir(f) and 'chunk' in f:
            for file in sorted(os.listdir(f)):
                if file.endswith('.gz'):
                    full_path = os.path.join(f, file)
                    print(f'processing {full_path}...')
                    yield full_path

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
    BASE = 'data'
    OUT = 'clean'
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
    ids = dict()

    user_pattern = r'(?!\b)@[\w.-]+\w+' # match @usernames
    matcher = re.compile(pattern=user_pattern)

    if not os.path.exists(OUT):
        os.makedirs(OUT)
        print('created new folder:', OUT)
        
    print('processing files in', BASE, 'and saving to', OUT)


    bad_lines = 0
    total_lines = 0
    kept_lines = 0
    null_post_id = 0
    null_user_id = 0
    null_instance = 0
    null_date = 0
    null_text = 0
    null_langs = 0
    null_like_count = 0
    null_reply_count = 0
    null_repost_count = 0
    null_reply_to = 0
    null_replied_author = 0
    null_thread_root = 0
    null_thread_root_author = 0
    null_repost_from = 0
    null_reposted_author = 0
    null_quotes = 0
    null_quoted_author = 0
    null_labels = 0

    for i, path in enumerate(gzip_iterator(BASE)):
        with gzip.open(path) as f:
            joined = os.path.join(OUT, f'{i}.jsonl.gz')
            with gzip.open(joined, 'a') as out:
                for line in tqdm(f):
                    total_lines += 1
                    post_id, user_id, instance = None, None, None
                    date, text, langs = None, None, None
                    like_count, reply_count, repost_count = None, None, None
                    reply_to, replied_author = None, None
                    thread_root, thread_root_author = None, None
                    repost_from, reposted_author = None, None
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

                        kept_lines += 1

                        # user_id, a unique user identifier, 
                        # obtained from a mapping of user handles to integers
                        handle = d.get('user')
                        if handle not in user_map:
                            user_map[handle] = len(user_map)
                        user_id = user_map[handle]
                        
                        # POST_ID, a unique post identifier,
                        # obtained from a mapping of post URIs+handles to integers
                    
                        uri = d['post'].get('uri') + handle 
                        if uri not in ids:
                            ids[uri] = len(ids)
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
                            

                        # REPOST 
                        if handle != d['post']['author']['handle']:
                            reposted_author = d['post']['author']['handle']
                            repost_from = d['post'].get('uri') + reposted_author
                            if repost_from not in ids:
                                ids[repost_from] = len(ids)
                            repost_from = ids[repost_from]

                            if reposted_author not in user_map:
                                user_map[reposted_author] = len(user_map)
                            reposted_author = user_map[reposted_author]
                            
                            
                            
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
                        
                        # count nulls
                        if post_id is None:
                            null_post_id += 1
                        if user_id is None:
                            null_user_id += 1
                        if instance is None:
                            null_instance += 1
                        if date is None:
                            null_date += 1
                        if text is None:
                            null_text += 1
                        if langs is None:
                            null_langs += 1
                        if like_count is None:
                            null_like_count += 1
                        if reply_count is None:
                            null_reply_count += 1
                        if repost_count is None:
                            null_repost_count += 1
                        if reply_to is None:
                            null_reply_to += 1
                        if replied_author is None:
                            null_replied_author += 1
                        if thread_root is None:
                            null_thread_root += 1
                        if thread_root_author is None:
                            null_thread_root_author += 1
                        if repost_from is None:
                            null_repost_from += 1
                        if reposted_author is None:
                            null_reposted_author += 1
                        if quotes is None:
                            null_quotes += 1
                        if quoted_author is None:
                            null_quoted_author += 1
                        if labels is None:
                            null_labels += 1
                                

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
                            'repost_from': repost_from,
                            'reposted_author': reposted_author,
                            'quotes': quotes,
                            'quoted_author': quoted_author,
                            'labels': labels

                        }) + '\n'
                        out.write(row.encode('utf8'))

    with open('enc_users2.txt', 'w') as f:
        for u, i in user_map.items():
            f.write(f'{i} {u}\n')

    with open('enc_uris.txt', 'w') as f:
        for u, i in ids.items():
            f.write(f'{i} {u}\n')
    

    print('done in', datetime.datetime.now() - start)
    print('total lines:', total_lines)
    print('bad lines:', bad_lines)
    print('kept lines:', kept_lines)
    print()
    print('null post_id:', null_post_id)
    print('null user_id:', null_user_id)
    print('null instance:', null_instance)
    print('null date:', null_date)
    print('null text:', null_text)
    print('null langs:', null_langs)
    print('null like_count:', null_like_count)
    print('null reply_count:', null_reply_count)
    print('null repost_count:', null_repost_count)
    print('null reply_to:', null_reply_to)
    print('null replied_author:', null_replied_author)
    print('null thread_root:', null_thread_root)
    print('null thread_root_author:', null_thread_root_author)
    print('null repost_from:', null_repost_from)
    print('null reposted_author:', null_reposted_author)
    print('null quotes:', null_quotes)
    print('null quoted_author:', null_quoted_author)
    print('null labels:', null_labels)
    
    print()