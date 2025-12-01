import gzip
import os
import json
import sys
from tqdm import tqdm
from collections import defaultdict
import time
import string, re
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

STOPWORDS = set(stopwords.words('english'))
LEMMATIZER = WordNetLemmatizer()

def gzip_iterator(BASE):
    for f in sorted(os.listdir(BASE)):
        f = os.path.join(BASE, f)
        if f.endswith('.gz'):   
            print(f'processing {f}...')
            yield f


def clean_text(text):
    text = str(text).lower()
    # Removing not printable characters
    text = "".join(filter(lambda x: x in string.printable, text))
    # Removing XSLT tags
    text = re.sub(r"&lt;/?[a-z]+&gt;", "", text)
    text = text.replace(r"&amp;", "and")
    text = text.replace(r"&gt;", "")
    # Removing newline, tabs and special reddit words
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")
    # Removing URLs
    text = re.sub(
        r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",
        "",
        text,
    )
    # remove emojis
    text = text.encode('ascii', 'ignore').decode('ascii')
    # Removing numbers
    text = re.sub(r"\w*\d+\w*", "", text)
    # Removing Punctuation
    text = text.translate(str.maketrans("", "", string.punctuation))
    # Removing extra spaces
    text = re.sub(r"\s{2,}", " ", text)
    # Removing leading and trailing spaces
    text = text.strip()

    # lemmatize
    text = " ".join([LEMMATIZER.lemmatize(word) for word in text.split() if word not in STOPWORDS])
    

    return text


    
                    

if __name__ == '__main__':
    
    tick = time.time()
    BASE = 'clean_feeds'
    OUT = 'results/feed_texts.txt.gz'

    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        if sys.argv[i] == '-o':
            OUT = sys.argv[i+1]

    
   


    print('processing files in', BASE, 'and saving to', OUT)
    result = defaultdict(int)
    with gzip.open(OUT, 'a') as outf:
        for path in gzip_iterator(BASE):
            with gzip.open(path) as f:
                feed_name = os.path.basename(path)[:os.path.basename(path).find('.')]
                for line in tqdm(f):
                    d = json.loads(line.strip())
                    langs = d.get('langs')
                    if langs is None or 'eng' not in langs:
                        continue
                    text = clean_text(d.get('text'))

                    outf.write(f"{feed_name},{text}\n"\
                               .encode('utf-8'))
    
    tock = time.time()
    print('done.', int(tock-tick), 's')