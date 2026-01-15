import gzip
import os
import json
import sys
from tqdm import tqdm
import time
import string, re

# NLTK imports (Natural Language Toolkit)
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

# --- CONFIGURATION ---
# Input: Your clean posts (from cleaning&processing)
BASE_DEFAULT = '../cleaning&processing/results/clean'
# Output: The cleaned text file
OUT_DEFAULT = 'results/feed_texts.txt.gz'
# ---------------------

# Ensure NLTK data is downloaded (Stopwords and WordNet dictionary)
print("Checking NLTK data...")
try:
    nltk.data.find('corpora/stopwords')
    nltk.data.find('corpora/wordnet')
except LookupError:
    print("Downloading NLTK data (stopwords, wordnet)...")
    nltk.download('stopwords')
    nltk.download('wordnet')
    nltk.download('omw-1.4')

STOPWORDS = set(stopwords.words('english'))
LEMMATIZER = WordNetLemmatizer()

def gzip_iterator(BASE):
    """Iterates through gzip files in the directory sorted numerically."""
    if not os.path.exists(BASE): 
        print(f"Error: Directory {BASE} not found.")
        return
    
    files = sorted([f for f in os.listdir(BASE) if f.endswith('.gz')], 
                   key=lambda x: int(x.split('.')[0]) if x[0].isdigit() else x)
    for f in files:
        f_path = os.path.join(BASE, f)
        print(f'processing {f_path}...')
        yield f_path

def clean_text(text):
    """
    Performs deep cleaning on text:
    - Lowercase
    - Remove HTML, URLs, Emojis, Numbers, Punctuation
    - Lemmatization (running -> run)
    - Remove Stopwords (the, is, at)
    """
    if not text: return ""
    text = str(text).lower()
    
    # Removing not printable characters
    text = "".join(filter(lambda x: x in string.printable, text))
    
    # Removing HTML/XML tags (legacy artifacts)
    text = re.sub(r"&lt;/?[a-z]+&gt;", "", text)
    text = text.replace(r"&amp;", "and")
    text = text.replace(r"&gt;", "")
    
    # Removing newline, tabs
    text = text.replace("\n", " ").replace("\t", " ")
    
    # Removing URLs
    text = re.sub(r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+", "", text)
    
    # Remove emojis (ascii encoding hack to strip non-ascii)
    try:
        text = text.encode('ascii', 'ignore').decode('ascii')
    except: pass
    
    # Removing numbers
    text = re.sub(r"\w*\d+\w*", "", text)
    
    # Removing Punctuation
    text = text.translate(str.maketrans("", "", string.punctuation))
    
    # Removing extra spaces
    text = re.sub(r"\s{2,}", " ", text)
    text = text.strip()

    # Lemmatize and remove stopwords
    tokens = [LEMMATIZER.lemmatize(word) for word in text.split() if word not in STOPWORDS]
    text = " ".join(tokens)

    return text

if __name__ == '__main__':
    
    tick = time.time()
    BASE = BASE_DEFAULT
    OUT = OUT_DEFAULT

    # Command line arguments
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]
        if sys.argv[i] == '-o': OUT = sys.argv[i+1]

    print(f'Processing files in {BASE} -> {OUT}')
    
    if not os.path.exists(os.path.dirname(OUT)):
        os.makedirs(os.path.dirname(OUT))

    # WINDOWS FIX: encoding='utf-8' and mode='wt'
    with gzip.open(OUT, 'wt', encoding='utf-8') as outf:
        for path in gzip_iterator(BASE):
            # Use filename as an ID (e.g. "0")
            file_id = os.path.basename(path).split('.')[0]
            
            # WINDOWS FIX: encoding='utf-8' and mode='rt'
            with gzip.open(path, 'rt', encoding='utf-8') as f:
                for line in tqdm(f, desc=f"Reading {os.path.basename(path)}"):
                    try:
                        d = json.loads(line.strip())
                        
                        # 1. Filter English Only
                        langs = d.get('langs')
                        is_eng = False
                        if langs and isinstance(langs, list):
                            if 'en' in langs or 'eng' in langs:
                                is_eng = True
                        
                        if not is_eng:
                            continue

                        # 2. Clean Text
                        raw_text = d.get('text')
                        cleaned = clean_text(raw_text)
                        
                        # Save format: ID, CleanedText
                        if cleaned:
                            outf.write(f"{file_id},{cleaned}\n")
                            
                    except Exception:
                        continue
    
    tock = time.time()
    print(f'Done. {int(tock-tick)} s')