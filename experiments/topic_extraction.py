import sys
from tqdm import tqdm
import time
import os
import torch
import numpy as np
import pandas as pd

# BERTopic imports
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from bertopic.representation import KeyBERTInspired
from umap import UMAP
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import CountVectorizer
from hdbscan import HDBSCAN

# --- CONFIGURATION ---
BASE_DEFAULT = 'results/topics/english_posts.txt'

# ### TEST LIMIT: 2000 posts (~2-4 mins on CPU). Set to None for full run ###
TEST_LIMIT = 2000
# ##########################################################################

def load_docs(BASE):
    docs = []
    # WINDOWS FIX: encoding='utf-8'
    with open(BASE, encoding='utf-8') as f:
        for i, line in enumerate(tqdm(f, desc="Loading docs")):
            
            # ### TEST LIMIT CHECK ###
            if TEST_LIMIT and i >= TEST_LIMIT:
                print(f"⚠️ Test Limit reached: loaded {TEST_LIMIT} docs.")
                break
            # ########################

            line = line.rstrip()
            # Remove non-ascii characters
            line = ''.join([i if ord(i) < 128 else ' ' for i in line])
            if line:
                docs.append(line)
    return list(set(docs)) 

def rescale(x, inplace=False):
    if not inplace:
        x = np.array(x, copy=True)
    x /= np.std(x[:, 0]) * 10000
    return x

def fit_transform(docs, embeddings, device):
    print("Initializing UMAP and HDBSCAN...")
    
    # 1. Dimensionality Reduction
    pca_embeddings = rescale(PCA(n_components=5).fit_transform(embeddings))

    umap_model = UMAP(
        n_neighbors=15,
        n_components=5,
        min_dist=0.0,
        metric="cosine",
        init=pca_embeddings,
        random_state=42
    )

    # 2. Clustering
    # Dynamic cluster size adjustment for small tests
    min_cluster = 100
    if TEST_LIMIT and TEST_LIMIT <= 2000:
        min_cluster = 15 # Smaller clusters for test data
        print(f"⚠️ Test Mode: reduced min_cluster_size to {min_cluster}")

    hdbscan_model = HDBSCAN(
        min_cluster_size=min_cluster,
        metric='euclidean', 
        prediction_data=True
    )

    # 3. Vectorizer
    vectorizer_model = CountVectorizer(stop_words="english")

    # 4. Embedding Model
    embedding_model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

    # 5. Representation
    representation_model = KeyBERTInspired()

    print("Building BERTopic model...")
    model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        nr_topics=5, 
        calculate_probabilities=False,
        language='english',
        verbose=True
    )
    
    print("Fitting model...")
    topics, probs = model.fit_transform(documents=docs, embeddings=embeddings)
    
    return model, topics

if __name__ == '__main__':
    
    tick = time.time()
    BASE = BASE_DEFAULT
    
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b': BASE = sys.argv[i+1]

    print(f'Processing file: {BASE}')

    if not os.path.exists(BASE):
        print(f'Error: File {BASE} does not exist.')
        sys.exit(1)
    
    base_dir = os.path.dirname(BASE)
    filename = os.path.basename(BASE).split('.')[0]
    output_dir = os.path.join(base_dir, f"{filename}_analysis")
    
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    print(f"Using device: {device}")

    # 1. Load Docs
    docs = load_docs(BASE)
    print(f'{len(docs)} documents loaded.')

    if len(docs) < 20:
        print("Error: Not enough documents (>20 needed).")
        sys.exit(0)

    # 2. Embeddings
    emb_path = os.path.join(output_dir, 'embeddings.npy')
    
    # Don't save embeddings during test to avoid polluting the real run later
    if TEST_LIMIT:
        print('Computing embeddings (Test Mode)...')
        sentence_model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
        embeddings = sentence_model.encode(docs, show_progress_bar=True)
    else:
        # Full run logic
        if not os.path.exists(emb_path):
            print('Computing embeddings...')
            sentence_model = SentenceTransformer('all-MiniLM-L6-v2', device=device)
            embeddings = sentence_model.encode(docs, show_progress_bar=True)
            print('Saving embeddings...')
            np.save(emb_path, embeddings)
        else:
            print('Loading existing embeddings...')
            embeddings = np.load(emb_path)

    # 3. Fit Model
    model, topics = fit_transform(docs, embeddings, device)
    
    # 4. Save Results
    print('Saving results...')
    
    # CSV for plotting
    info = model.get_topic_info()
    info.to_csv('results/topics_info.csv', index=False)
    
    # TXT List
    with open(os.path.join(output_dir, 'topics.txt'), 'w', encoding='utf-8') as f:
        if isinstance(topics, np.ndarray): topics = topics.tolist()
        for topic in topics:
            f.write(f"{topic}\n")
        
    tock = time.time()
    print('Done.', int(tock-tick), 's')
    print(f"Results saved to results/topics_info.csv")