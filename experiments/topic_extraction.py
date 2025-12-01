
import sys
from tqdm import tqdm
import time
import os
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from bertopic.representation import KeyBERTInspired, MaximalMarginalRelevance
import numpy as np
from umap import UMAP
from bertopic import BERTopic
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import CountVectorizer
from hdbscan import HDBSCAN


def load_docs(BASE):
    docs = []
    with open(BASE) as f:
        for line in tqdm(f):
            line = line.rstrip()
            # REMOVE non ascii characters 
            line = ''.join([i if ord(i) < 128 else ' ' for i in line])

            if line:
                docs.append(line)
    return list(set(docs)) 




def rescale(x, inplace=False):
    """ Rescale an embedding so optimization will not have convergence issues.
    """
    if not inplace:
        x = np.array(x, copy=True)

    x /= np.std(x[:, 0]) * 10000

    return x

def kmenans_fit_transform(docs, embeddings):
    from sklearn.cluster import KMeans

    representation_model = MaximalMarginalRelevance(diversity=0.4) 
    embedding_model = SentenceTransformer('all-MiniLM-L6-v2', 
                                          device='cuda')
    vectorizer_model = CountVectorizer(stop_words="english",                    )



    cluster_model = KMeans(n_clusters=20, random_state=42)
    
    model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,
        hdbscan_model=cluster_model,
        #low_memory=True,
        calculate_probabilities=False,
        language='english',
        verbose=True
        )
    
    topics = model.fit_transform(documents=docs, embeddings=embeddings)
    
    return model, topics


def fit_transform(docs, embeddings):

    # Initialize and rescale PCA embeddings
    pca_embeddings = rescale(PCA(n_components=5).fit_transform(embeddings))

    # Start UMAP from PCA embeddings
    umap_model = UMAP(
        n_neighbors=15,
        n_components=5,
        min_dist=0.0,
        metric="cosine",
        init=pca_embeddings,
        random_state=42
    )

    vectorizer_model = CountVectorizer(stop_words="english",                    )

    embedding_model = SentenceTransformer('all-MiniLM-L6-v2', 
                                          device='cuda')

    representation_model = KeyBERTInspired()

    hdbscan_model = HDBSCAN(min_cluster_size=100,
                            metric='euclidean', 
                            prediction_data=True
                            )
    model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        representation_model=representation_model,
        umap_model=umap_model,
        hdbscan_model=hdbscan_model,
        #low_memory=True,
        nr_topics=5,
        calculate_probabilities=False,
        language='english',
        verbose=True
        )
    
    topics = model.fit_transform(documents=docs, embeddings=embeddings)
    
    return model, topics
    

if __name__ == '__main__':
    
    tick = time.time()
    BASE = 'chunk.txt'
    
    for i in range(len(sys.argv)):
        if sys.argv[i] == '-b':
            BASE = sys.argv[i+1]
        

    print('processing files in', BASE)

    if not os.path.exists(BASE):
        print('file', BASE, 'does not exist.')
        sys.exit(1)
    else:
        time_window = BASE.split('.')[0]
        if not os.path.exists(time_window):
            os.mkdir(time_window)

    
    docs = load_docs(BASE)
    print(len(docs), 'documents loaded.')

    if not os.path.exists(f'{time_window}/embeddings.npy'):
        print('computing embeddings...')
        sentence_model = SentenceTransformer('all-MiniLM-L6-v2', device='cuda')
        embeddings = sentence_model.encode(docs, show_progress_bar=True) # this is a ndarray
        print('saving embeddings...')
        np.save(f'{time_window}/embeddings.npy', embeddings)
    
    else:
        print('loading embeddings...')
        embeddings = np.load(f'{time_window}/embeddings.npy')
        print('done.')

    


    print('fitting model...')
    model, topics = fit_transform(docs, embeddings)
    
    print('saving model...')
    model.save(f'{time_window}/topic_model.bin')

    print('saving topics info...')
    info = model.get_topic_info() # this is a pandas dataframe
    info.to_csv(f'{time_window}/topics_info.csv', index=False)
    
    print('saving topics...')
    with open(f'{time_window}/topics.txt', 'w') as f:
        for topic in topics:
            f.write(f"{topic}\n")
        
    tock = time.time()
    print('done.', int(tock-tick), 's')