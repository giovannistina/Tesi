# .../Tesi/experiments/classify_creators_semantic.py

import pandas as pd
from sentence_transformers import SentenceTransformer, util
from langdetect import detect, LangDetectException
import torch
import os
import datetime
import pytz
import random
import numpy as np # Import necessario per il seed

# --- CONFIGURAZIONE ---
INPUT_FILE = '../data_collection/results/feed_stats/bluesky_feed_census_v2_with_lang_and_bios.csv'
OUTPUT_FILE = 'results/feed_stats/bluesky_feed_census_classified.csv'
RANDOM_SEED = 42  # Il "numero magico" per la riproducibilità

# Definiamo i 3 "Prototipi" (Anchor Sentences)
PROTOTYPES = {
    "Automated Bot": "This is an automated bot account posting updates via script, feed generator or algorithm.",
    "Professional/Dev": "I am a software engineer, developer, official organization, news outlet or project maintainer.",
    "Amateur User": "I am a private person sharing my personal interests, hobbies, life, thoughts and opinions."
}

def set_global_seed(seed):
    """
    Fissa il seed per tutte le librerie coinvolte per garantire
    che i risultati siano identici a ogni esecuzione.
    """
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    print(f"Seed globale fissato a: {seed}")

def get_rome_time():
    """Restituisce l'orario corrente formattato con fuso orario Roma"""
    rome_tz = pytz.timezone('Europe/Rome')
    return datetime.datetime.now(rome_tz).strftime("%H:%M:%S")

def main():
    # 1. Fissiamo il seed PRIMA di fare qualsiasi altra cosa
    set_global_seed(RANDOM_SEED)

    print(f"[{get_rome_time()}] --- INIZIO CLASSIFICAZIONE SEMANTICA ---")
    
    # 2. Caricamento Dati
    if not os.path.exists(INPUT_FILE):
        print("File di input non trovato.")
        return
    
    df = pd.read_csv(INPUT_FILE)
    print(f"Righe totali caricate: {len(df)}")

    # 3. Pre-processing
    unique_creators = df[['creator_did', 'creator_description']].drop_duplicates(subset='creator_did').copy()
    
    unique_creators = unique_creators.dropna(subset=['creator_description'])
    unique_creators = unique_creators[unique_creators['creator_description'].str.strip() != ""]
    print(f"Creator univoci con bio presente: {len(unique_creators)}")

    # 4. Filtro Lingua
    start_time = get_rome_time()
    num_users = len(unique_creators)

    print(f"\n[{start_time}] Inizio rilevamento lingua su {num_users} utenti univoci.")
    print("Stima temporale: circa 5-10 minuti (con CPU performante).")
    print("Attendere prego...")
    
    def is_english(text):
        try:
            if len(text) < 10: return False 
            return detect(text) == 'en'
        except LangDetectException:
            return False

    unique_creators['is_english'] = unique_creators['creator_description'].apply(is_english)
    
    english_creators = unique_creators[unique_creators['is_english'] == True].copy()
    print(f"[{get_rome_time()}] Finito rilevamento lingua. Bio in inglese identificate: {len(english_creators)}")

    # 5. Caricamento Modello S-BERT
    print("\nCaricamento modello S-BERT (all-MiniLM-L6-v2)...")
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # 6. Calcolo Embeddings e Classificazione
    labels = list(PROTOTYPES.keys())
    prototype_sentences = list(PROTOTYPES.values())
    
    # Anche l'encoding beneficia del seed fissato (anche se su CPU è spesso deterministico di base)
    prototype_embeddings = model.encode(prototype_sentences, convert_to_tensor=True)

    print(f"[{get_rome_time()}] Calcolo embeddings delle bio e classificazione...")
    
    bio_embeddings = model.encode(english_creators['creator_description'].tolist(), convert_to_tensor=True, show_progress_bar=True)

    cosine_scores = util.cos_sim(bio_embeddings, prototype_embeddings)

    best_match_indices = torch.argmax(cosine_scores, dim=1)
    
    assigned_labels = [labels[idx] for idx in best_match_indices]
    english_creators['predicted_category'] = assigned_labels

    confidence_scores = [cosine_scores[i][idx].item() for i, idx in enumerate(best_match_indices)]
    english_creators['confidence'] = confidence_scores

    # 7. Merge dei risultati
    print("Unione dei risultati...")
    final_mapping = english_creators.set_index('creator_did')[['predicted_category', 'confidence']]
    
    df = df.merge(final_mapping, on='creator_did', how='left')
    df['predicted_category'] = df['predicted_category'].fillna('Unknown/Non-English')

    # 8. Salvataggio
    df.to_csv(OUTPUT_FILE, index=False)
    
    end_time = get_rome_time()
    print("\n--- CLASSIFICAZIONE COMPLETATA ---")
    print(f"Finito alle ore: {end_time}")
    print(f"File salvato in: {OUTPUT_FILE}")
    print("\nDistribuzione Categorie (sui soli creator inglesi):")
    print(english_creators['predicted_category'].value_counts())

if __name__ == "__main__":
    main()