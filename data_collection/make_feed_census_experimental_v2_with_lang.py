# .../data_collection/make_feed_census_experimental_v2_with_lang.py

import requests
from bs4 import BeautifulSoup
import time
from datetime import timedelta
import pandas as pd
from atproto import Client
from tqdm import tqdm
import os
from langdetect import detect, DetectorFactory

# --- CONFIGURAZIONE ---
# Salviamo in un file nuovo per sicurezza
OUTPUT_CSV = "results/feed_stats/bluesky_feed_census_v2_with_lang.csv"
DIRECTORY_URL = "https://blueskydirectory.com/feeds/all"

# Fissiamo il seme per rendere il rilevamento lingua riproducibile
DetectorFactory.seed = 0

def get_session():
    paths = ['session.txt', '../data_collection/session.txt']
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                return f.read().strip()
    return None

def detect_language_safe(text):
    """
    Prova a capire la lingua del testo.
    Restituisce 'en', 'it', ecc. oppure 'unknown' se fallisce.
    """
    if not text or len(str(text)) < 3:
        return 'unknown'
    try:
        return detect(text)
    except:
        return 'unknown'

def scrape_directory():
    """Scarica i link dei feed da Bluesky Directory (versione infinita)."""
    found_urls = set()
    print("🕷️  Inizio Scraping COMPLETO di Bluesky Directory...")
    
    headers = {'User-Agent': 'Mozilla/5.0 (Research Thesis; Bot)'}
    page = 1 
    
    while True:
        target_url = f"{DIRECTORY_URL}?page={page}"
        print(f"   Scraping page {page}...", end="\r")
        
        try:
            r = requests.get(target_url, headers=headers)
            if r.status_code != 200:
                print(f"\n   ⛔ Stop: Status code {r.status_code} alla pagina {page}")
                break
            
            soup = BeautifulSoup(r.text, 'html.parser')
            links = soup.find_all('a', href=True)
            
            feeds_in_this_page = 0
            for link in links:
                href = link['href']
                if "bsky.app/profile/" in href and "/feed/" in href:
                    found_urls.add(href)
                    feeds_in_this_page += 1
            
            if feeds_in_this_page == 0:
                print(f"\n   ✅ Nessun feed trovato alla pagina {page}. Elenco finito.")
                break
            
            page += 1
            time.sleep(0.5)
            
        except Exception as e:
            print(f"\n   ⚠️ Errore scraping alla pagina {page}: {e}")
            break

    print(f"\n✅ Scraping completato. Trovati {len(found_urls)} feed unici.")
    return list(found_urls)

def extract_data_from_api(client, url_list):
    """Scarica i dati e rileva la lingua."""
    feed_data = []
    print("🔄  Convertendo URL web in Dati API + Rilevamento Lingua...")
    
    for url in tqdm(url_list):
        try:
            parts = url.split('/')
            if len(parts) < 7: continue
            
            handle = parts[4]
            feed_rkey = parts[6]
            
            # 1. Profilo Autore
            res = client.app.bsky.actor.get_profile({'actor': handle})
            author_did = res.did
            author_followers = res.followers_count or 0
            
            # 2. Dati Feed
            at_uri = f"at://{author_did}/app.bsky.feed.generator/{feed_rkey}"
            feed_info = client.app.bsky.feed.get_feed_generator({'feed': at_uri})
            view = feed_info.view
            
            # --- NOVITÀ: RILEVAMENTO LINGUA ---
            feed_name = view.display_name or ""
            feed_description = view.description or "" # Prendiamo la descrizione!
            
            # Uniamo Titolo e Descrizione per un rilevamento più preciso
            full_text = f"{feed_name} {feed_description}"
            lang_code = detect_language_safe(full_text)
            
            feed_data.append({
                'name': feed_name,
                'description': feed_description, # Salviamo anche la descrizione nel CSV
                'language': lang_code,           # Salviamo la lingua rilevata ('en', 'it', etc)
                'creation_date': view.indexed_at,
                'feed_likes': view.like_count or 0,
                'creator_followers': author_followers,
                'creator_handle': view.creator.handle,
                'creator_did': view.creator.did,
                'uri': view.uri,
                'source': 'Scraped'
            })
            
        except Exception as e:
            continue
            
    return feed_data

def main():
    start_time = time.time()
    print(f"⏱️  Script avviato alle: {time.strftime('%H:%M:%S')}")

    session = get_session()
    if not session:
        print("Manca session.txt")
        return
        
    client = Client()
    try:
        client.login(session_string=session)
    except Exception as e:
        print(f"Errore Login: {e}")
        return
    
    start_scrape = time.time()
    urls = scrape_directory()
    end_scrape = time.time()
    
    start_api = time.time()
    data = extract_data_from_api(client, urls)
    end_api = time.time()
    
    if data:
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        df = pd.DataFrame(data)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Salvati {len(df)} feed in {OUTPUT_CSV}")
        
        # Statistica rapida a schermo
        en_count = len(df[df['language'] == 'en'])
        print(f"📊 Di cui {en_count} rilevati come INGLESE ({(en_count/len(df))*100:.1f}%)")
    else:
        print("Nessun dato valido estratto.")

    total_time = time.time() - start_time
    print(f"\n🏁 FINITO! Tempo totale: {str(timedelta(seconds=int(total_time)))}")

if __name__ == "__main__":
    main()