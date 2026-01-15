# .../data_collection/make_feed_census_experimental.py


import requests
from bs4 import BeautifulSoup
import time
from datetime import timedelta
import pandas as pd
from atproto import Client
from tqdm import tqdm
import os

# --- CONFIGURAZIONE ---
OUTPUT_CSV = "results/feed_stats/bluesky_feed_census_experimental.csv"
DIRECTORY_URL = "https://blueskydirectory.com/feeds/all"

def get_session():
    # (Usa la stessa funzione del file originale)
    paths = ['session.txt', '../data_collection/session.txt']
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                return f.read().strip()
    return None

def scrape_directory():
    """
    Scarica i link dei feed da Bluesky Directory.
    Usa un ciclo WHILE per andare avanti finché trova dati.
    """
    found_urls = set()
    print("🕷️  Inizio Scraping COMPLETO di Bluesky Directory...")
    
    headers = {'User-Agent': 'Mozilla/5.0 (Research Thesis; Bot)'}
    
    page = 1  # Si parte dalla pagina 1
    
    while True: # Ciclo infinito
        target_url = f"{DIRECTORY_URL}?page={page}"
        print(f"   Scraping page {page}...", end="\r") # end="\r" sovrascrive la riga per pulizia
        
        try:
            r = requests.get(target_url, headers=headers)
            
            # Se la pagina non esiste (es. Error 404), ci fermiamo
            if r.status_code != 200:
                print(f"\n   ⛔ Stop: Status code {r.status_code} alla pagina {page}")
                break
            
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # Cerchiamo tutti i link
            links = soup.find_all('a', href=True)
            
            # Contiamo quanti feed nuovi troviamo in questa pagina
            feeds_in_this_page = 0
            
            for link in links:
                href = link['href']
                if "bsky.app/profile/" in href and "/feed/" in href:
                    found_urls.add(href)
                    feeds_in_this_page += 1
            
            # --- CONDIZIONE DI USCITA ---
            # Se in questa pagina non c'è nemmeno un feed, abbiamo finito l'elenco.
            if feeds_in_this_page == 0:
                print(f"\n   ✅ Nessun feed trovato alla pagina {page}. Elenco finito.")
                break
            
            # Passiamo alla pagina successiva
            page += 1
            time.sleep(0.5) # Pausa di mezzo secondo per non bloccare il server
            
        except Exception as e:
            print(f"\n   ⚠️ Errore scraping alla pagina {page}: {e}")
            break

    print(f"\n✅ Scraping completato. Trovati {len(found_urls)} feed unici.")
    return list(found_urls)

    
def extract_data_from_api(client, url_list):
    """
    Prende la lista di URL (web) e chiede all'API di Bluesky i dati tecnici reali.
    """
    feed_data = []
    
    print("🔄  Convertendo URL web in Dati API (questo richiede tempo)...")
    
    for url in tqdm(url_list):
        try:
            # URL: https://bsky.app/profile/USERNAME/feed/FEED_ID
            parts = url.split('/')
            if len(parts) < 7: continue
            
            handle = parts[4] # es. bossett.social
            feed_rkey = parts[6] # es. for-science
            
            # 1. Troviamo il profilo COMPLETO dell'autore
            # (Qui c'è il numero di follower corretto!)
            res = client.app.bsky.actor.get_profile({'actor': handle})
            author_did = res.did
            author_followers = res.followers_count or 0  # <--- PRENDIAMOLO DA QUI
            
            # 2. Costruiamo l'URI AT
            at_uri = f"at://{author_did}/app.bsky.feed.generator/{feed_rkey}"
            
            # 3. Scarichiamo i dati veri del feed
            feed_info = client.app.bsky.feed.get_feed_generator({'feed': at_uri})
            view = feed_info.view
            
            # 4. Salviamo i dati
            feed_data.append({
                'name': view.display_name,
                'creation_date': view.indexed_at,
                'feed_likes': view.like_count or 0,
                'creator_followers': author_followers, # Usiamo il dato preso dallo step 1
                'creator_handle': view.creator.handle,
                'creator_did': view.creator.did,
                'uri': view.uri,
                'source': 'Scraped'
            })
            
        except Exception as e:
            # Errori comuni: profilo sospeso (AccountTakedown), feed cancellato (InvalidRequest)
            # Li ignoriamo per non sporcare il dataset con dati corrotti.
            # print(f"❌ Saltato {url}: {e}") # Decommenta se vuoi vedere gli errori
            continue
            
    return feed_data

    
def main():
    # --- AVVIO TIMER TOTALE ---
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
    
    # 1. FASE SCRAPING (con timer parziale)
    start_scrape = time.time()
    urls = scrape_directory()
    end_scrape = time.time()
    print(f"⏱️  Tempo Scraping: {str(timedelta(seconds=int(end_scrape - start_scrape)))}")
    
    # 2. FASE ARRICCHIMENTO API (con timer parziale)
    start_api = time.time()
    data = extract_data_from_api(client, urls)
    end_api = time.time()
    print(f"⏱️  Tempo API Download: {str(timedelta(seconds=int(end_api - start_api)))}")
    
    # 3. SALVATAGGIO
    if data:
        # Assicuriamoci che la cartella esista
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        
        df = pd.DataFrame(data)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"✅ Salvati {len(df)} feed in {OUTPUT_CSV}")
    else:
        print("Nessun dato valido estratto.")

    # --- FINE TIMER TOTALE ---
    end_time = time.time()
    total_time = end_time - start_time
    print(f"\n🏁 FINITO! Tempo totale di esecuzione: {str(timedelta(seconds=int(total_time)))}")

if __name__ == "__main__":
    main()