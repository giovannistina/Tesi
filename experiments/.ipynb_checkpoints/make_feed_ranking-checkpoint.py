# .../experiments/make_feed_ranking.py
"""
Description: Scarica la classifica dei Feed Generator usando la paginazione (cursor).
             Tenta di scaricare fino al numero richiesto (es. 10.000) o finché la lista finisce.
"""

import os
import time
import pandas as pd
from atproto import Client

# --- CONFIGURAZIONE ---
OUTPUT_CSV = "results/feed_stats/feeds_registry.csv"
# ----------------------

def get_session():
    # Cerca il file session.txt nella cartella corrente o in quella superiore
    paths = ['session.txt', '../data_collection/session.txt']
    for p in paths:
        if os.path.exists(p):
            with open(p, 'r') as f:
                return f.read().strip()
    return None

def main():
    print("--- BLUESKY FEED DOWNLOADER (EXTENDED) ---")
    
    # 1. Login
    session = get_session()
    if not session:
        print("❌ Errore: File 'session.txt' non trovato. Esegui prima 'create_session.py'.")
        return

    client = Client()
    try:
        client.login(session_string=session)
        print("✅ Login effettuato con successo.")
    except Exception as e:
        print(f"❌ Errore di Login: {e}")
        print("Il token potrebbe essere scaduto. Esegui 'create_session.py' per rinnovarlo.")
        return

    # 2. Richiesta numero target
    try:
        user_input = input("Quanti feed vuoi provare a scaricare? (es. 10000): ")
        target_count = int(user_input)
    except ValueError:
        target_count = 1000
        print("Valore non valido. Impostato default: 1000.")

    print(f"\nInizio scaricamento (Target: {target_count})...")
    
    feeds_data = []
    cursor = None # Il puntatore alla pagina successiva
    page_num = 1
    
    # 3. Ciclo di scaricamento
    while len(feeds_data) < target_count:
        try:
            # Calcoliamo quanti ne mancano per arrivare all'obiettivo
            remaining = target_count - len(feeds_data)
            # Chiediamo il minimo tra 100 (limite API) e quelli che mancano
            batch_limit = min(100, remaining)
            
            # Chiamata API
            response = client.app.bsky.unspecced.get_popular_feed_generators({
                'limit': batch_limit,
                'cursor': cursor
            })
            
            # Se la risposta è vuota, abbiamo finito
            if not response.feeds:
                print("⚠️ La lista dei feed è terminata (Bluesky non ne ha altri).")
                break
                
            # Estrazione Dati
            for feed in response.feeds:
                feeds_data.append({
                    'rank_position': len(feeds_data) + 1,
                    'name': feed.display_name,
                    'like_count': feed.like_count,
                    'description': feed.description.replace('\n', ' ') if feed.description else '',
                    'creator_handle': feed.creator.handle,
                    'uri': feed.uri,
                    'indexed_at': feed.indexed_at
                })
            
            print(f"Pagina {page_num}: Scaricati {len(feeds_data)} / {target_count} (Ultimo: {feeds_data[-1]['name']})")
            
            # Preparazione per il prossimo giro
            cursor = response.cursor
            page_num += 1
            
            # Se non c'è cursore, non ci sono altre pagine
            if not cursor:
                print("✅ Raggiunta la fine della classifica ufficiale.")
                break
            
            # Piccola pausa per non sovraccaricare il server
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Errore durante il download: {e}")
            break

    # 4. Salvataggio
    if feeds_data:
        # Assicuriamoci che la cartella esista
        os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)
        
        df = pd.DataFrame(feeds_data)
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        
        print("\n" + "="*40)
        print(f"🎉 COMPLETATO!")
        print(f"Totale Feed scaricati: {len(df)}")
        print(f"File salvato in: {OUTPUT_CSV}")
        print("="*40)
    else:
        print("Nessun dato scaricato.")

if __name__ == "__main__":
    main()