import gzip
import json
import os

# --- CONFIGURAZIONE ---
DIR_PATH = '.'
SUFFIX = '_merged.jsonl.gz'
OUTPUT_FILE = 'struttura_dati_merged_spiegata.txt'

# --- DESCRIZIONI PERSONALIZZATE ---
# Qui mappiamo le chiavi ai commenti che vuoi vedere nel file
DESCRIPTIONS = {
    # --- CAMPI STANDARD BLUESKY ---
    "did": "🆔 ID univoco e immutabile dell'utente (usare questo per le analisi).",
    "handle": "👤 Nome utente leggibile (es. nome.bsky.social).",
    "created_at": "📅 Data e ora di creazione (formato ISO 8601 UTC).",
    "text": "📝 Il testo effettivo del post.",
    "like_count": "❤️ Numero totale di Mi Piace (sul post).",
    "reply_count": "💬 Numero di risposte ricevute.",
    "repost_count": "🔁 Numero totale di Repost.",
    "quote_count": "❝ Numero di citazioni (Quote posts).",
    "uri": "Identificativo tecnico del post nel network (at://...).",
    "cid": "Hash del contenuto (firma digitale del post).",
    "langs": "Lingua rilevata del post (es. ['en'], ['it']).",
    "record": "📦 Il contenitore dei dati grezzi creati dall'utente.",
    "embed": "Contenuto multimediale (Immagini, Link esterni, Video).",
    "facets": "Metadati nel testo: posizioni di link, menzioni e hashtag.",
    "display_name": "Il nome visualizzato sul profilo (può contenere emoji/spazi).",
    
    # --- !!! NUOVI CAMPI AGGIUNTI DAL MERGE !!! ---
    "author_enriched": "🌟 [NUOVO] DATI AGGIUNTI DAL MERGE (Follower, Bio, ecc).",
    "followers": "👥 Numero totale di follower (scaricato dal profilo).",
    "follows": "👀 Numero di profili seguiti (scaricato dal profilo).",
    "description": "📝 Bio/Descrizione del profilo utente.",
    "created_at": "📅 Data creazione (del post o del profilo a seconda del contesto)."
}

def analyze_structure(data, indent=0, lines=[]):
    """Funzione ricorsiva che esplora il JSON e scrive le righe formattate"""
    
    if isinstance(data, dict):
        for key, value in data.items():
            # Determina emoji e tipo
            is_container = isinstance(value, (dict, list))
            emoji = "📂" if is_container else "🔹"
            
            # Cerca descrizione
            desc = f"  --> {DESCRIPTIONS[key]}" if key in DESCRIPTIONS else ""
            
            # Scrittura riga
            prefix = "    " * indent
            lines.append(f"{prefix}{emoji} {key}{desc}")
            
            # Ricorsione
            if isinstance(value, dict):
                analyze_structure(value, indent + 1, lines)
            elif isinstance(value, list):
                if len(value) > 0:
                    lines.append(f"{prefix}    [Lista di {len(value)} elementi. Mostro struttura del primo]")
                    analyze_structure(value[0], indent + 1, lines)
                else:
                    lines.append(f"{prefix}    [Lista vuota]")

def main():
    # 1. Trova il file merged
    files = [f for f in os.listdir(DIR_PATH) if f.endswith(SUFFIX)]
    
    if not files:
        print(f"❌ Nessun file {SUFFIX} trovato.")
        return

    target_file = files[0]
    print(f"📄 Analizzo la struttura di: {target_file}...")

    try:
        data = None
        # Legge solo la prima riga per capire la struttura
        with gzip.open(target_file, 'rt', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                break 
        
        if not data:
            print("❌ Il file sembra vuoto.")
            return

        # Genera documentazione
        lines = []
        lines.append(f"DOCUMENTAZIONE STRUTTURA DATI (MERGED)")
        lines.append(f"Generato da file: {target_file}")
        lines.append("=" * 60 + "\n")
        
        analyze_structure(data, indent=0, lines=lines)

        # Salva su file
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write("\n".join(lines))
            
        print("\n".join(lines)) # Stampa anche a video
        print(f"\n✅ Documentazione salvata in: {OUTPUT_FILE}")

    except Exception as e:
        print(f"❌ Errore: {e}")

if __name__ == "__main__":
    main()