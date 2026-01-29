# .../Tesi/experiments/validate_classification.py

import pandas as pd
import os

# --- CONFIGURAZIONE ---
INPUT_FILE = 'results/feed_stats/bluesky_feed_census_classified.csv'
OUTPUT_REPORT = 'results/plots/ai_vs_human/validation_qualitative_report.txt'

# LE TUE FRASI PROTOTIPO (Copiate dal codice di classificazione)
PROTOTYPES = {
    "Automated Bot": "This is an automated bot account posting updates via script, feed generator or algorithm.",
    "Professional/Dev": "I am a software engineer, developer, official organization, news outlet or project maintainer.",
    "Amateur User": "I am a private person sharing my personal interests, hobbies, life, thoughts and opinions."
}

def format_bio(text):
    """
    Indenta le bio multilinea per renderle leggibili nel report.
    Aggiunge una tabulazione dopo ogni 'a capo'.
    """
    if pd.isna(text): return "[NESSUNA BIO]"
    # Sostituisce i newline con newline + tabulazione per mantenere l'allineamento
    return text.replace('\n', '\n\t> ')

def main():
    print("--- INIZIO VALIDAZIONE QUALITATIVA (FORMATTAZIONE MIGLIORATA) ---")
    
    if not os.path.exists(INPUT_FILE):
        print(f"Errore: File {INPUT_FILE} non trovato.")
        return

    df = pd.read_csv(INPUT_FILE)
    
    # Pulizia: Solo creator univoci e bio inglesi
    df_clean = df[df['predicted_category'] != 'Unknown/Non-English'].copy()
    df_unique = df_clean.drop_duplicates(subset='creator_did')
    
    print(f"Dataset caricato: {len(df_unique)} creator univoci classificati.")
    
    # Ordine personalizzato per il report (opzionale, ma più ordinato)
    categories_order = ["Automated Bot", "Professional/Dev", "Amateur User"]

    # Apriamo il file in scrittura
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write("====================================================================\n")
        f.write("      REPORT DI VALIDAZIONE QUALITATIVA (SANITY CHECK)\n")
        f.write("====================================================================\n\n")
        f.write("Obiettivo: Verificare la coerenza semantica tra la bio e la categoria assegnata.\n")
        f.write("Legenda: [Conf] = Punteggio di confidenza (similarità coseno).\n\n")

        for category in categories_order:
            # Recuperiamo la frase prototipo
            proto_sentence = PROTOTYPES.get(category, "N/A")
            
            f.write(f"\n{'#'*70}\n")
            f.write(f" CATEGORIA: {category.upper()}\n")
            f.write(f" Frase Prototipo usata: \"{proto_sentence}\"\n")
            f.write(f"{'#'*70}\n")
            
            # Filtriamo il dataframe
            cat_df = df_unique[df_unique['predicted_category'] == category]
            
            if len(cat_df) == 0:
                f.write("\n(Nessun utente trovato in questa categoria)\n")
                continue

            # Funzione helper per scrivere i blocchi
            def write_section(title, description, dataframe_subset):
                f.write(f"\n   {title}\n")
                f.write(f"   {description}\n")
                f.write(f"   {'-'*60}\n")
                
                for i, row in dataframe_subset.iterrows():
                    bio_formatted = format_bio(row['creator_description'])
                    f.write(f"   * [Conf: {row['confidence']:.3f}]\n")
                    f.write(f"     Bio:\n\t> {bio_formatted}\n")
                    f.write(f"     {'.'*40}\n") # Separatore leggero tra utenti

            # --- 1. ARCHETIPI ---
            top_conf = cat_df.sort_values(by='confidence', ascending=False).head(5)
            write_section(
                "A. GLI ARCHETIPI (Top 5 Confidence)", 
                "Questi sono i profili che l'IA considera 'perfetti' per la categoria.", 
                top_conf
            )

            # --- 2. CASI LIMITE ---
            low_conf = cat_df.sort_values(by='confidence', ascending=True).head(5)
            write_section(
                "B. I CASI LIMITE (Bottom 5 Confidence)", 
                "Profili con punteggio basso (ma scelti comunque). Cerca qui errori o ambiguità.", 
                low_conf
            )

            # --- 3. CAMPIONE CASUALE ---
            if len(cat_df) > 5:
                random_sample = cat_df.sample(5, random_state=42)
                write_section(
                    "C. CAMPIONE CASUALE (Random 5)", 
                    "Controllo imparziale della qualità media.", 
                    random_sample
                )
            
            f.write("\n\n") # Spazio extra tra le categorie

    print(f"Report generato con successo in: {OUTPUT_REPORT}")
    print("Ora il file dovrebbe essere molto più leggibile!")

if __name__ == "__main__":
    main()