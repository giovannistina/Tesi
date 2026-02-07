import pandas as pd
import numpy as np
import scipy.stats as st
import os
import sys

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_EVENTS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))

# Parametro Theta (Aggiornato al valore reale senza repost)
THETA = 0.3254 

# Distribuzioni da testare (Dizionario)
DISTRIBUTIONS = {
    "Lognormale": st.lognorm,
    "Pareto (Power Law)": st.pareto,
    "Esponenziale": st.expon,
    "Weibull": st.weibull_min
}

def main():
    print("--- STEP 2c: TORNEO DI DISTRIBUZIONI (BEST FIT FINDER) ---")
    
    # 1. Caricamento Dati
    if not os.path.exists(INPUT_EVENTS):
        print(f"❌ Errore: File non trovato {INPUT_EVENTS}")
        sys.exit(1)

    print("📥 Caricamento dati (con filtro Post Originali)...")
    
    # Carichiamo solo colonne utili
    cols = ['v_i', 'prev_X', 'post_type']
    try:
        df = pd.read_csv(INPUT_EVENTS, compression='gzip', usecols=lambda c: c in cols)
    except:
        df = pd.read_csv(INPUT_EVENTS, compression='gzip')

    if 'post_type' not in df.columns:
        print("⚠️ Colonna 'post_type' non trovata. Assumo siano tutti post.")
        df['post_type'] = 'post'

    # 2. FILTRO RIGOROSO
    # Teniamo solo Post Originali con storia valida ed engagement positivo
    df = df[
        (df['post_type'] == 'post') & 
        (df['prev_X'] > 0.1) & 
        (df['v_i'] > 0)
    ]
    
    print(f"📊 Eventi validi (Post Originali): {len(df)}")
    
    if len(df) == 0:
        print("❌ Errore: Nessun dato valido trovato dopo i filtri.")
        sys.exit(1)

    # Calcolo Beta
    # Beta = Successo / (Popolarità ^ Theta)
    beta_values = df['v_i'] / (df['prev_X'] ** THETA)
    
    # Pulizia
    data = beta_values.dropna()
    data = data[data > 0]
    
    # Campionamento per velocità (fit su milioni di righe è lento)
    if len(data) > 100000:
        print("⚠️ Dataset enorme: uso un campione casuale di 100.000 punti per il fitting.")
        data_sample = data.sample(100000, random_state=42).values
    else:
        data_sample = data.values

    print(f"🧪 Analisi statistica su {len(data_sample)} campioni (Theta={THETA}).")
    print("-" * 75)
    print(f"{'DISTRIBUZIONE':<20} | {'AIC (Min vince)':<20} | {'Parametri'}")
    print("-" * 75)

    results = []

    for name, distribution in DISTRIBUTIONS.items():
        try:
            # 1. Fitting (Stima parametri)
            # floc=0 fissa la posizione a 0 per distribuzioni che partono da 0 (come lognormale)
            if name == "Lognormale":
                params = distribution.fit(data_sample, floc=0)
            else:
                params = distribution.fit(data_sample)
            
            # 2. Log-Likelihood
            # Calcoliamo quanto è probabile osservare questi dati data la distribuzione
            log_likelihood = np.sum(distribution.logpdf(data_sample, *params))
            
            # 3. AIC (Akaike Information Criterion)
            # AIC = 2k - 2ln(L). Più basso è, meglio è (penalizza complessità).
            k = len(params)
            aic = 2 * k - 2 * log_likelihood
            
            results.append((name, aic, params))
            
            # Formattazione parametri per output leggibile
            params_str = ", ".join([f"{p:.2f}" for p in params])
            print(f"{name:<20} | {aic:<20.2f} | {params_str}")
            
        except Exception as e:
            print(f"{name:<20} | FALLITO ({str(e)})")

    print("-" * 75)
    
    if not results:
        print("❌ Nessuna distribuzione ha completato il fit.")
        return

    # Trova il vincitore
    results.sort(key=lambda x: x[1]) # Ordina per AIC crescente (minore è meglio)
    winner = results[0]
    
    print(f"\n🏆 VINCITORE: {winner[0]}")
    print(f"   I dati empirici (puliti) assomigliano di più a una {winner[0]}.")
    
    # Interpretazione per la Tesi
    print("\nINTERPRETAZIONE PER LA TESI:")
    
    if winner[0] == "Lognormale":
        print("✅ CONFERMA DEL PAPER ORIGINALE.")
        print("- Dopo la pulizia dai repost, Bluesky si comporta come Facebook.")
        print("- Il merito è distribuito in modo moltiplicativo ma 'controllato'.")
        print("- I parametri stimati (vedi sopra) sono la tua Sigma e Mu ufficiali.")
        
    elif winner[0] == "Pareto (Power Law)":
        print("⚠️ DIFFERENZA STRUTTURALE.")
        print("- Anche senza i repost, Bluesky mostra disuguaglianze estreme.")
        print("- È un ambiente 'Winner-takes-all' più aggressivo di Facebook.")
        print("- I super-post virali sono molto più frequenti del previsto.")
        
    elif winner[0] == "Weibull":
        print("⚠️ MODELLO IBRIDO.")
        print("- La distribuzione è una via di mezzo (Stretched Exponential).")
        print("- Indica che il sistema invecchia o ha meccanismi di frenata.")
        
    else:
        print(f"- Risultato inatteso: {winner[0]}")

if __name__ == "__main__":
    main()