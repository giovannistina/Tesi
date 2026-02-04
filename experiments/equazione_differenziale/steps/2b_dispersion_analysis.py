# Tesi / experiments / equazione_differenziale / steps / 2b_dispersion_analysis.py




import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Usa events_enriched perché è già pulito, ma lavoriamo solo su TS e DID
INPUT_EVENTS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))

# Output
OUTPUT_DIR_IMGS = os.path.abspath(os.path.join(CURRENT_DIR, "../results/figures"))
OUTPUT_REPORT = os.path.abspath(os.path.join(CURRENT_DIR, "../results/report_step2b_dispersion.txt"))

# Parametri Paper
WINDOW_DAYS = 7  # Finestra temporale (Settimanale come nel paper)

def main():
    print("--- STEP 2b: ANALISI DISPERSIONE (BURSTINESS) ---")
    
    if not os.path.exists(INPUT_EVENTS):
        print(f"❌ Errore: Manca {INPUT_EVENTS}.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR_IMGS, exist_ok=True)

    print("📥 Caricamento timestamp eventi...")
    # Carichiamo solo le colonne necessarie per velocità
    df = pd.read_csv(INPUT_EVENTS, usecols=['ts', 'did'], compression='gzip')
    
    # Ordiniamo per sicurezza
    df = df.sort_values('ts')
    
    # Convertiamo timestamp in "Settimana assoluta" dall'inizio del dataset
    # Questo è molto più veloce del resample di pandas
    t_start = df['ts'].min()
    window_sec = WINDOW_DAYS * 86400
    df['time_window'] = ((df['ts'] - t_start) // window_sec).astype(int)

    print(f"📊 Calcolo attività su finestre di {WINDOW_DAYS} giorni...")

    # 1. Contiamo i post per utente per finestra (N_k)
    # create una serie con (did, window) -> count
    counts = df.groupby(['did', 'time_window']).size().reset_index(name='post_count')

    # 2. Per ogni utente, calcoliamo Media e Varianza dei conteggi settimanali
    user_stats = counts.groupby('did')['post_count'].agg(['mean', 'var', 'count'])
    
    # Filtriamo utenti con poca storia (almeno 4 settimane di attività per avere una varianza sensata)
    # Nota: Riempiamo i NaN della varianza con 0 (per chi ha postato 1 sola volta)
    user_stats = user_stats.fillna(0)
    valid_users = user_stats[user_stats['count'] >= 4].copy()
    
    print(f"👥 Utenti validi per l'analisi (attività > 4 settimane): {len(valid_users)}")

    # 3. Calcolo Indice di Dispersione D = Var / Mean
    # Aggiungiamo un epsilon piccolissimo per evitare divisioni per zero
    valid_users['dispersion_index'] = valid_users['var'] / (valid_users['mean'] + 1e-9)

    # =========================================================================
    # GENERAZIONE GRAFICO (REPLICA FIGURA 4a PAPER)
    # =========================================================================
    print("📈 Generazione Grafico Distribuzione Dispersione...")
    
    plt.figure(figsize=(10, 6))
    
    # Usiamo scala logaritmica sull'asse X perché D può variare molto
    # Il paper mostra una "heavy tail"
    sns.histplot(valid_users['dispersion_index'], log_scale=True, element="step", fill=True, stat="density")
    
    # Linea di riferimento Poisson (D=1)
    plt.axvline(x=1.0, color='red', linestyle='--', label='Poisson Process (Random)')
    
    plt.title(f"Indice di Dispersione dei Post (Finestra {WINDOW_DAYS}gg)\n(D > 1 implica 'Burstiness' / Self-Excitement)")
    plt.xlabel(r"Indice di Dispersione $D = \sigma^2 / \mu$ (Log Scale)")
    plt.ylabel("Densità Utenti")
    plt.legend()
    
    output_img = os.path.join(OUTPUT_DIR_IMGS, "2d_dispersion_distribution.png")
    plt.savefig(output_img)
    plt.close()
    
    # =========================================================================
    # REPORT
    # =========================================================================
    mean_D = valid_users['dispersion_index'].mean()
    median_D = valid_users['dispersion_index'].median()
    over_dispersed_pct = (valid_users['dispersion_index'] > 1.1).mean() * 100
    
    report = f"""
    === REPORT ANALISI DISPERSIONE (BURSTINESS) ===
    
    Riferimento Paper: Section 3.1.2 (Posting Activity validation)
    
    1. METODOLOGIA
       - Finestra temporale: {WINDOW_DAYS} giorni
       - Utenti analizzati: {len(valid_users)} (con almeno 4 settimane di dati)
       - Formula: D = Varianza(Post/Week) / Media(Post/Week)
    
    2. RISULTATI
       - Indice di Dispersione Medio: {mean_D:.4f}
       - Indice di Dispersione Mediano: {median_D:.4f}
       - % Utenti "Bursty" (D > 1.1): {over_dispersed_pct:.2f}%
       
    3. INTERPRETAZIONE TESI
       - Se D ≈ 1: Gli utenti postano a caso (Poisson). Il modello statico basterebbe.
       - Se D >> 1 (Il tuo caso atteso): Gli utenti hanno esplosioni di attività.
         Questo GIUSTIFICA l'uso della funzione Lambda dinamica:
         Lambda(t) = Lambda_0 + Lambda_1 * X(t)^Phi
         (La popolarità o l'entusiasmo portano a postare raffiche di contenuti).
    """
    
    print(report)
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
        
    print(f"✅ Analisi completata. Grafico salvato in: {output_img}")

if __name__ == "__main__":
    main()