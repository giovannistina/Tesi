# Tesi / experiments / equazione_differenziale / step_2 / a_parameter_estimation.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from scipy.stats import linregress, probplot, lognorm
from scipy.optimize import curve_fit

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Input dallo Step 1 (Eventi con X(t) pre-calcolato)
INPUT_EVENTS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))

# Output (Immagini e CSV Parametri)
OUTPUT_DIR_IMGS = os.path.abspath(os.path.join(CURRENT_DIR, "../results/figures"))
OUTPUT_PARAMS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_parameters_estimated.csv"))
OUTPUT_REPORT = os.path.abspath(os.path.join(CURRENT_DIR, "../results/report_step2.txt"))

def main():
    print("--- STEP 2: STIMA PARAMETRI (PAPER) ---")
    
    # 0. CARICAMENTO DATI
    if not os.path.exists(INPUT_EVENTS):
        print(f"❌ Errore: Manca {INPUT_EVENTS}. Esegui prima lo Step 1.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR_IMGS, exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_PARAMS), exist_ok=True)

    print("📥 Caricamento eventi (questo può richiedere tempo)...")
    df = pd.read_csv(INPUT_EVENTS, compression='gzip')
    print(f"   Righe totali: {len(df)}")

    # Filtro base: Analizziamo solo eventi dove la popolarità pregressa è > 0.1
    # (per evitare log(0) e rumore di fondo)
    df_clean = df[(df['prev_X'] > 0.1) & (df['v_i'] > 0)].copy()

    # =========================================================================
    # ASSUMPTION 1: LA LEGGE DI POTENZA (Theta)
    # Obiettivo: Capire se i "ricchi diventano più ricchi" (V ~ X^theta)
    # =========================================================================
    print("\n📊 1. ASSUMPTION 1: Stima Theta (Viralità)...")
    
    # Binning: Raggruppiamo i dati in 50 fasce di popolarità per pulire il grafico
    df_clean['bin_X'] = pd.qcut(df_clean['prev_X'], q=50, duplicates='drop')
    binned_success = df_clean.groupby('bin_X', observed=True).agg({
        'prev_X': 'mean',
        'v_i': 'mean'
    }).reset_index()

    # Regressione Log-Log
    log_x = np.log(binned_success['prev_X'])
    log_y = np.log(binned_success['v_i'])
    slope, intercept, r_val, _, _ = linregress(log_x, log_y)
    
    GLOBAL_THETA = slope
    print(f"   ✅ THETA stimato: {GLOBAL_THETA:.4f} (R2={r_val**2:.3f})")

    # Grafico Assumption 1
    plt.figure(figsize=(8,6))
    plt.scatter(binned_success['prev_X'], binned_success['v_i'], alpha=0.6, label='Dati Empirici')
    plt.plot(binned_success['prev_X'], np.exp(intercept) * binned_success['prev_X']**slope, 
             'r--', label=rf'Fit: $V \sim X^{{{slope:.2f}}}$')
    plt.xscale('log'); plt.yscale('log')
    plt.title(f"Assumption 1: Successo Atteso vs Popolarità\n(Theta={GLOBAL_THETA:.2f})")
    plt.xlabel("Popolarità Istantanea X(t)")
    plt.ylabel("Successo del Post (Engagement)")
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "2a_assumption1_theta.png"))
    plt.close()

    # =========================================================================
    # ASSUMPTION 2: LOGNORMALITÀ DEGLI SCARTI & BETA PULITO
    # Obiettivo: Calcolare il vero merito (Beta) togliendo l'effetto popolarità
    # =========================================================================
    print("\n📊 2. ASSUMPTION 2: Beta 'Meritocratico' e Lognormalità...")
    
    # Formula chiave: Beta_Merit = V / (X^Theta)
    # Questo ci dice quanto vale il post "al netto" della fama dell'autore
    df_clean['beta_merit_i'] = df_clean['v_i'] / (df_clean['prev_X'] ** GLOBAL_THETA)
    
    # Analisi degli scarti (Log dei beta meritocratici)
    # Se il paper ha ragione, questa distribuzione deve essere una Gaussiana (campana)
    log_residuals = np.log(df_clean['beta_merit_i'])
    log_residuals = log_residuals[np.isfinite(log_residuals)]

    # Grafico Assumption 2
    plt.figure(figsize=(10,6))
    sns.histplot(df_clean['beta_merit_i'], log_scale=True, stat='density', label='Empirica')
    
    # Sovrapposizione Fit Lognormale teorico
    shape, loc, scale = lognorm.fit(df_clean['beta_merit_i'])
    x_pdf = np.logspace(np.log10(df_clean['beta_merit_i'].min()), 
                        np.log10(df_clean['beta_merit_i'].quantile(0.99)), 100)
    plt.plot(x_pdf, lognorm.pdf(x_pdf, shape, loc, scale), 'r-', lw=2, label='Fit Lognormale')
    
    plt.title("Assumption 2: Distribuzione del Merito 'Puro' (Beta)")
    plt.xlabel(r"Beta Meritocratico ($V / X^\theta$)")
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "2b_assumption2_lognormal.png"))
    plt.close()

    # =========================================================================
    # ASSUMPTION 3: ATTIVITÀ DI POSTING (Lambda vs X)
    # Obiettivo: Vedere se chi è famoso posta di più
    # =========================================================================
    print("\n📊 3. ASSUMPTION 3: Dinamica di Posting...")
    
    # Calcolo tempo inter-post per ogni utente
    df = df.sort_values(['did', 'ts'])
    df['prev_ts'] = df.groupby('did')['ts'].shift(1)
    df['inter_post_time'] = df['ts'] - df['prev_ts']
    
    # Filtriamo dati validi
    df_activity = df.dropna(subset=['inter_post_time']).copy()
    df_activity = df_activity[df_activity['prev_X'] > 0.1]
    
    # Posting rate = 1 / (giorni passati dall'ultimo post)
    df_activity['posting_rate'] = 86400.0 / df_activity['inter_post_time']
    # Rimuoviamo casi estremi (spam o bug)
    df_activity = df_activity[(df_activity['posting_rate'] > 0.01) & (df_activity['posting_rate'] < 50)]

    # Binning su X per vedere il trend medio
    df_activity['bin_X'] = pd.qcut(df_activity['prev_X'], q=40, duplicates='drop')
    binned_activity = df_activity.groupby('bin_X', observed=True).agg({
        'prev_X': 'mean',
        'posting_rate': 'mean'
    }).reset_index()

    # Fit della curva Lambda = L0 + L1 * X^Phi
    try:
        popt, _ = curve_fit(lambda x, a, b, c: a + b * x**c, 
                            binned_activity['prev_X'], binned_activity['posting_rate'],
                            p0=[1, 0.1, 0.1], maxfev=5000)
        lam0, lam1, phi = popt
        fit_label = rf'Fit: $\lambda \approx {lam0:.1f} + {lam1:.2f}X^{{{phi:.2f}}}$'
    except:
        lam0, lam1, phi = 0, 0, 0
        fit_label = "Fit fallito (Relazione debole)"
    
    print(f"   ✅ Parametri Attività stimati: Lambda0={lam0:.2f}, Phi={phi:.3f}")

    # Grafico Assumption 3
    plt.figure(figsize=(8,6))
    plt.scatter(binned_activity['prev_X'], binned_activity['posting_rate'], alpha=0.7, label='Dati Empirici')
    if lam0 != 0:
        x_fit = np.logspace(np.log10(binned_activity['prev_X'].min()), np.log10(binned_activity['prev_X'].max()), 100)
        plt.plot(x_fit, lam0 + lam1 * x_fit**phi, 'r--', label=fit_label)
    
    plt.xscale('log'); plt.yscale('log')
    plt.title("Assumption 3: Frequenza Posting vs Popolarità")
    plt.xlabel("Popolarità Istantanea X")
    plt.ylabel("Posting Rate (post/day)")
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "2c_assumption3_activity.png"))
    plt.close()

    # =========================================================================
    # 4. SALVATAGGIO PARAMETRI FINALI
    # =========================================================================
    print("\n💾 4. Salvataggio Parametri Utente...")
    
    # Calcoliamo il Beta Medio per ogni utente
    user_params = df_clean.groupby('did').agg({
        'beta_merit_i': 'mean',  # Merito medio dell'utente
        'v_i': 'count'           # Numero di post analizzati
    }).rename(columns={'beta_merit_i': 'beta_merit', 'v_i': 'n_posts_analyzed'})
    
    user_params.reset_index().to_csv(OUTPUT_PARAMS, index=False)
    
    # Scrittura Report Testuale
    report = f"""
    === REPORT STEP 2: STIMA PARAMETRI (DETTAGLIATO) ===
    
    1. ASSUMPTION 1: LEGGE DI POTENZA (VIRALITÀ - Theta)
       -----------------------------------------------------
       > Valore Theta Globale: {GLOBAL_THETA:.4f}
       > Metodo: Regressione Lineare su scala Log-Log (binning su 50 fasce).
       > Significato: Rappresenta l'esponente della relazione E[V] ~ X^theta.
       > Interpretazione Tesi:
         - Se Theta < 1.0: Il sistema tende all'Equilibrio (Fair Play).
           Il vantaggio di essere famosi cresce meno che proporzionalmente.
         - Se Theta >= 1.0: Il sistema tende al Monopolio (Dominanza).
           Effetto "Rich-get-Richer" esplosivo.

    2. ASSUMPTION 2: QUALITÀ INTRINSECA (MERITO - Beta)
       -----------------------------------------------------
       > Verifica Distribuzione: Vedi grafico 'step2_2_lognormal_check.png'.
         (Se la curva rossa fitta l'istogramma, il rumore è moltiplicativo/Lognormale).
       > Merito Medio (Beta Pulito): {user_params['beta_merit'].mean():.4f}
       > Metodo: Calcolato per ogni post con la formula inversa:
         Beta_i = V_reale / (Popolarità_pregressa ^ Theta)
       > Significato:
         È la capacità "pura" dell'utente di generare like,
         depurata dal vantaggio di avere già follower/visibilità.

    3. ASSUMPTION 3: DINAMICA DI POSTING (ATTIVITÀ - Lambda)
       -----------------------------------------------------
       > Modello fittato: Lambda(X) = {lam0:.2f} + {lam1:.2f} * X^{phi:.2f}
       > Parametro Chiave Phi (Reattività): {phi:.4f}
       > Interpretazione Tesi:
         - Se Phi ~ 0: L'attività è costante (Processo di Poisson omogeneo).
           La popolarità non influenza quanto spesso un utente posta.
         - Se Phi > 0: Processo "Self-Exciting".
           Diventare popolari incentiva l'utente a postare molto di più.
    
    4. DATASET FINALE
       -----------------------------------------------------
       Parametri individuali salvati in:
       {OUTPUT_PARAMS}
       
       Contiene per ogni utente ({len(user_params)} totali):
       - beta_merit: La qualità media stimata dell'utente.
       - n_posts_analyzed: Su quanti post è basata la stima.
    """
    
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
        
    print(report)
    print("✅ STEP 2 COMPLETATO.")
    

if __name__ == "__main__":
    main()