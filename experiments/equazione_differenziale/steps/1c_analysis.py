# Tesi / experiments / equazione_differenziale / steps / 1c_analysis.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from scipy.stats import linregress

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Input 1: Metriche Utente (per analisi Gini/Dominanza)
INPUT_METRICS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_metrics.csv"))
# Input 2: Eventi Arricchiti (per fitting parametri Theta e Beta)
INPUT_EVENTS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))

# Output
OUTPUT_DIR_IMGS = os.path.abspath(os.path.join(CURRENT_DIR, "../results/figures"))
OUTPUT_REPORT = os.path.abspath(os.path.join(CURRENT_DIR, "../results/report_step1.txt"))

def gini_coefficient(x):
    """Calcola Gini (0=Equità, 1=Disuguaglianza Totale)."""
    x = np.array(x, dtype=np.float64)
    if np.any(x < 0): x = np.abs(x)
    n = len(x)
    if n == 0: return 0.0
    s = x.sum()
    if s == 0: return 0.0
    r = np.argsort(np.argsort(-x)) # Ranks
    return 1 - (2.0 * (r * x).sum() + s)/(n * s)

def analyze_power_law_fit(df_events):
    """
    Replica Fig. 2a del paper: E[V|X] ~ X^theta
    Raggruppa i post per popolarità (X) e calcola il successo medio (V).
    Restituisce theta (pendenza) e i dati per il plot.
    """
    # Rimuoviamo zeri o valori negativi per i logaritmi
    data = df_events[(df_events['prev_X'] > 0.1) & (df_events['v_i'] > 0)].copy()
    
    # Creiamo 20 bin logaritmici basati sulla popolarità
    data['bin_X'] = pd.qcut(data['prev_X'], q=20, duplicates='drop')
    
    # Calcoliamo la media di X e V per ogni bin
    binned = data.groupby('bin_X', observed=True).agg({
        'prev_X': 'mean',
        'v_i': 'mean'
    }).reset_index()
    
    # Regressione Lineare in scala Log-Log: log(V) = theta * log(X) + log(beta)
    log_X = np.log(binned['prev_X'])
    log_V = np.log(binned['v_i'])
    
    slope, intercept, r_value, p_value, std_err = linregress(log_X, log_V)
    
    return slope, intercept, r_value, binned

def main():
    print(f"--- Modulo C: Analisi Statistica Completa ---")
    
    # 1. CARICAMENTO DATI MACRO (UTENTI)
    if not os.path.exists(INPUT_METRICS):
        print(f"❌ File metriche non trovato: {INPUT_METRICS}")
        sys.exit(1)
        
    df_users = pd.read_csv(INPUT_METRICS)
    # FIX NOMI COLONNE: Nel modulo B abbiamo usato 'avg_jump_size', non 'beta'
    if 'beta' not in df_users.columns and 'avg_jump_size' in df_users.columns:
        df_users = df_users.rename(columns={'avg_jump_size': 'beta'})
        
    print(f"👥 Utenti caricati: {len(df_users)}")
    
    # Creazione cartelle
    os.makedirs(OUTPUT_DIR_IMGS, exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_REPORT), exist_ok=True)

    # 2. ANALISI MACRO (Dominanza)
    # Filtro utenti inattivi per statistiche pulite
    df_active = df_users[df_users['mean_X'] > 1.0].copy() 

    gini_X = gini_coefficient(df_active['mean_X'])
    gini_Beta = gini_coefficient(df_active['beta'])
    gini_Lambda = gini_coefficient(df_active['lambda'])

    # Correlazioni (Log-Log)
    # Usiamo log1p per gestire gli zero in modo sicuro
    corr_lambda_X = df_active[['lambda', 'mean_X']].apply(np.log1p).corr().iloc[0,1]
    corr_beta_X = df_active[['beta', 'mean_X']].apply(np.log1p).corr().iloc[0,1]

    # 3. ANALISI MICRO (Fitting Parametri dal Paper)
    theta_est = np.nan
    r_squared = np.nan
    binned_data = None
    
    if os.path.exists(INPUT_EVENTS):
        print("📉 Caricamento Eventi per Fitting Theta (potrebbe richiedere tempo)...")
        # Carichiamo solo le colonne utili per risparmiare RAM
        df_events = pd.read_csv(INPUT_EVENTS, usecols=['prev_X', 'v_i'], compression='gzip')
        print(f"   Eventi caricati: {len(df_events)}")
        
        theta_est, intercept_est, r_val, binned_data = analyze_power_law_fit(df_events)
        r_squared = r_val**2
        print(f"   ✅ Fitting completato: Theta = {theta_est:.4f} (R2={r_squared:.2f})")
    else:
        print("⚠️ File eventi enriched non trovato. Salto analisi fitting.")

    
    # 4. REPORT TESTUALE
    report = f"""
    === REPORT ANALISI EQUAZIONE DIFFERENZIALE (BLUESKY) ===
    
    1. DATI ANALIZZATI
       Utenti Totali: {len(df_users)}
       Utenti con engagment (pop > 1): {len(df_active)} ({(len(df_active) / len(df_users)) * 100:.2f}%)
         * Nota: La soglia >1 esclude utenti "fantasma" o con zero interazioni storiche.
         * Popolarità = 1 + Like + Repost + Reply (Decadimento esponenziale 24h).
       Utenti esclusi (zero interazioni): {len(df_users)-len(df_active)} 
    
    2. DISUGUAGLIANZA (Indice di Gini)
       - Popolarità (X): {gini_X:.4f}  (Se > 0.8 c'è forte disuguaglianza)
       - Abilità/Viralità (Beta): {gini_Beta:.4f}
       - Attività (Lambda): {gini_Lambda:.4f}
         * Metodo: Calcolato su array ordinati. 0 = Equità perfetta, 1 = Monopolio totale.
    
    3. PARAMETRI DEL MODELLO (Stimati dai dati)
       - Theta (Esponente Viralità): {theta_est:.4f}
       - R^2 del Fit: {r_squared:.4f}
         * Metodo Calcolo: Regressione lineare su scala Log-Log.
           I post sono raggruppati in 20 "bin" (fasce) di popolarità crescente.
           Theta è la pendenza della retta: log(Successo) = Theta * log(Popolarità).
         * Interpretazione:
           - Theta ~ 0.7: Sistema stabile (come Facebook nel paper).
           - Theta > 1.0: Sistema "Winner-takes-all" (instabile/dominio totale).
       
    4. CORRELAZIONI (Log-Log)
       - Chi posta di più diventa famoso? (Lambda vs X): {corr_lambda_X:.4f}
       - Chi fa contenuti migliori diventa famoso? (Beta vs X): {corr_beta_X:.4f}
    
    5. LEGENDA GRAFICI GENERATI
       - Fig 1 (Istogramma): Distribuzione della popolarità media (Scala Log).
         Serve a visualizzare la "Power Law" (pochi giganti, "coda lunga" di utenti piccoli).
       - Fig 2 (Fitting Theta): IL CUORE DEL PAPER. Mostra la relazione tra
         popolarità istantanea (Asse X) e successo del post (Asse Y).
         I punti neri sono i dati reali mediati; la linea rossa è il modello teorico.
       - Fig 3 (Scatter): Relazione tra Qualità media (Beta) e Popolarità finale (X).
    """
    
    print(report)
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)


        
    # 5. GENERAZIONE GRAFICI
    sns.set_theme(style="whitegrid")

    # FIG 1: Distribuzione Popolarità
    plt.figure(figsize=(10, 6))
    sns.histplot(df_active['mean_X'], log_scale=True, stat="density", kde=True)
    plt.title(f"Distribuzione Popolarità (Gini={gini_X:.2f})")
    plt.xlabel("Popolarità Media <X>")
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "1a_distribuzione_popolarita.png"))
    plt.close()

    # FIG 2: Fitting Theta (Replicazione Fig 2a Paper)
    if binned_data is not None:
        plt.figure(figsize=(8, 8))
        # Punti sperimentali
        plt.scatter(binned_data['prev_X'], binned_data['v_i'], color='black', label='Dati Empirici (Binned)')
        
        # Retta di fitting
        x_vals = np.linspace(binned_data['prev_X'].min(), binned_data['prev_X'].max(), 100)
        y_vals = np.exp(intercept_est) * (x_vals ** theta_est)
        
        # FIX: Aggiunta 'r' (raw string) prima della f-string per gestire LaTeX
        plt.plot(x_vals, y_vals, color='red', linestyle='--', label=rf'Fit: $V \sim X^{{{theta_est:.2f}}}$')
        
        plt.xscale('log')
        plt.yscale('log')
        plt.xlabel(r"Popolarità Istantanea $X(t^-)$")
        plt.ylabel(r"Successo del Post $V_i$ (Engagment)")
        plt.title(f"Rich-get-Richer Effect (Theta={theta_est:.2f})")
        plt.legend()
        plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "1b_fitting_theta.png"))
        plt.close()

    # Funzione helper locale per aggiungere la retta di regressione log-log (figura 3 e 4)
    def add_log_regression(x, y, ax, color_line='red'):
        # Filtriamo valori <= 0 per evitare errori col log
        mask = (x > 0) & (y > 0)
        x_clean = x[mask]
        y_clean = y[mask]
        
        if len(x_clean) > 1:
            log_x = np.log(x_clean)
            log_y = np.log(y_clean)
            slope, intercept, r_val, _, _ = linregress(log_x, log_y)
            
            # Creiamo i punti per disegnare la retta
            x_vals = np.linspace(x_clean.min(), x_clean.max(), 100)
            y_vals = np.exp(intercept) * (x_vals ** slope)
            
            # Disegniamo la retta
            ax.plot(x_vals, y_vals, color=color_line, linestyle='--', linewidth=2, 
                    label=rf'Fit (Slope={slope:.2f}, $R={r_val:.2f}$)')
            return r_val
        return 0.0

    # FIG 3: Scatter Qualità (Beta) vs Popolarità (X)
    plt.figure(figsize=(8, 8))
    plt.scatter(df_active['beta'], df_active['mean_X'], alpha=0.3, s=10, c='purple', label='Utenti')
    
    # Aggiunta retta di regressione
    r_beta = add_log_regression(df_active['beta'], df_active['mean_X'], plt.gca(), color_line='orange')
    
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel(r"Qualità Media $\beta$ (Avg Engagement)")
    plt.ylabel(r"Popolarità Media $\langle X \rangle$")
    plt.title(f"Impatto della Qualità (Corr R={r_beta:.2f})")
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "1c_scatter_beta_X.png"))
    plt.close()

    # FIG 4: Scatter Quantità (Lambda) vs Popolarità (X)
    plt.figure(figsize=(8, 8))
    plt.scatter(df_active['lambda'], df_active['mean_X'], alpha=0.3, s=10, c='green', label='Utenti')
    
    # Aggiunta retta di regressione
    r_lambda = add_log_regression(df_active['lambda'], df_active['mean_X'], plt.gca(), color_line='blue')
    
    plt.xscale('log')
    plt.yscale('log')
    plt.xlabel(r"Attività $\lambda$ (Post/Giorno)")
    plt.ylabel(r"Popolarità Media $\langle X \rangle$")
    plt.title(f"Impatto della Quantità (Corr R={r_lambda:.2f})")
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "1d_scatter_lambda_X.png"))
    plt.close()

    print(f"✅ Analisi completata. Grafici salvati in: {OUTPUT_DIR_IMGS}")

if __name__ == "__main__":
    main()