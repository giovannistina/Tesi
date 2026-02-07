# Tesi / experiments / equazione_differenziale / infl_wanna-be / 3_equation_discovery.py
# RICORDARSI DI INSERIRE MANUALMENTE I VALORI     THETA E MU IN params_input 



import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from scipy.stats import linregress
from tqdm import tqdm

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_EVENTS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))
INPUT_GROUPS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_groups.csv"))
OUTPUT_PLOT = os.path.join(CURRENT_DIR, "plots/3_noise_structure_gamma.png")
OUTPUT_REPORT = os.path.join(CURRENT_DIR, "plots/final_equation_report.txt")

sns.set_theme(style="whitegrid")
MAX_USERS = 500 # Campione per velocità

def load_data():
    print("📂 Caricamento dati...")
    df_groups = pd.read_csv(INPUT_GROUPS)
    
    # Selezione casuale utenti
    selected_dids = []
    np.random.seed(42)
    for grp in ['INFLUENCER', 'WANNABE']:
        dids = df_groups[df_groups['group'] == grp]['did'].unique()
        chosen = np.random.choice(dids, min(len(dids), MAX_USERS), replace=False)
        selected_dids.extend(chosen)
        
    df_ev = pd.read_csv(INPUT_EVENTS, compression='gzip', usecols=['did', 'ts', 'prev_X'])
    df_ev = df_ev[df_ev['did'].isin(selected_dids)]
    df = df_ev.merge(df_groups[['did', 'group']], on='did')
    df['date'] = pd.to_datetime(df['ts'], unit='s')
    return df

def analyze_noise_structure(df, group_name, theta_est, mu_est):
    """
    Calcola i residui (Rumore) e vede come scalano rispetto a X.
    Teoria: Residual^2 ~ sigma^2 * X^(2*gamma)
    Log-Log Regression: log(Residual^2) = A + B * log(X)
    Gamma = B / 2
    """
    print(f"⚙️ Analisi struttura rumore per {group_name}...")
    
    X_vals = []
    Squared_Residuals = []
    
    subset = df[df['group'] == group_name]
    
    for did in tqdm(subset['did'].unique(), leave=False):
        # Resampling giornaliero
        u = subset[subset['did'] == did].set_index('date').sort_index()
        daily = u['prev_X'].resample('1D').mean().dropna()
        if len(daily) < 3: continue
        
        vals = daily.values
        X_t = vals[:-1]
        dX = vals[1:] - vals[:-1]
        
        # Calcoliamo cosa avrebbe dovuto fare il modello deterministico
        expected_drift = theta_est * (mu_est - X_t)
        
        # Calcoliamo l'errore (il rumore puro)
        residuals = dX - expected_drift
        
        # Ci servono solo i residui validi per il logaritmo
        mask = (X_t > 1) & (np.abs(residuals) > 0.001)
        
        X_vals.extend(X_t[mask])
        Squared_Residuals.extend(residuals[mask]**2)

    X_arr = np.array(X_vals)
    Res2_arr = np.array(Squared_Residuals)
    
    # Log-Log Regression
    log_X = np.log(X_arr)
    log_Res2 = np.log(Res2_arr)
    
    slope, intercept, r_val, p_val, std_err = linregress(log_X, log_Res2)
    
    # Gamma è metà della pendenza (perché abbiamo usato i residui al quadrato)
    gamma = slope / 2
    sigma_base = np.exp(intercept / 2)
    
    return {
        'gamma': gamma,
        'sigma_base': sigma_base,
        'slope': slope,
        'R2': r_val**2,
        'X': X_arr,
        'Res2': Res2_arr
    }

def main():
    df = load_data()
    
    # Parametri stimati dallo step 2b (valori approssimativi medi presi dai tuoi report precedenti)
    # È meglio usare i valori che hai trovato tu, ma qui metto delle stime plausibili
    # per calcolare i residui corretti.
    params_input = {
        'INFLUENCER': {'theta': 0.0766, 'mu': 1328.59},
        'WANNABE':    {'theta': 0.1220, 'mu': 69.38}
    }
    
    results = {}
    report = ["=== EQUAZIONE DIFFERENZIALE FINALE (Step 3) ===\n"]
    
    for grp in ['INFLUENCER', 'WANNABE']:
        p = params_input[grp]
        res = analyze_noise_structure(df, grp, p['theta'], p['mu'])
        results[grp] = res
        
        report.append(f"GRUPPO: {grp}")
        report.append(f"  Gamma (Esponente del Rumore): {res['gamma']:.3f}")
        report.append(f"  Sigma Base (Coefficiente):    {res['sigma_base']:.3f}")
        report.append(f"  R^2 (Log-Log Fit):            {res['R2']:.3f}")
        
        # Interpretazione Fisica
        if res['gamma'] < 0.2:
            model_type = "Rumore Costante (Simile a OU)"
        elif 0.4 <= res['gamma'] <= 0.6:
            model_type = "Rumore Radice Quadrata (Simile a CIR)"
        elif 0.8 <= res['gamma'] <= 1.2:
            model_type = "Rumore Moltiplicativo (Simile a Geometrico)"
        else:
            model_type = "Modello Ibrido / Complesso"
            
        report.append(f"  -> MODELLO SUGGERITO: {model_type}")
        
        # Scrittura Equazione in LaTeX
        eq = f"dX_t = {p['theta']:.2f}({p['mu']:.0f} - X_t)dt + {res['sigma_base']:.2f} X_t^{{{res['gamma']:.2f}}} dW_t"
        report.append(f"  -> EQUAZIONE: {eq}")
        report.append("-" * 40)

    # Plotting
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    for i, grp in enumerate(['INFLUENCER', 'WANNABE']):
        ax = axes[i]
        res = results[grp]
        
        # Scatter Plot (Log-Log)
        # Campioniamo per visualizzazione
        idx = np.random.choice(len(res['X']), min(5000, len(res['X'])), replace=False)
        ax.scatter(res['X'][idx], res['Res2'][idx], alpha=0.3, s=5, color='gray', label='Residui^2')
        
        # Fit Line
        x_line = np.linspace(min(res['X']), max(res['X']), 100)
        # y = exp(intercept + slope * log(x)) = exp(intercept) * x^slope
        y_line = (res['sigma_base']**2) * (x_line**(res['slope']))
        
        ax.plot(x_line, y_line, color='red', linewidth=2, linestyle='--', label=f'Fit ($\\gamma={res["gamma"]:.2f}$)')
        
        ax.set_xscale('log')
        ax.set_yscale('log')
        ax.set_xlabel('Influenza X(t) [Log]')
        ax.set_ylabel('Intensità Rumore (Residui^2) [Log]')
        ax.set_title(f"Scaling del Rumore: {grp}", fontweight='bold')
        ax.legend()
        ax.grid(True, which="both", ls="--", alpha=0.5)

    plt.tight_layout()
    os.makedirs(os.path.dirname(OUTPUT_PLOT), exist_ok=True)
    plt.savefig(OUTPUT_PLOT, dpi=300)
    print(f"✅ Grafico salvato: {OUTPUT_PLOT}")
    
    with open(OUTPUT_REPORT, "w") as f:
        f.write("\n".join(report))
    print(f"📄 Equazione finale salvata in: {os.path.basename(OUTPUT_REPORT)}")

if __name__ == "__main__":
    main()