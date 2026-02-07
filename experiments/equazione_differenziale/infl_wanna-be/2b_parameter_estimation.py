# Tesi / experiments / equazione_differenziale / infl_wanna-be / 2b_parameter_estimation.py

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

OUTPUT_PLOTS_DIR = os.path.join(CURRENT_DIR, "plots")
OUTPUT_REPORT = os.path.join(CURRENT_DIR, "plots/parameters_report.txt")

sns.set_theme(style="whitegrid")
COLORS = {'INFLUENCER': 'darkred', 'WANNABE': 'darkblue'}

# Numero massimo di utenti da campionare per evitare di saturare la RAM
MAX_USERS_PER_GROUP = 500 

def load_and_filter_data():
    print("📂 Caricamento dati grezzi (Analisi Integrale)...")
    if not os.path.exists(INPUT_GROUPS) or not os.path.exists(INPUT_EVENTS):
        print("❌ File mancanti.")
        sys.exit(1)

    df_groups = pd.read_csv(INPUT_GROUPS)
    
    selected_dids = []
    np.random.seed(42)
    
    # Selezioniamo un campione rappresentativo
    for grp in ['INFLUENCER', 'WANNABE']:
        dids = df_groups[df_groups['group'] == grp]['did'].unique()
        if len(dids) > MAX_USERS_PER_GROUP:
            chosen = np.random.choice(dids, MAX_USERS_PER_GROUP, replace=False)
        else:
            chosen = dids
        selected_dids.extend(chosen)
        print(f"   -> Gruppo {grp}: analizzo {len(chosen)} utenti.")
    
    print("⏳ Lettura eventi (può richiedere tempo)...")
    df_events = pd.read_csv(INPUT_EVENTS, compression='gzip', usecols=['did', 'ts', 'prev_X'])
    df_events = df_events[df_events['did'].isin(selected_dids)].copy()
    
    df_merged = df_events.merge(df_groups[['did', 'group']], on='did', how='inner')
    df_merged['date'] = pd.to_datetime(df_merged['ts'], unit='s')
    
    return df_merged

def prepare_pooled_data(df, group_name):
    """
    Raccoglie TUTTE le transizioni giornaliere di tutti gli utenti del gruppo.
    Nessun filtro sugli outlier: prendiamo tutto, anche i salti virali.
    """
    X_list = []
    Drift_list = []
    
    subset = df[df['group'] == group_name]
    unique_users = subset['did'].unique()
    
    print(f"⚙️  Estrazione traiettorie per {group_name}...")
    
    for did in tqdm(unique_users, leave=False):
        user_data = subset[subset['did'] == did].sort_values('ts')
        
        # Resampling Giornaliero
        user_data = user_data.set_index('date')
        daily = user_data['prev_X'].resample('1D').mean().dropna()
        
        if len(daily) < 3: continue
        
        vals = daily.values
        X_t = vals[:-1]             # Stato oggi
        dX = vals[1:] - vals[:-1]   # Salto verso domani (Drift)
        
        X_list.extend(X_t)
        Drift_list.extend(dX)
        
    return np.array(X_list), np.array(Drift_list)

def estimate_pooled_parameters(X_pool, Drift_pool, dt=1.0):
    """
    Stima i parametri OU sui dati grezzi.
    """
    if len(X_pool) < 10: return None
    
    # Regressione Lineare: dX = A + B*X
    slope, intercept, r_value, p_value, std_err = linregress(X_pool, Drift_pool)
    
    # Parametri Fisici
    theta = -slope
    mu = intercept / theta if abs(theta) > 1e-6 else 0
    
    # Calcolo Sigma (Deviazione Standard dei residui)
    fitted_drift = intercept + slope * X_pool
    residuals = Drift_pool - fitted_drift
    sigma = np.std(residuals) / np.sqrt(dt)
    
    # Metriche Relative
    cv = (sigma / mu * 100) if mu > 0 else 0
    half_life = np.log(2) / theta if theta > 0 else 999
    
    return {
        'theta': theta,
        'mu': mu,
        'sigma': sigma,
        'cv': cv,
        'half_life': half_life,
        'R2': r_value**2,
        'n_samples': len(X_pool)
    }

def plot_pooled_drift(data_storage):
    """
    Grafico Compact (12x5) ad Alto Contrasto.
    Mostra i dati reali con i loro outlier.
    """
    print("🎨 Generazione grafico Drift (Con Outliers)...")
    os.makedirs(OUTPUT_PLOTS_DIR, exist_ok=True)
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    for i, grp in enumerate(['INFLUENCER', 'WANNABE']):
        if grp not in data_storage: continue
        
        ax = axes[i]
        d = data_storage[grp]
        X_vals = d['X']
        Drift_vals = d['Drift']
        res = d['res']
        
        # Colore scuro per visibilità
        col = COLORS[grp]
        
        # Plot dei punti (Max 8000 per non appesantire il rendering, ma rappresentativi)
        if len(X_vals) > 8000:
            idx = np.random.choice(len(X_vals), 8000, replace=False)
            ax.scatter(X_vals[idx], Drift_vals[idx], color=col, alpha=0.5, s=6, label='Dati Reali')
        else:
            ax.scatter(X_vals, Drift_vals, color=col, alpha=0.6, s=6, label='Dati Reali')
        
        # Linea del Modello (Fit)
        x_line = np.linspace(min(X_vals), max(X_vals), 100)
        y_line = -res['theta'] * (x_line - res['mu']) 
        ax.plot(x_line, y_line, color='black', linewidth=2, linestyle='--', label='Trend Medio')
        
        # Linea Zero
        ax.axhline(0, color='gray', linewidth=1)
        
        ax.set_title(f"Dinamica {grp}", fontsize=12, fontweight='bold', color=col)
        ax.set_xlabel("Influenza X(t)", fontsize=10)
        ax.set_ylabel("Drift dX/dt", fontsize=10)
        
        # Box Parametri
        stats = (f"$\\mu$ (Target) = {res['mu']:.0f}\n"
                 f"$\\theta$ (Speed) = {res['theta']:.3f}\n"
                 f"$\\sigma$ (Noise) = {res['sigma']:.1f}\n"
                 f"$CV$ = {res['cv']:.1f}%")
        
        props = dict(boxstyle='round', facecolor='white', alpha=0.9, edgecolor='black')
        ax.text(0.95, 0.95, stats, transform=ax.transAxes, va='top', ha='right', bbox=props, fontsize=9)
        
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out = os.path.join(OUTPUT_PLOTS_DIR, "2b_drift_pooled_scatter.png")
    plt.savefig(out, dpi=150)
    plt.show()
    print(f"✅ Plot salvato: {out}")

def main():
    print("--- Step 2b: Stima Parametri (Metodo Pooled - Dati Completi) ---")
    
    df = load_and_filter_data()
    
    report_lines = ["=== REPORT PARAMETRI (DATI COMPLETI CON OUTLIERS) ===\n"]
    data_storage = {}
    
    for grp in ['INFLUENCER', 'WANNABE']:
        # 1. Prepara (Senza filtri)
        X_pool, Drift_pool = prepare_pooled_data(df, grp)
        
        # 2. Stima
        res = estimate_pooled_parameters(X_pool, Drift_pool)
        
        if res:
            data_storage[grp] = {'X': X_pool, 'Drift': Drift_pool, 'res': res}
            
            report_lines.append(f"GRUPPO: {grp}")
            report_lines.append(f"  Punti analizzati:     {res['n_samples']}")
            report_lines.append(f"  > MU (Target):        {res['mu']:.2f}")
            report_lines.append(f"  > THETA (Rigidità):   {res['theta']:.4f}")
            report_lines.append(f"  > SIGMA (Volatilità): {res['sigma']:.2f}")
            report_lines.append(f"  > CV (Rischio Rel.):  {res['cv']:.2f}%")
            report_lines.append(f"  > Half-Life:          {res['half_life']:.2f} giorni")
            report_lines.append(f"  > R^2:                {res['R2']:.4f}")
            report_lines.append("-" * 40)
            
    with open(OUTPUT_REPORT, "w") as f:
        f.write("\n".join(report_lines))
    print(f"📄 Report salvato: {OUTPUT_REPORT}")
    
    plot_pooled_drift(data_storage)

if __name__ == "__main__":
    main()