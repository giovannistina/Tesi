# Tesi / experiments / equazione_differenziale / steps / 2a_parameter_estimation.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from scipy.stats import linregress, lognorm
from scipy.optimize import curve_fit

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_EVENTS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))

OUTPUT_DIR_IMGS = os.path.abspath(os.path.join(CURRENT_DIR, "../results/figures"))
OUTPUT_PARAMS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_parameters_estimated.csv"))
OUTPUT_REPORT = os.path.abspath(os.path.join(CURRENT_DIR, "../results/report_step2.txt"))

def fit_activity_curve(binned_data):
    """
    Funzione helper per fittare la curva di attività (Lambda vs X).
    """
    x_data = binned_data['prev_X']
    
    # Gestione nome colonna (rate o posting_rate)
    if 'posting_rate' in binned_data.columns:
        y_data = binned_data['posting_rate']
    else:
        y_data = binned_data['rate']
    
    # 1. Tentativo Power Law: a + b * x^phi
    try:
        popt, _ = curve_fit(lambda x, a, b, c: a + b * x**c, 
                            x_data, y_data,
                            p0=[25, -10, -0.5], maxfev=10000)
        lam0, lam1, phi = popt
        
        x_fit = np.logspace(np.log10(x_data.min()), np.log10(x_data.max()), 100)
        y_fit = lam0 + lam1 * x_fit**phi
        return phi, rf'Fit Power: $\lambda \approx {lam0:.1f} + {lam1:.2f}X^{{{phi:.2f}}}$', (lam0, lam1, phi), (x_fit, y_fit)
    except:
        pass

    # 2. Tentativo Logaritmico
    try:
        popt, _ = curve_fit(lambda x, a, b: a + b * np.log(x), x_data, y_data)
        lam0, lam1 = popt
        phi = 0.0
        
        x_fit = np.logspace(np.log10(x_data.min()), np.log10(x_data.max()), 100)
        y_fit = lam0 + lam1 * np.log(x_fit)
        return phi, rf'Fit Log: $\lambda \sim \log(X)$', (lam0, lam1), (x_fit, y_fit)
    except:
        # 3. Fallback Costante
        phi = 0.0
        avg_val = y_data.mean()
        x_fit = np.logspace(np.log10(x_data.min()), np.log10(x_data.max()), 100)
        y_fit = [avg_val] * len(x_fit)
        return phi, f"Media Costante: {avg_val:.2f}", (avg_val,), (x_fit, y_fit)

def main():
    print("--- STEP 2: STIMA PARAMETRI (DUAL PHI VERSION - FIXED 2) ---")
    
    # 0. CARICAMENTO
    if not os.path.exists(INPUT_EVENTS):
        print(f"❌ Errore: Manca {INPUT_EVENTS}.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR_IMGS, exist_ok=True)
    os.makedirs(os.path.dirname(OUTPUT_PARAMS), exist_ok=True)

    print("📥 Caricamento eventi...")
    cols_needed = ['v_i', 'prev_X', 'post_type', 'ts', 'did']
    try:
        df = pd.read_csv(INPUT_EVENTS, compression='gzip', usecols=lambda c: c in cols_needed)
    except:
        df = pd.read_csv(INPUT_EVENTS, compression='gzip')

    if 'post_type' not in df.columns:
        df['post_type'] = 'post'

    # =========================================================================
    # 1. THETA & BETA (SOLO POST ORIGINALI)
    # =========================================================================
    df_content = df[(df['post_type'] == 'post') & (df['prev_X'] > 0.1) & (df['v_i'] > 0)].copy()
    print(f"📊 1. Analisi Viralità su {len(df_content)} post originali...")

    # Binning Theta
    try:
        df_content['bin_X'] = pd.qcut(df_content['prev_X'], q=50, duplicates='drop')
    except:
        df_content['bin_X'] = pd.qcut(df_content['prev_X'], q=10, duplicates='drop')

    binned_success = df_content.groupby('bin_X', observed=True).agg({'prev_X': 'mean', 'v_i': 'mean'}).dropna()
    
    slope, intercept, r_val, _, _ = linregress(np.log(binned_success['prev_X']), np.log(binned_success['v_i']))
    GLOBAL_THETA = slope
    print(f"   ✅ THETA (Viralità): {GLOBAL_THETA:.4f}")

    # Plot Theta
    plt.figure(figsize=(6,4))
    plt.scatter(binned_success['prev_X'], binned_success['v_i'], color='black', alpha=0.6)
    plt.plot(binned_success['prev_X'], np.exp(intercept) * binned_success['prev_X']**slope, 'r--')
    plt.xscale('log'); plt.yscale('log'); plt.title(f"Viralità (Theta={GLOBAL_THETA:.2f})"); 
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "2a_assumption1_theta.png")); plt.close()

    # Beta Lognormale
    df_content['beta'] = df_content['v_i'] / (df_content['prev_X'] ** GLOBAL_THETA)
    betas = df_content['beta'].dropna()[df_content['beta'] > 0]
    mu_fit, sigma_fit = np.mean(np.log(betas)), np.std(np.log(betas))
    print(f"   ✅ BETA (Qualità): Mu={mu_fit:.2f}, Sigma={sigma_fit:.2f}")

    # Plot Beta
    plt.figure(figsize=(6,4))
    x_th = np.logspace(np.log10(betas.min()), np.log10(betas.max()), 200)
    sns.histplot(betas, stat='density', log_scale=True, element="step", fill=False, color='blue')
    plt.plot(x_th, lognorm.pdf(x_th, s=sigma_fit, scale=np.exp(mu_fit)), 'r-', label='Lognormale')
    plt.xscale('log'); plt.yscale('log'); plt.title(f"Qualità (Sigma={sigma_fit:.2f})"); plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "2b_assumption2_beta.png")); plt.close()

    # =========================================================================
    # 2. ATTIVITÀ: CALCOLO DOPPIO (TOTALE vs CREATIVA)
    # =========================================================================
    print("\n📊 2. Analisi Attività (Phi)...")
    
    # --- A. ATTIVITÀ TOTALE (Post + Repost) ---
    df_full = df.sort_values(['did', 'ts'])
    df_full['inter_time'] = df_full.groupby('did')['ts'].diff()
    df_tot = df_full.dropna(subset=['inter_time'])
    df_tot = df_tot[(df_tot['prev_X'] > 1) & (df_tot['inter_time'] > 60)] # Filtro spam < 1 min
    
    # QUI CHIAMIAMO LA COLONNA 'rate'
    df_tot['rate'] = 86400.0 / df_tot['inter_time']
    df_tot = df_tot[df_tot['rate'] < 500]

    # Binning Totale
    try:
        df_tot['bin_X'] = pd.qcut(df_tot['prev_X'], q=40, duplicates='drop')
    except:
        df_tot['bin_X'] = pd.qcut(df_tot['prev_X'], q=10, duplicates='drop')
        
    bin_tot = df_tot.groupby('bin_X', observed=True).agg({'prev_X': 'mean', 'rate': 'mean'}).dropna()
    
    # Fit Totale
    phi_tot, label_tot, _, (xf_tot, yf_tot) = fit_activity_curve(bin_tot)
    print(f"   🔹 Phi TOTALE (Visibilità): {phi_tot:.4f}")

    # --- B. ATTIVITÀ CREATIVA (Solo Post Originali) ---
    df_orig = df[df['post_type'] == 'post'].sort_values(['did', 'ts']).copy()
    df_orig['inter_time'] = df_orig.groupby('did')['ts'].diff()
    df_orig = df_orig.dropna(subset=['inter_time'])
    df_orig = df_orig[(df_orig['prev_X'] > 1) & (df_orig['inter_time'] > 60)]
    
    df_orig['rate'] = 86400.0 / df_orig['inter_time']
    df_orig = df_orig[df_orig['rate'] < 500]

    # Binning Creativo
    try:
        df_orig['bin_X'] = pd.qcut(df_orig['prev_X'], q=40, duplicates='drop')
    except:
        df_orig['bin_X'] = pd.qcut(df_orig['prev_X'], q=10, duplicates='drop')
        
    bin_orig = df_orig.groupby('bin_X', observed=True).agg({'prev_X': 'mean', 'rate': 'mean'}).dropna()

    # Fit Creativo
    phi_creat, label_creat, _, (xf_creat, yf_creat) = fit_activity_curve(bin_orig)
    print(f"   🔸 Phi CREATIVO (Sforzo Umano): {phi_creat:.4f}")

    # =========================================================================
    # GRAFICO COMPARATIVO ATTIVITÀ
    # =========================================================================
    plt.figure(figsize=(10, 7))
    plt.scatter(bin_tot['prev_X'], bin_tot['rate'], color='blue', alpha=0.3, label='Dati Totali (Post+Repost)')
    plt.plot(xf_tot, yf_tot, 'b-', lw=2, label=f'Fit Totale: Phi={phi_tot:.2f}')
    plt.scatter(bin_orig['prev_X'], bin_orig['rate'], color='green', marker='x', alpha=0.5, label='Dati Creativi (Solo Post)')
    plt.plot(xf_creat, yf_creat, 'g--', lw=2, label=f'Fit Creativo: Phi={phi_creat:.2f}')
    plt.xscale('log'); plt.yscale('log')
    plt.title("Assumption 3: Attività Totale vs Sforzo Creativo")
    plt.xlabel("Popolarità (X)"); plt.ylabel("Azioni al Giorno")
    plt.legend(); plt.grid(True, which="both", ls="--", alpha=0.3)
    out_path = os.path.join(OUTPUT_DIR_IMGS, "2c_assumption3_dual_activity.png")
    plt.savefig(out_path); plt.close()

    # =========================================================================
    # SALVATAGGIO DATI UTENTE (FIXED: ORA INCLUDE n_posts_analyzed)
    # =========================================================================
    print("\n💾 Salvataggio dati utente (con conteggio post)...")
    
    # 1. Qualità (Beta) + CONTEGGIO POST
    user_q = df_content.groupby('did').agg({
        'beta': 'mean',
        'v_i': 'count' # <--- ECCO LA COLONNA MANCANTE AGGIUNTA
    }).reset_index().rename(columns={'beta': 'beta_merit', 'v_i': 'n_posts_analyzed'})
    
    # 2. Attività Totale
    user_a = df_tot.groupby('did')['rate'].mean().reset_index(name='lambda_total')
    
    # 3. Attività Creativa
    user_c = df_orig.groupby('did')['rate'].mean().reset_index(name='lambda_creative')
    
    # Merge
    user_params = pd.merge(user_q, user_a, on='did', how='left')
    user_params = pd.merge(user_params, user_c, on='did', how='left')
    
    user_params.to_csv(OUTPUT_PARAMS, index=False)

    # REPORT
    report = f"""
    === REPORT STEP 2: STIMA PARAMETRI (DUAL PHI) ===
    1. THETA (VIRALITÀ): {GLOBAL_THETA:.4f}
    2. BETA (MERITO): Mu={mu_fit:.4f}, Sigma={sigma_fit:.4f}
    3. LAMBDA (ATTIVITÀ):
       - Visibilità Totale (Post + Repost): Phi = {phi_tot:.4f}
       - Sforzo Creativo (Solo Post): Phi = {phi_creat:.4f}
    """
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
    
    print(report)
    print("✅ STEP 2 COMPLETATO.")

if __name__ == "__main__":
    main()