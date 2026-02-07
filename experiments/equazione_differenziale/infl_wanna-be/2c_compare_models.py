# Tesi / experiments / equazione_differenziale / infl_wanna-be / 2c_compare_models.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
import statsmodels.api as sm
from tqdm import tqdm

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_EVENTS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))
INPUT_GROUPS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_groups.csv"))
OUTPUT_REPORT = os.path.join(CURRENT_DIR, "plots/model_comparison_report.txt")

MAX_USERS_PER_GROUP = 300 

def load_data():
    print("📂 Caricamento dati...")
    df_groups = pd.read_csv(INPUT_GROUPS)
    
    selected_dids = []
    np.random.seed(42)
    for grp in ['INFLUENCER', 'WANNABE']:
        dids = df_groups[df_groups['group'] == grp]['did'].unique()
        chosen = np.random.choice(dids, min(len(dids), MAX_USERS_PER_GROUP), replace=False)
        selected_dids.extend(chosen)
        
    df_events = pd.read_csv(INPUT_EVENTS, compression='gzip', usecols=['did', 'ts', 'prev_X'])
    df_events = df_events[df_events['did'].isin(selected_dids)]
    
    df = df_events.merge(df_groups[['did', 'group']], on='did')
    df['date'] = pd.to_datetime(df['ts'], unit='s')
    return df

def prepare_data(df, group_name):
    print(f"⚙️  Preparing Pooled Data: {group_name}...")
    X_list = []
    dX_list = []
    
    subset = df[df['group'] == group_name]
    for did in tqdm(subset['did'].unique(), leave=False):
        user_data = subset[subset['did'] == did].sort_values('ts').set_index('date')
        daily = user_data['prev_X'].resample('1D').mean().dropna()
        if len(daily) < 3: continue
        
        vals = daily.values
        X_t = vals[:-1]
        dX = vals[1:] - vals[:-1]
        
        # Filtriamo valori <= 0 per il CIR (che richiede radice quadrata)
        valid_idx = X_t > 0
        X_list.extend(X_t[valid_idx])
        dX_list.extend(dX[valid_idx])
        
    return np.array(X_list), np.array(dX_list)

def fit_ou(X, dX, dt=1.0):
    """ Fit Ornstein-Uhlenbeck: dX ~ A + B*X """
    X_with_const = sm.add_constant(X)
    model = sm.OLS(dX, X_with_const).fit()
    
    theta = -model.params[1]
    mu = model.params[0] / theta if theta != 0 else 0
    sigma = np.std(model.resid) / np.sqrt(dt)
    
    return {'name': 'OU (Standard)', 'R2': model.rsquared, 'AIC': model.aic, 'params': (theta, mu, sigma)}

def fit_cir(X, dX, dt=1.0):
    """ 
    Fit Cox-Ingersoll-Ross: dX/sqrt(X) ~ A/sqrt(X) + B*sqrt(X) 
    Questa trasformazione rende i residui omoschedastici.
    """
    sqrt_X = np.sqrt(X)
    y_trans = dX / sqrt_X
    
    # Feature 1: 1/sqrt(X)
    x1 = 1.0 / sqrt_X
    # Feature 2: sqrt(X)
    x2 = sqrt_X
    
    # Regressione senza intercetta (l'intercetta è implicita nella struttura)
    features = np.column_stack((x1, x2))
    model = sm.OLS(y_trans, features).fit()
    
    # Recupero parametri fisici
    # Coeff x1 = theta * mu
    # Coeff x2 = -theta
    theta = -model.params[1]
    mu = model.params[0] / theta if theta != 0 else 0
    sigma = np.std(model.resid) / np.sqrt(dt) # Sigma qui è la volatilità del termine browniano
    
    # Calcoliamo un R2 "Pseudo" sui dati originali per confronto equo
    # Predizione dX = theta*(mu - X)
    dX_pred = theta * (mu - X)
    ss_res = np.sum((dX - dX_pred)**2)
    ss_tot = np.sum((dX - np.mean(dX))**2)
    pseudo_r2 = 1 - (ss_res / ss_tot)
    
    return {'name': 'CIR (Volatilità Dinamica)', 'R2': pseudo_r2, 'AIC': model.aic, 'params': (theta, mu, sigma)}

def main():
    df = load_data()
    report = ["=== CONFRONTO MODELLI MATEMATICI ===\n"]
    
    for grp in ['INFLUENCER', 'WANNABE']:
        report.append(f"GRUPPO: {grp}")
        X, dX = prepare_data(df, grp)
        
        # 1. Fit OU
        res_ou = fit_ou(X, dX)
        
        # 2. Fit CIR
        res_cir = fit_cir(X, dX)
        
        # Report OU
        t, m, s = res_ou['params']
        report.append(f"  [Modello 1: OU (Rumore Costante)]")
        report.append(f"    R^2:    {res_ou['R2']:.5f}")
        report.append(f"    AIC:    {res_ou['AIC']:.0f} (Più basso è meglio)")
        report.append(f"    Theta:  {t:.4f} | Mu: {m:.1f} | Sigma: {s:.1f}")
        
        # Report CIR
        t2, m2, s2 = res_cir['params']
        report.append(f"  [Modello 2: CIR (Rumore Crescente)]")
        report.append(f"    R^2*:   {res_cir['R2']:.5f} (Pseudo)")
        report.append(f"    AIC:    {res_cir['AIC']:.0f} (Più basso è meglio)")
        report.append(f"    Theta:  {t2:.4f} | Mu: {m2:.1f} | Sigma: {s2:.1f}")
        
        # Verdetto
        winner = "CIR" if res_cir['AIC'] < res_ou['AIC'] else "OU"
        report.append(f"  🏆 VINCITORE STATISTICO: {winner}")
        report.append("-" * 40)
        
    with open(OUTPUT_REPORT, "w") as f:
        f.write("\n".join(report))
    
    print(f"✅ Analisi completata. Leggi: {os.path.basename(OUTPUT_REPORT)}")

if __name__ == "__main__":
    main()