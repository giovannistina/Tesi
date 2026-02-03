# Tesi / experiments / equazione_differenziale / step_1 / b_compute_Xt.py

import pandas as pd
import numpy as np
import os
import sys
from tqdm import tqdm 

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_log.csv.gz"))

OUTPUT_EVENTS_ENRICHED = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))
OUTPUT_USER_METRICS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_metrics.csv"))

# --- PARAMETRI DEL MODELLO ---
HALF_LIFE_HOURS = 24.0 
BURN_IN_DAYS = 30 

def process_user_history(group, gamma, burn_in_seconds):
    """
    Calcola la serie temporale X(t) per un singolo utente.
    """
    # 1. Ordinamento temporale
    group = group.sort_values('ts')
    timestamps = group['ts'].values
    v_values = group['v_i'].values 
    
    if len(timestamps) < 2: 
        return None, None

    t_start = timestamps[0]
    t_end = timestamps[-1]
    t_threshold = t_start + burn_in_seconds

    # Array per salvare la X(t) istantanea PRE-SALTO
    x_history = np.zeros(len(timestamps))
    
    current_X = 0.0
    integral_X = 0.0
    valid_time_duration = 0.0
    prev_t = t_start
    
    # --- CICLO DI RICOSTRUZIONE X(t) ---
    for i in range(len(timestamps)):
        t = timestamps[i]
        v = v_values[i]
        
        dt = t - prev_t
        
        # A. Decadimento
        decay_factor = np.exp(-gamma * dt) if dt > 0 else 1.0
        
        # Integrale
        if dt > 0:
            segment_area = current_X * (1.0 - decay_factor) / gamma
            if t > t_threshold:
                integral_X += segment_area
                valid_time_duration += dt

        # Applichiamo decadimento
        current_X *= decay_factor
        
        # B. Snapshot X(t^-)
        x_history[i] = current_X
        
        # C. Salto
        current_X += v
        
        prev_t = t

    # --- SALVATAGGIO DATI VALIDI ---
    valid_mask = timestamps > t_threshold
    
    if np.sum(valid_mask) == 0:
        return None, None

    # Calcolo metriche aggregate
    mean_x_temporal = integral_X / valid_time_duration if valid_time_duration > 0 else 0.0
    avg_jump_size = np.mean(v_values[valid_mask]) 
    lambda_val = np.sum(valid_mask) / (valid_time_duration / 86400.0) if valid_time_duration > 0 else 0

    metrics = {
        # 'did' verrà aggiunto dopo nel loop principale
        'lambda': lambda_val,
        'avg_jump_size': avg_jump_size, 
        'mean_X': mean_x_temporal,
        'post_count': len(timestamps), 
        'valid_post_count': np.sum(valid_mask),
        'duration_days': (t_end - t_start) / 86400.0
    }
    
    group['prev_X'] = x_history
    group_valid = group.loc[valid_mask].copy()
    
    return metrics, group_valid

def main():
    print(f"--- Modulo B: Calcolo X(t) (Engagement Totale) ---", flush=True)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Errore: Input {INPUT_FILE} mancante.", flush=True)
        sys.exit(1)
        
    print(f"📥 Lettura eventi da: {INPUT_FILE} ...", flush=True)
    df = pd.read_csv(INPUT_FILE, compression='gzip') 
    
    gamma = np.log(2) / (HALF_LIFE_HOURS * 3600)
    burn_in_sec = BURN_IN_DAYS * 86400
    
    print(f"⚙️  Parametri: Half-Life={HALF_LIFE_HOURS}h, Burn-In={BURN_IN_DAYS}gg")
    
    all_metrics = []
    all_enriched_events = []
    
    unique_users = df['did'].unique()
    print(f"🚀 Inizio elaborazione per {len(unique_users)} utenti...", flush=True)
    
    grouped = df.groupby('did', sort=False)
    
    # MODIFICA 1: mininterval=10.0 fa aggiornare la stampa ogni 10 secondi
    for did, group in tqdm(grouped, total=len(unique_users), desc="Processing Users", mininterval=10.0):
        mets, enriched_df = process_user_history(group, gamma, burn_in_sec)
        
        if mets is not None:
            mets['did'] = did  # Inseriamo il DID nel dizionario
            all_metrics.append(mets)
            all_enriched_events.append(enriched_df)
            
    print("\n💾 Salvataggio risultati...", flush=True)
    
    # 1. Metriche Utenti
    if all_metrics:
        metrics_df = pd.DataFrame(all_metrics)
        
        # MODIFICA 2: Riordiniamo le colonne per avere 'did' all'inizio
        cols = ['did'] + [c for c in metrics_df.columns if c != 'did']
        metrics_df = metrics_df[cols]
        
        os.makedirs(os.path.dirname(OUTPUT_USER_METRICS), exist_ok=True)
        metrics_df.to_csv(OUTPUT_USER_METRICS, index=False)
        print(f"✅ User Metrics salvate (con DID in prima colonna): {OUTPUT_USER_METRICS}")
    
    # 2. Eventi Arricchiti
    if all_enriched_events:
        enriched_final_df = pd.concat(all_enriched_events)
        
        # Anche qui, per sicurezza, mettiamo 'did' e 'ts' all'inizio se possibile
        cols_enriched = list(enriched_final_df.columns)
        if 'did' in cols_enriched:
            cols_enriched.insert(0, cols_enriched.pop(cols_enriched.index('did')))
        enriched_final_df = enriched_final_df[cols_enriched]

        enriched_final_df.to_csv(OUTPUT_EVENTS_ENRICHED, index=False, compression='gzip')
        print(f"✅ Eventi Arricchiti salvati: {OUTPUT_EVENTS_ENRICHED}")
    else:
        print("⚠️ Nessun dato valido prodotto.")

if __name__ == "__main__":
    main()