# Tesi / experiments / equazione_differenziale / steps / 1b_compute_Xt.py

import pandas as pd
import numpy as np
import os
import sys
from tqdm import tqdm 

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Legge l'output di 1a (che DEVE contenere la colonna 'post_type')
INPUT_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_log.csv.gz"))

OUTPUT_EVENTS_ENRICHED = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))
OUTPUT_USER_METRICS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_metrics.csv"))

# --- PARAMETRI DEL MODELLO ---
HALF_LIFE_HOURS = 24.0 
BURN_IN_DAYS = 30 

def process_user_history(group, gamma, burn_in_seconds):
    """
    Calcola la serie temporale X(t).
    NOTA: La vitalità (avg_jump_size) viene calcolata SOLO sui post originali.
    """
    # 1. Ordinamento temporale
    group = group.sort_values('ts')
    timestamps = group['ts'].values
    v_values = group['v_i'].values 
    
    # RECUPERO TIPO DI POST (Fondamentale per la modifica)
    post_types = group['post_type'].values
    
    # Recupero colonne extra
    likes_vals = group['likes'].values
    reposts_vals = group['reposts'].values
    replies_vals = group['replies'].values
    followers_val = group['followers_count'].values[0] if 'followers_count' in group.columns else 0

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
    # Qui usiamo TUTTI gli eventi (anche i repost) per la continuità temporale
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

    # --- FILTRO VALIDITÀ (BURN-IN) ---
    # 1. Maschera Temporale: Solo eventi dopo i 30 giorni
    time_mask = timestamps > t_threshold
    
    if np.sum(time_mask) == 0:
        return None, None

    # 2. Maschera Qualitativa: Solo POST ORIGINALI (per le metriche di vitalità)
    #    (Escludiamo i 'repost' dal calcolo della media voto)
    original_post_mask = (post_types == 'post')
    
    # Combinazione: Post originali avvenuti dopo il burn-in
    metrics_mask = time_mask & original_post_mask
    
    valid_post_count_original = np.sum(metrics_mask)
    
    # SE L'UTENTE NON HA POST ORIGINALI NEL PERIODO VALIDO:
    # Lo scartiamo (o restituiamo None), perché non possiamo valutare la sua "bravura".
    # Chi vive di soli repost non è un creator.
    if valid_post_count_original == 0:
        return None, None

    # --- CALCOLO METRICHE (Modificato) ---
    
    mean_x_temporal = integral_X / valid_time_duration if valid_time_duration > 0 else 0.0
    
    # QUI LA MODIFICA: La media è calcolata solo sui post originali
    avg_jump_size = np.mean(v_values[metrics_mask]) 
    
    # La frequenza (lambda) è calcolata sui post originali (quanto spesso CREI contenuti)
    lambda_val = valid_post_count_original / (valid_time_duration / 86400.0) if valid_time_duration > 0 else 0

    # Somme totali (Solo sui post originali validi)
    total_likes = np.sum(likes_vals[metrics_mask])
    total_reposts = np.sum(reposts_vals[metrics_mask])
    total_replies = np.sum(replies_vals[metrics_mask])

    metrics = {
        'did': None, 
        'lambda': lambda_val,
        'avg_jump_size': avg_jump_size,  # <--- PULITA (Solo Post)
        'mean_X': mean_x_temporal,       # <--- MISTO (Include inerzia repost)
        'post_count': len(timestamps), 
        'valid_post_count': valid_post_count_original, # <--- SOLO ORIGINALI
        'duration_days': (t_end - t_start) / 86400.0,
        'total_likes': total_likes,
        'total_reposts': total_reposts,
        'total_replies': total_replies,
        'followers_count': followers_val
    }
    
    group['prev_X'] = x_history
    
    # Restituiamo il dataframe con TUTTI gli eventi validi temporalmente (anche i repost)
    # perché servono per visualizzare la storia completa nei grafici
    group_valid = group.loc[time_mask].copy()
    
    return metrics, group_valid

def main():
    print(f"--- Modulo B: Calcolo X(t) v2 (Vitality on Posts Only) ---", flush=True)
    
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
    
    for did, group in tqdm(grouped, total=len(unique_users), desc="Processing Users", mininterval=10.0):
        mets, enriched_df = process_user_history(group, gamma, burn_in_sec)
        
        if mets is not None:
            mets['did'] = did 
            all_metrics.append(mets)
            all_enriched_events.append(enriched_df)
            
    print("\n💾 Salvataggio risultati...", flush=True)
    
    # 1. SALVATAGGIO USER METRICS
    if all_metrics:
        metrics_df = pd.DataFrame(all_metrics)
        
        cols = ['did', 'followers_count', 'valid_post_count', 'total_likes', 'total_reposts', 'total_replies', 
                'mean_X', 'avg_jump_size', 'lambda', 'duration_days', 'post_count']
        
        existing_cols = list(metrics_df.columns)
        final_cols = [c for c in cols if c in existing_cols] + [c for c in existing_cols if c not in cols]
        metrics_df = metrics_df[final_cols]
        
        os.makedirs(os.path.dirname(OUTPUT_USER_METRICS), exist_ok=True)
        metrics_df.to_csv(OUTPUT_USER_METRICS, index=False)
        print(f"✅ User Metrics salvate: {OUTPUT_USER_METRICS}")
        print(f"ℹ️  INFO: 'avg_jump_size' calcolato solo su {metrics_df['valid_post_count'].sum()} post originali.")
    
    # 2. SALVATAGGIO EVENTI ARRICCHITI
    if all_enriched_events:
        enriched_final_df = pd.concat(all_enriched_events)
        
        cols_enriched = list(enriched_final_df.columns)
        if 'did' in cols_enriched:
            cols_enriched.insert(0, cols_enriched.pop(cols_enriched.index('did')))
        enriched_final_df = enriched_final_df[cols_enriched]

        enriched_final_df.to_csv(OUTPUT_EVENTS_ENRICHED, index=False, compression='gzip')
        print(f"✅ Eventi Arricchiti salvati: {OUTPUT_EVENTS_ENRICHED}")
    else:
        print("⚠️ Nessun dato valido prodotto (forse tutti hanno solo repost o <30gg?).")

if __name__ == "__main__":
    main()