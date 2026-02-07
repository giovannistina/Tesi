# Tesi / experiments / equazione_differenziale / infl_wanna-be / 1_segmentation.py

import pandas as pd
import numpy as np
import os
import sys

# --- CONFIGURAZIONE PERCORSI ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_METRICS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_metrics.csv"))

# OUTPUT
OUTPUT_GROUPS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_groups.csv"))
OUTPUT_REPORT = os.path.abspath(os.path.join(CURRENT_DIR, "../data/segmentation_report.txt"))

def main():
    print(f"--- Modulo: Segmentazione Utenti (Status + Vitality Check) v2 ---")
    
    if not os.path.exists(INPUT_METRICS):
        print(f"❌ Errore: {INPUT_METRICS} non trovato.")
        sys.exit(1)

    report_lines = []
    report_lines.append("=== REPORT SEGMENTAZIONE UTENTI (LOGICA CORRETTA) ===")
    report_lines.append(f"Data: {pd.Timestamp.now()}\n")

    # 1. Caricamento Metriche
    df = pd.read_csv(INPUT_METRICS)
    msg = f"👥 Utenti totali nel DB: {len(df)}"
    print(msg)
    report_lines.append(msg)

    if 'followers_count' not in df.columns:
        print("❌ ERRORE CRITICO: Manca 'followers_count'.")
        sys.exit(1)

    # 2. Filtro Attività Minima
    min_posts = 5
    active_users = df[df['valid_post_count'] >= min_posts].copy()
    msg = f"📉 Utenti attivi : {len(active_users)} (>{min_posts} post validi, altrimenti non si possono considerare influencer o wanna-be)"
    print(msg)
    report_lines.append(msg)

    # 3. DEFINIZIONE SOGLIE
    # ELITE = Top 1%
    thresh_elite = active_users['followers_count'].quantile(0.99)
    # WANNABE START = Mediana (50%)
    thresh_wannabe_start = active_users['followers_count'].quantile(0.50)
    # WANNABE END = coincide con l'inizio dell'Elite (chiudiamo il buco 90-99%)
    
    vitality_threshold = active_users['avg_jump_size'].median()

    # Log Parametri
    report_lines.append("\n--- SOGLIE CALCOLATE ---")
    report_lines.append(f"👑 SOGLIA ELITE (Top 1%): > {thresh_elite:,.0f} followers")
    report_lines.append(f"🏃 FASCIA WANNABE (50% - 99%): tra {thresh_wannabe_start:,.0f} e {thresh_elite:,.0f} followers")
    report_lines.append(f"❤️ SOGLIA VITALITÀ (Like Medi): > {vitality_threshold:.2f}")
    
    print(f"\n📊 Parametri Calcolati:")
    print(f"   👑 Elite: > {thresh_elite:,.0f}")
    print(f"   🏃 Wannabe: {thresh_wannabe_start:,.0f} - {thresh_elite:,.0f}")

    # 4. ASSEGNAZIONE GRUPPI
    def assign_group(row):
        # CASO 1: INATTIVI (Finiamo in OTHER indipendentemente dai follower)
        # Nota: Qui potremmo trovare dei "Giganti dormienti"
        if row['valid_post_count'] < min_posts:
            return 'OTHER'
        
        followers = row['followers_count']
        performance = row['avg_jump_size']
        
        # CASO 2: ELITE (Top 1%)
        if followers >= thresh_elite:
            if performance >= vitality_threshold:
                return 'INFLUENCER'
            else:
                return 'ZOMBIE'
        
        # CASO 3: WANNABE (Dal 50% fino al 99%)
        # Nota: Ora include anche la fascia 90-99 che prima finiva in Other
        elif followers >= thresh_wannabe_start:
            return 'WANNABE'
            
        # CASO 4: USER COMUNI (Sotto la mediana)
        else:
            return 'OTHER'

    df['group'] = df.apply(assign_group, axis=1)

    # 5. Salvataggio CSV
    cols_to_save = ['did', 'group', 'followers_count', 'total_likes', 'avg_jump_size', 'valid_post_count']
    output_df = df[cols_to_save].sort_values(by=['followers_count'], ascending=False)
    
    output_df.to_csv(OUTPUT_GROUPS, index=False)
    print(f"\n✅ File CSV salvato: {OUTPUT_GROUPS}")

    # --- REPORTING ---
    counts = output_df['group'].value_counts()
    report_lines.append("\n--- DISTRIBUZIONE GRUPPI ---")
    report_lines.append(counts.to_string())
    print("\n📈 Distribuzione:")
    print(counts)

    report_lines.append("\n" + "="*80)
    report_lines.append("   ISPEZIONE CAMPIONI (TOP 5 PER CATEGORIA)")
    report_lines.append("="*80)
    
    categories = ['INFLUENCER', 'ZOMBIE', 'WANNABE', 'OTHER']
    
    for cat in categories:
        report_lines.append(f"\n📂 CATEGORIA: {cat}")
        
        subset = output_df[output_df['group'] == cat].head(5)
        
        if len(subset) == 0:
            report_lines.append("   (Nessun utente)")
            continue
            
        header = f"{'DID':<35} | {'Followers':<10} | {'Avg Likes':<10} | {'Post':<5}"
        report_lines.append("-" * len(header))
        report_lines.append(header)
        report_lines.append("-" * len(header))
        
        for _, row in subset.iterrows():
            did = row['did']
            fol = int(row['followers_count'])
            perf = row['avg_jump_size']
            posts = int(row['valid_post_count'])
            line = f"{did:<35} | {fol:<10,d} | {perf:<10.2f} | {posts:<5}"
            report_lines.append(line)

    with open(OUTPUT_REPORT, "w", encoding='utf-8') as f:
        f.write("\n".join(report_lines))
        
    print(f"✅ Report salvato: {OUTPUT_REPORT}")
    print(f"   (Controlla ora: i Wannabe dovrebbero avere più follower degli Other attivi)")

if __name__ == "__main__":
    main()