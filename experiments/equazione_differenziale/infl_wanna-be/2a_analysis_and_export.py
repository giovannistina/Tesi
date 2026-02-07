# Tesi / experiments / equazione_differenziale / infl_wanna-be / 2_analysis_and_export.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys

# --- CONFIGURAZIONE PERCORSI ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# INPUT
INPUT_EVENTS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/events_enriched.csv.gz"))
INPUT_GROUPS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_groups.csv"))

# OUTPUT
OUTPUT_PLOTS_DIR = os.path.join(CURRENT_DIR, "plots")
OUTPUT_AGGREGATED_CSV = os.path.abspath(os.path.join(CURRENT_DIR, "../data/grouped_timeseries.csv"))
OUTPUT_USER_REPORT = os.path.join(CURRENT_DIR, "plots/selected_users_for_inspection.txt")

# CONFIGURAZIONE GRAFICA
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (14, 7)
COLORS = {'INFLUENCER': 'red', 'WANNABE': 'blue'}

# FREQUENZA DI AGGREGAZIONE
AGGREGATION_FREQ = 'D' 

# Quanti utenti mostrare nel grafico Micro
N_SAMPLES = 5 

def load_data():
    print("📂 Caricamento dati...")
    if not os.path.exists(INPUT_GROUPS) or not os.path.exists(INPUT_EVENTS):
        print(f"❌ Errore: File di input mancanti.")
        sys.exit(1)
        
    df_groups = pd.read_csv(INPUT_GROUPS)
    df_events = pd.read_csv(INPUT_EVENTS, compression='gzip', 
                           usecols=['did', 'ts', 'prev_X', 'v_i'])
    
    print("🔗 Merge e Filtro (Solo INFLUENCER e WANNABE)...")
    df_merged = df_events.merge(df_groups[['did', 'group', 'followers_count']], on='did', how='inner')
    
    target_groups = ['INFLUENCER', 'WANNABE']
    df_merged = df_merged[df_merged['group'].isin(target_groups)].copy()
    
    df_merged['date'] = pd.to_datetime(df_merged['ts'], unit='s')
    
    return df_merged

def plot_micro_dynamics_and_report(df):
    """
    Step 2a: Plot Micro-Dinamica + Report
    """
    print("🎨 [Step 2a] Selezione utenti, Plot Micro e Report...")
    os.makedirs(OUTPUT_PLOTS_DIR, exist_ok=True)
    
    np.random.seed(42) 
    
    selected_dids = []
    report_lines = []
    report_lines.append("=== REPORT UTENTI SELEZIONATI PER GRAFICO MICRO-DINAMICA ===\n")
    
    for grp in ['INFLUENCER', 'WANNABE']:
        candidates_df = df[df['group'] == grp][['did', 'followers_count']].drop_duplicates()
        candidates = candidates_df['did'].values
        
        if len(candidates) > 0:
            n_select = min(N_SAMPLES, len(candidates))
            chosen = np.random.choice(candidates, n_select, replace=False)
            selected_dids.extend(chosen)
            
            report_lines.append(f"\nGruppo: {grp}")
            report_lines.append("-" * 60)
            for user_did in chosen:
                fol = candidates_df[candidates_df['did'] == user_did]['followers_count'].values[0]
                report_lines.append(f"DID:       {user_did}")
                report_lines.append(f"Followers: {fol}")
                report_lines.append(f"Link:      https://bsky.app/profile/{user_did}")
                report_lines.append("-" * 30)
    
    with open(OUTPUT_USER_REPORT, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))
    print(f"📄 Report utenti salvato: {OUTPUT_USER_REPORT}")

    if not selected_dids: return

    subset = df[df['did'].isin(selected_dids)].copy()
    
    plt.figure()
    sns.lineplot(data=subset, x='date', y='prev_X', hue='group', units='did', estimator=None,
                 palette=COLORS, alpha=0.7, linewidth=1.2)
    
    plt.title(f"Micro-Dinamica: Traiettorie Individuali ({N_SAMPLES} casi per gruppo)", fontsize=16)
    plt.yscale('log')
    plt.xlabel("")
    plt.ylabel("Influenza X(t)")
    
    handles, labels = plt.gca().get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    plt.legend(by_label.values(), by_label.keys(), loc='upper left')
    
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_PLOTS_DIR, "2a_micro_trajectories.png"), dpi=300)
    plt.close()

def aggregate_and_export(df):
    """
    Step 2b: Aggregazione
    """
    print(f"🧮 [Step 2b] Aggregazione Serie Temporali (Freq: {AGGREGATION_FREQ})...")
    
    df['time_bin'] = df['date'].dt.floor(AGGREGATION_FREQ)
    
    grouped_stats = df.groupby(['time_bin', 'group'])['prev_X'].agg(['mean', 'std', 'count']).reset_index()
    grouped_stats.columns = ['date', 'group', 'mean_X', 'std_X', 'event_count']
    
    grouped_stats.to_csv(OUTPUT_AGGREGATED_CSV, index=False)
    print(f"✅ Dataset aggregato salvato: {OUTPUT_AGGREGATED_CSV}")
    return grouped_stats

def plot_macro_dynamics_from_agg(agg_df):
    """
    Step 2a (Parte 2): Plot Macro Standard (Scala Logaritmica Unica)
    """
    print("🎨 [Step 2a] Plot Macro-Dinamica (Standard)...")
    
    plt.figure()
    sns.lineplot(data=agg_df, x='date', y='mean_X', hue='group', 
                 palette=COLORS, linewidth=2.5)
    
    groups = agg_df['group'].unique()
    for grp in groups:
        subset = agg_df[agg_df['group'] == grp].sort_values('date')
        plt.fill_between(subset['date'], 
                         subset['mean_X'] - subset['std_X'], 
                         subset['mean_X'] + subset['std_X'], 
                         color=COLORS[grp], alpha=0.15)

    plt.title(f"Macro-Dinamica: Trend Medio (Scala Log)", fontsize=16)
    plt.ylabel("Influenza Media X(t)")
    plt.xlabel("")
    plt.yscale('log')
    plt.grid(True, which="both", ls="-", alpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_PLOTS_DIR, "2a_macro_trends_log.png"), dpi=300)
    plt.close()

def plot_macro_dual_axis(agg_df):
    """
    Step 2a (Parte 3): Plot Macro DOPPIA SCALA (Twin Axis)
    Permette di vedere la forma d'onda del Wannabe "zoomata" sulla destra.
    """
    print("🎨 [Step 2a] Plot Macro-Dinamica (Doppia Scala)...")
    
    # Creiamo la figura e il primo asse (Sinistro)
    fig, ax1 = plt.subplots(figsize=(14, 7))
    
    # Dati separati
    inf_data = agg_df[agg_df['group'] == 'INFLUENCER'].sort_values('date')
    wan_data = agg_df[agg_df['group'] == 'WANNABE'].sort_values('date')
    
    # --- ASSE SINISTRO: INFLUENCER (ROSSO) ---
    color_inf = 'tab:red'
    ax1.set_xlabel('Data')
    ax1.set_ylabel('Influenza X(t) - INFLUENCER', color=color_inf, fontweight='bold', fontsize=12)
    
    # Linea
    ax1.plot(inf_data['date'], inf_data['mean_X'], color=color_inf, linewidth=2.5, label='Influencer')
    # Ombra (Std Dev)
    ax1.fill_between(inf_data['date'], 
                     inf_data['mean_X'] - inf_data['std_X'], 
                     inf_data['mean_X'] + inf_data['std_X'], 
                     color=color_inf, alpha=0.1)
    
    ax1.tick_params(axis='y', labelcolor=color_inf)
    ax1.grid(True, which='major', linestyle='--', alpha=0.5)
    
    # --- ASSE DESTRO: WANNABE (BLU) ---
    ax2 = ax1.twinx()  # Crea il secondo asse che condivide la X
    color_wan = 'tab:blue'
    ax2.set_ylabel('Influenza X(t) - WANNABE', color=color_wan, fontweight='bold', fontsize=12)
    
    # Linea
    ax2.plot(wan_data['date'], wan_data['mean_X'], color=color_wan, linewidth=2.5, label='Wannabe')
    # Ombra (Std Dev)
    ax2.fill_between(wan_data['date'], 
                     wan_data['mean_X'] - wan_data['std_X'], 
                     wan_data['mean_X'] + wan_data['std_X'], 
                     color=color_wan, alpha=0.1)
    
    ax2.tick_params(axis='y', labelcolor=color_wan)
    # Togliamo la griglia del secondo asse per non fare casino
    ax2.grid(False) 
    
    plt.title("Confronto Dinamica Indipendente (Doppia Scala Y)", fontsize=16)
    plt.tight_layout()
    
    outfile = os.path.join(OUTPUT_PLOTS_DIR, "2a_macro_trends_dual_axis.png")
    plt.savefig(outfile, dpi=300)
    plt.close()
    print(f"✅ Salvato Grafico Doppia Scala: {outfile}")

def main():
    print(f"--- Step 2: Analisi Completa Traiettorie ---")
    
    # 1. Caricamento
    df = load_data()
    print(f"📊 Eventi caricati: {len(df)}")
    
    # 2. Micro + Report
    plot_micro_dynamics_and_report(df)
    
    # 3. Aggregazione
    agg_df = aggregate_and_export(df)
    
    # 4. Macro Standard (Log)
    plot_macro_dynamics_from_agg(agg_df)
    
    # 5. Macro Dual Axis (NUOVO)
    plot_macro_dual_axis(agg_df)
    
    print("\n✅ Tutto completato.")

if __name__ == "__main__":
    main()