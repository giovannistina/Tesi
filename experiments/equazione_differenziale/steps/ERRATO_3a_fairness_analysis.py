# Tesi / experiments / equazione_differenziale / steps / 3a_fairness_analysis.py




import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
from scipy.stats import kendalltau, spearmanr, pearsonr

# --- CONFIGURAZIONE ---
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

# Input: Uniamo i risultati dello Step 1 (Popolarità) e dello Step 2 (Qualità/Merito)
INPUT_METRICS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_metrics.csv"))
INPUT_PARAMS = os.path.abspath(os.path.join(CURRENT_DIR, "../data/user_parameters_estimated.csv"))

# Output
OUTPUT_DIR_IMGS = os.path.abspath(os.path.join(CURRENT_DIR, "../results/figures"))
OUTPUT_REPORT = os.path.abspath(os.path.join(CURRENT_DIR, "../results/report_step3.txt"))

def main():
    print("--- STEP 3: ANALISI FAIRNESS E DOMINANZA (COMPLETO) ---")

    # 1. Caricamento e Merge dei Dati
    if not os.path.exists(INPUT_METRICS) or not os.path.exists(INPUT_PARAMS):
        print("❌ Errore: Mancano i file di input (Esegui Step 1 e 2).")
        sys.exit(1)

    # Carichiamo i CSV
    df_metrics = pd.read_csv(INPUT_METRICS) # Contiene mean_X (Popolarità)
    df_params = pd.read_csv(INPUT_PARAMS)   # Contiene beta_merit, n_posts_analyzed (ORIGINALI)

    # Uniamo i dataset usando il 'did' (User ID)
    # Usiamo 'inner' join: teniamo solo utenti presenti in entrambi i file
    df = pd.merge(df_metrics, df_params, on='did', how='inner')
    
    print(f"📥 Utenti totali (Merge Step 1 & 2): {len(df)}")
    
    # =========================================================================
    # FILTRO ATTIVITÀ
    # =========================================================================
    # Consideriamo solo CREATORS stabili (> 5 post originali).
    # n_posts_analyzed viene dallo step 2a e conta solo i post originali.
    MIN_POSTS = 5
    
    # Filtriamo anche per popolarità minima per evitare rumore di fondo
    df_active = df[(df['n_posts_analyzed'] >= MIN_POSTS) & (df['mean_X'] > 1.0)].copy()
    
    # Pulizia NaN
    df_active = df_active.dropna(subset=['beta_merit', 'mean_X'])
    
    print(f"👥 Utenti validi per Fairness (> {MIN_POSTS} post originali): {len(df_active)}")
    print(f"   (Esclusi {len(df) - len(df_active)} utenti poco attivi o solo-reposter)")

    os.makedirs(OUTPUT_DIR_IMGS, exist_ok=True)

    # =========================================================================
    # 1. CORRELAZIONE TRA QUALITÀ (Beta) E POPOLARITÀ (X)
    # =========================================================================
    print("\n⚖️  1. Calcolo Correlazioni (Meritocrazia)...")
    
    # Logaritmi per Pearson (la qualità varia su ordini di grandezza)
    log_beta = np.log1p(df_active['beta_merit'])
    log_X = np.log1p(df_active['mean_X'])

    # Pearson: Correlazione lineare sui logaritmi
    pearson_corr, _ = pearsonr(log_beta, log_X)
    
    # Spearman: Correlazione di rango (non parametrica)
    spearman_corr, _ = spearmanr(df_active['beta_merit'], df_active['mean_X'])

    # Kendall Tau: La misura più robusta per le classifiche
    # Ci dice: "Se l'utente A è più bravo di B, è anche più famoso di B?"
    tau, _ = kendalltau(df_active['beta_merit'], df_active['mean_X'])

    print(f"   🔹 Pearson (Log-Log): {pearson_corr:.4f}")
    print(f"   🔹 Spearman Rank:     {spearman_corr:.4f}")
    print(f"   🔹 Kendall Tau:       {tau:.4f} (Indice principale di Fairness)")

    # Grafico A: Scatter Classico (Valori) 
    plt.figure(figsize=(8, 8))
    # Campioniamo se sono troppi punti per alleggerire il PDF finale
    if len(df_active) > 20000:
        data_plot = df_active.sample(20000)
    else:
        data_plot = df_active
        
    plt.scatter(data_plot['beta_merit'], data_plot['mean_X'], alpha=0.3, s=10, c='teal')
    plt.xscale('log'); plt.yscale('log')
    plt.xlabel(r"Qualità Intrinseca $\beta_{merit}$ (Solo Post Originali)")
    plt.ylabel(r"Popolarità Raggiunta $\langle X \rangle$")
    plt.title(f"Efficienza del Mercato (Meritocrazia)\nKendall Tau = {tau:.2f} (1.0 = Perfetta)")
    
    try:
        m, q = np.polyfit(np.log1p(data_plot['beta_merit']), np.log1p(data_plot['mean_X']), 1)
        x_fit = np.linspace(log_beta.min(), log_beta.max(), 100)
        plt.plot(np.exp(x_fit), np.exp(m*x_fit + q), 'r--', label='Trend', lw=2)
    except: pass
    
    plt.legend()
    plt.grid(True, which="both", ls="--", alpha=0.2)
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "3a_fairness_scatter_values.png"))
    plt.close()

    # =========================================================================
    # 2. ANALISI TOP-K OVERLAP (Dominanza)
    # =========================================================================
    print("\n🏆 2. Analisi Top-K Overlap...")
    
    # Creiamo i ranking (1 = Migliore)
    df_active['rank_X'] = df_active['mean_X'].rank(ascending=False)
    df_active['rank_Beta'] = df_active['beta_merit'].rank(ascending=False)

    k_values = [10, 100, 1000, 5000, 10000]
    overlap_results = []

    for k in k_values:
        if k > len(df_active): break
        
        top_k_famous = set(df_active[df_active['rank_X'] <= k]['did'])
        top_k_best = set(df_active[df_active['rank_Beta'] <= k]['did'])
        
        overlap = len(top_k_famous.intersection(top_k_best))
        perc = (overlap / k) * 100
        overlap_results.append((k, perc))
        print(f"   Top {k}: {perc:.1f}% di sovrapposizione")

    # Grafico B: Overlap Curve 
    if overlap_results:
        ks, percs = zip(*overlap_results)
        plt.figure(figsize=(8, 6))
        plt.plot(ks, percs, marker='o', linestyle='-', color='purple', linewidth=2)
        plt.xscale('log')
        plt.ylim(0, 100) 
        plt.xlabel("Top K Utenti (Log Scale)")
        plt.ylabel("% Sovrapposizione (Bravi & Famosi)")
        plt.title("Allineamento tra Qualità e Popolarità (Top-K Overlap)")
        plt.grid(True, which="both", ls="--", alpha=0.5)
        plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "3b_topk_overlap.png"))
        plt.close()

    # =========================================================================
    # 3. GRAFICO HEXBIN DEI RANGHI (Visualizzazione Tau)
    # =========================================================================
    print("\n📈 3. Generazione Scatter Plot dei Ranghi (Hexbin)...")
    plt.figure(figsize=(9, 8))
    
    # Hexbin plot: Gestisce meglio la densità di punti sovrapposti
    hb = plt.hexbin(df_active['rank_X'], df_active['rank_Beta'], 
                    gridsize=50, cmap='inferno', mincnt=1, bins='log')
    
    cb = plt.colorbar(hb, label='Numero di Utenti (Log)')
    
    # Linea diagonale (Meritocrazia Perfetta)
    max_rank = max(df_active['rank_X'].max(), df_active['rank_Beta'].max())
    plt.plot([0, max_rank], [0, max_rank], 'c--', lw=2, label='Meritocrazia Ideale (Tau=1.0)')

    plt.title(f"Inefficienza del Mercato: Fama vs Qualità\nKendall Tau = {tau:.2f} (Bassa Correlazione)")
    plt.xlabel("Ranking per Fama (Popolarità X)")
    plt.ylabel("Ranking per Qualità (Merito Beta)")
    
    # Invertiamo gli assi: In basso a sinistra c'è la "Top 1"
    plt.gca().invert_xaxis()
    plt.gca().invert_yaxis()
    
    plt.legend(loc='upper left')
    plt.grid(True, alpha=0.3)
    
    outfile_hex = os.path.join(OUTPUT_DIR_IMGS, "3c_fairness_rank_hexbin.png")
    plt.savefig(outfile_hex)
    plt.close()
    print(f"   ✅ Grafico Ranghi salvato: {outfile_hex}")

    # =========================================================================
    # 4. REPORT FINALE
    # =========================================================================
    report = f"""
    === REPORT STEP 3: FAIRNESS E DOMINANZA (DATASET CORRETTO) ===
    
    1. ANALISI CORRELAZIONE (EFFICIENZA DI MERCATO)
       Utenti analizzati: {len(df_active)}
       (Filtrati: Solo utenti con > {MIN_POSTS} post originali)
       
       - Kendall Tau: {tau:.4f}
         * Se vicino a 1.0: Il sistema è meritocratico.
         * Se vicino a 0.0: Il successo è scollegato dalla qualità.
       
       - Pearson (Log-Log): {pearson_corr:.4f}

    2. ANALISI TOP-RANK (ELITE)
       Quanto spesso i "Migliori" (per qualità dei post originali)
       sono anche i "Più Famosi"?
       
       - Top 100 Overlap: {overlap_results[1][1] if len(overlap_results)>1 else 'N/A'}%
       - Top 1000 Overlap: {overlap_results[2][1] if len(overlap_results)>2 else 'N/A'}%
       
    3. INTERPRETAZIONE POST-MODIFICA
       Aver rimosso i repost rende questo dato molto più solido.
       Se Tau è basso, non è colpa dello spam, ma è proprio una caratteristica
       strutturale della piattaforma: la fama non segue il merito creativo.
    """
    
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
        
    print(report)
    print("✅ STEP 3 COMPLETATO.")

if __name__ == "__main__":
    main()