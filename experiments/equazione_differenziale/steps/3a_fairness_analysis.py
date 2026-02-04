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
    print("--- STEP 3: ANALISI FAIRNESS E DOMINANZA ---")

    # 1. Caricamento e Merge dei Dati
    if not os.path.exists(INPUT_METRICS) or not os.path.exists(INPUT_PARAMS):
        print("❌ Errore: Mancano i file di input (Esegui Step 1 e 2).")
        sys.exit(1)

    df_metrics = pd.read_csv(INPUT_METRICS) # Contiene mean_X (Popolarità)
    df_params = pd.read_csv(INPUT_PARAMS)   # Contiene beta_merit (Qualità)

    # Uniamo i dataset usando il 'did' (User ID)
    df = pd.merge(df_metrics, df_params, on='did', how='inner')
    
    print(f"👥 Utenti analizzati (con dati completi): {len(df)}")
    
    # Filtro: Consideriamo solo utenti "stabili" (es. con almeno 5 post e popolarità minima)
    # per evitare che utenti con 1 post falsino la correlazione
    df_active = df[(df['post_count'] >= 5) & (df['mean_X'] > 1.0)].copy()
    print(f"👥 Utenti attivi considerati per la Fairness: {len(df_active)}")

    os.makedirs(OUTPUT_DIR_IMGS, exist_ok=True)

    # =========================================================================
    # 1. CORRELAZIONE TRA QUALITÀ (Beta) E POPOLARITÀ (X)
    # =========================================================================
    print("\n⚖️  1. Calcolo Correlazioni (Meritocrazia)...")
    
    # Usiamo i logaritmi perché le grandezze variano su ordini di grandezza
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

    # Grafico Scatter: Qualità vs Successo
    plt.figure(figsize=(8, 8))
    plt.scatter(df_active['beta_merit'], df_active['mean_X'], alpha=0.3, s=10, c='teal')
    plt.xscale('log'); plt.yscale('log')
    plt.xlabel(r"Qualità Intrinseca $\beta_{merit}$")
    plt.ylabel(r"Popolarità Raggiunta $\langle X \rangle$")
    plt.title(f"Efficiency of the Market\nKendall Tau = {tau:.2f} (1.0 = Perfetta Meritocrazia)")
    
    # Aggiungiamo linea di tendenza
    m, q = np.polyfit(log_beta, log_X, 1)
    x_fit = np.linspace(log_beta.min(), log_beta.max(), 100)
    plt.plot(np.exp(x_fit), np.exp(m*x_fit + q), 'r--', label='Trend')
    
    plt.legend()
    plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "3a_fairness_scatter.png"))
    plt.close()

    # =========================================================================
    # 2. ANALISI TOP-K OVERLAP (Dominanza)
    # =========================================================================
    print("\n🏆 2. Analisi Top-K Overlap...")
    # Confrontiamo la Top 100 degli utenti "Bravi" con la Top 100 dei "Famosi"
    
    k_values = [10, 100, 1000, 5000]
    overlap_results = []

    # Creiamo i ranking
    # Rank per Popolarità (chi ha X più alto è 1°)
    df_active['rank_X'] = df_active['mean_X'].rank(ascending=False)
    # Rank per Qualità (chi ha Beta più alto è 1°)
    df_active['rank_Beta'] = df_active['beta_merit'].rank(ascending=False)

    for k in k_values:
        if k > len(df_active): break
        
        top_k_famous = set(df_active[df_active['rank_X'] <= k]['did'])
        top_k_best = set(df_active[df_active['rank_Beta'] <= k]['did'])
        
        # Intersezione: Quanti sono SIA bravi CHE famosi?
        overlap = len(top_k_famous.intersection(top_k_best))
        perc = (overlap / k) * 100
        overlap_results.append((k, perc))
        print(f"   Top {k}: {perc:.1f}% di sovrapposizione")

    # Grafico Overlap
    if overlap_results:
        ks, percs = zip(*overlap_results)
        plt.figure(figsize=(8, 6))
        plt.plot(ks, percs, marker='o', linestyle='-', color='purple', linewidth=2)
        plt.xscale('log')
        
        # --- MODIFICA RICHIESTA: Range Asse Y da 0 a 50% ---
        plt.ylim(0, 50) 
        
        plt.xlabel("Top K Utenti (Log Scale)")
        plt.ylabel("% Sovrapposizione (Bravi & Famosi)")
        plt.title("Allineamento tra Qualità e Popolarità nei Top Utenti")
        plt.grid(True, which="both", ls="--", alpha=0.5)
        plt.savefig(os.path.join(OUTPUT_DIR_IMGS, "3b_topk_overlap.png"))
        plt.close()

    # =========================================================================
    # 3. REPORT FINALE
    # =========================================================================
    report = f"""
    === REPORT STEP 3: FAIRNESS E DOMINANZA ===
    
    1. ANALISI CORRELAZIONE (EFFICIENZA DI MERCATO)
       Utenti analizzati: {len(df_active)}
       
       - Kendall Tau: {tau:.4f}
         * Se vicino a 1.0: Il sistema è meritocratico (i migliori emergono sempre).
         * Se vicino a 0.0: Il successo è casuale o dominato solo dal 'Rich-get-Richer'.
         * Confronto paper (es. Facebook): Di solito è intorno a 0.4 - 0.6.
       
       - Pearson Correlation (Log-Log): {pearson_corr:.4f}
         Indica quanto la popolarità scala con la qualità.

    2. ANALISI TOP-RANK (ELITE)
       Quanto spesso i "Migliori" sono anche i "Più Famosi"?
       
       - Top 100: {overlap_results[1][1]:.1f}% di sovrapposizione.
       - Top 1000: {overlap_results[2][1]:.1f}% di sovrapposizione.
       
       Interpretazione:
       Se questa percentuale è bassa (es. < 20%), significa che l'élite della piattaforma 
       non è composta dai creatori di contenuti migliori, ma da chi è arrivato prima.
    """
    
    with open(OUTPUT_REPORT, "w") as f:
        f.write(report)
        
    print(report)
    print("✅ STEP 3 COMPLETATO.")

if __name__ == "__main__":
    main()