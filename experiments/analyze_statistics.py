# .../experiments/analyze_statistics.py

"""
Description:
    COMPLETE THESIS ANALYSIS SUITE.
    Generates 9 Figures covering Descriptive, Statistical, and Macro analysis.
    Prints a rigorous statistical report to the terminal.
    
    OUTPUTS:
    - Fig 1: Active vs Silent (Pie)
    - Fig 2: Popularity Tiers (Bar)
    - Fig 3: Top 20 Feeds (Horizontal Bar)
    - Fig 4: Zipf's Law (Log-Log)
    - Fig 5: Lorenz Curve (Inequality)
    - Fig 6: Correlation (Followers vs Likes)
    - Fig 7: Growth Timeline (Line)
    - Fig 8: Top Keywords (Bar)
    - Fig 9: Creator Productivity (Bar)
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy import stats
from collections import Counter
import os
import re

# --- CONFIGURATION ---
INPUT_CSV = "results/feed_stats/bluesky_feed_census_hybrid.csv"
OUTPUT_DIR = "results/plots/"

# Keywords to ignore in analysis
STOPWORDS = {
    'feed', 'bluesky', 'the', 'for', 'and', 'my', 'of', 'in', 'a', 'to', 
    'test', 'custom', 'new', 'posts', 'with', 'from', 'on', 'bot', 'all',
    'feeds', 'app', 'bsky', 'generator'
}

# --- HELPER FUNCTIONS ---

def clean_text(text):
    """Removes emojis and non-ASCII chars for clean plotting."""
    if not isinstance(text, str): return str(text)
    return re.sub(r'[^\x00-\x7F]+', '', text).strip()

def clean_text_for_keywords(text):
    """Prepares text for keyword frequency analysis."""
    if not isinstance(text, str): return ""
    return re.sub(r'[^a-zA-Z\s]', '', text).lower()

def gini_coefficient(x):
    """Calculates Gini coefficient (0 = Equality, 1 = Inequality)."""
    x = np.array(x, dtype=np.float64)
    if np.amin(x) < 0: x -= np.amin(x)
    x = np.sort(x)
    index = np.arange(1, x.shape[0] + 1)
    n = x.shape[0]
    return ((np.sum((2 * index - n  - 1) * x)) / (n * np.sum(x)))

def print_terminal_report(df):
    """Prints rigorous statistics to the terminal."""
    likes = df['feed_likes']
    print("\n" + "="*40)
    print("      STATISTICAL REPORT (LIKES)")
    print("="*40)
    print(f"Count:      {len(likes)}")
    print(f"Mean:       {likes.mean():.4f}")
    print(f"Median:     {likes.median():.4f}")
    print(f"Mode:       {likes.mode()[0]:.4f}")
    print("-" * 20)
    print(f"Std Dev:    {likes.std():.4f}")
    print(f"Variance:   {likes.var():.4f}")
    print(f"Max Like:   {likes.max():.4f}")
    print("-" * 20)
    print(f"Skewness:   {likes.skew():.4f} (>1 = Long Tail)")
    print(f"Kurtosis:   {likes.kurt():.4f}")
    print("-" * 20)
    print("Quantiles:")
    print(f"  25% (Q1): {likes.quantile(0.25):.2f}")
    print(f"  75% (Q3): {likes.quantile(0.75):.2f}")
    print(f"  90%:      {likes.quantile(0.90):.2f}")
    print(f"  99%:      {likes.quantile(0.99):.2f} (Top 1%)")
    print("-" * 20)
    gini = gini_coefficient(likes.values)
    print(f"Gini Coeff: {gini:.4f}")
    print("="*40 + "\n")

# --- MAIN EXECUTION ---

def main():
    print("--- FULL THESIS ANALYSIS STARTED ---")
    
    # 1. LOAD DATA
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Error: {INPUT_CSV} not found.")
        return
    df = pd.read_csv(INPUT_CSV)
    print(f"Dataset loaded: {len(df)} records.")
    
    # 2. PRE-PROCESSING
    df['clean_name'] = df['name'].apply(clean_text)
    # Ensure creator_did exists (extract from URI if missing)
    if 'creator_did' not in df.columns:
        df['creator_did'] = df['uri'].apply(lambda x: x.split('/')[2])

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    sns.set_theme(style="whitegrid")

    # 3. PRINT STATS
    print_terminal_report(df)

    # ==========================================
    # PART A: DESCRIPTIVE PLOTS
    # ==========================================

    print("Generating Fig 1: Active vs Silent (Pie)...")
    zero = df[df['feed_likes'] == 0].shape[0]
    active = df[df['feed_likes'] > 0].shape[0]
    plt.figure(figsize=(7, 7))
    plt.pie([zero, active], labels=['Silent (0)', 'Active (>0)'], autopct='%1.1f%%', 
            colors=['#ff9999','#66b3ff'], startangle=90, explode=(0.05, 0))
    plt.title('Ratio of Silent vs Active Feeds', fontsize=14)
    plt.savefig(f"{OUTPUT_DIR}Fig1_Silent_Ratio.png", dpi=300); plt.close()

    print("Generating Fig 2: Popularity Tiers (Bar)...")
    bins = [-1, 0, 10, 100, 1000, 10000, float('inf')]
    labels = ['Zero (0)', 'Nano (1-10)', 'Micro (11-100)', 'Mid (101-1k)', 'Macro (1k-10k)', 'Viral (10k+)']
    df['tier'] = pd.cut(df['feed_likes'], bins=bins, labels=labels)
    plt.figure(figsize=(10, 6))
    ax = sns.countplot(data=df, x='tier', hue='tier', palette='Blues_d', legend=False)
    plt.title('Feed Distribution by Popularity Tier', fontsize=14)
    for p in ax.patches:
        h = int(p.get_height())
        if h > 0: ax.annotate(f'{h}', (p.get_x()+p.get_width()/2., h), ha='center', va='bottom')
    plt.savefig(f"{OUTPUT_DIR}Fig2_Popularity_Tiers.png", dpi=300); plt.close()

    print("Generating Fig 3: Top 20 Feeds (Detailed)...")
    # 1. Prendiamo i top 20 e usiamo .copy() per evitare warning
    top20 = df.sort_values('feed_likes', ascending=False).head(20).copy()
    
    # 2. Creiamo l'etichetta univoca: "Nome Feed (@handle_creatore)"
    # Questo distingue i feed omonimi (es. i vari "For You") ed elimina le barre di errore
    top20['unique_label'] = top20['clean_name'] + " (" + top20['creator_handle'] + ")"
    
    plt.figure(figsize=(12, 10)) # Altezza aumentata per leggere meglio i nomi
    sns.barplot(data=top20, x='feed_likes', y='unique_label', hue='unique_label', palette='viridis', legend=False)
    
    plt.title('Top 20 Feeds by Likes (with Creator Source)', fontsize=14)
    plt.xlabel('Likes'); plt.ylabel('')
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}Fig3_Top20_Ranking.png", dpi=300); plt.close()

    # ==========================================
    # PART B: STATISTICAL PLOTS
    # ==========================================

    print("Generating Fig 4: Zipf's Law (Log-Log)...")
    df_nz = df[df['feed_likes'] > 0].sort_values('feed_likes', ascending=False)
    ranks = np.arange(1, len(df_nz) + 1)
    plt.figure(figsize=(8, 6))
    plt.loglog(ranks, df_nz['feed_likes'], marker='.', linestyle='none', alpha=0.3, color='blue')
    plt.title("Zipf's Law: Rank-Size Distribution", fontsize=14)
    plt.xlabel("Rank (Log)"); plt.ylabel("Likes (Log)")
    plt.grid(True, which="both", ls="--", alpha=0.2)
    plt.savefig(f"{OUTPUT_DIR}Fig4_PowerLaw_Zipf.png", dpi=300); plt.close()

    print("Generating Fig 5: Lorenz Curve (Inequality)...")
    gini = gini_coefficient(df['feed_likes'].values)
    sorted_likes = np.sort(df['feed_likes'].values)
    y_lor = np.cumsum(sorted_likes) / np.sum(sorted_likes)
    y_lor = np.insert(y_lor, 0, 0)
    x_lor = np.linspace(0, 1, len(y_lor))
    plt.figure(figsize=(8, 8))
    plt.plot(x_lor, y_lor, color='red', linewidth=2, label=f'Gini={gini:.3f}')
    plt.plot([0, 1], [0, 1], color='gray', linestyle='--', label='Equality')
    plt.title(f'Lorenz Curve (Gini: {gini:.3f})', fontsize=14)
    plt.legend(); plt.grid(alpha=0.3)
    plt.savefig(f"{OUTPUT_DIR}Fig5_Lorenz_Gini.png", dpi=300); plt.close()

    print("Generating Fig 6: Correlation Scatter...")
    if df['creator_followers'].sum() == 0:
        print("   ⚠️ Skipping Correlation (Followers data missing). Run Enrichment first.")
    else:
        corr, p = stats.spearmanr(df['creator_followers'], df['feed_likes'])
        plt.figure(figsize=(9, 6))
        plt.xscale('symlog'); plt.yscale('symlog')
        sns.scatterplot(data=df, x='creator_followers', y='feed_likes', alpha=0.4)
        plt.title(f'Creator Capital vs Feed Success (Rho={corr:.2f})', fontsize=14)
        plt.xlabel('Followers (Log)'); plt.ylabel('Likes (Log)')
        plt.grid(True, alpha=0.3)
        plt.savefig(f"{OUTPUT_DIR}Fig6_Correlation.png", dpi=300); plt.close()

    # ==========================================
    # PART C: MACRO / EXTRA PLOTS
    # ==========================================

    print("Generating Fig 7: Growth Timeline...")
    try:
        # 1. Convert dates
        df['created_dt'] = pd.to_datetime(df['creation_date'], errors='coerce')
        
        # 2. FILTER: Keep only feeds from 2023 onwards (exclude 1970 artifacts)
        valid_feeds = df[df['created_dt'].dt.year >= 2023]
        excluded_count = len(df) - len(valid_feeds)
        
        # 3. Prepare data (Group by Month)
        # .dt.tz_localize(None) removes timezone info to avoid warnings
        timeline = valid_feeds['created_dt'].dt.tz_localize(None).dt.to_period('M').value_counts().sort_index()
        timeline.index = timeline.index.astype(str)
        
        # 4. Plot
        plt.figure(figsize=(12, 7)) # Increased height for the footnote
        sns.lineplot(x=timeline.index, y=timeline.values, marker='o', linewidth=2.5, color='#007acc')
        plt.fill_between(timeline.index, timeline.values, color='#007acc', alpha=0.1)
        
        plt.title('Monthly Feed Creation Growth', fontsize=14)
        plt.xticks(rotation=45)
        plt.grid(True, linestyle='--', alpha=0.5)
        
        # 5. Add Footnote if data was excluded
        if excluded_count > 0:
            note_text = f"* Note: {excluded_count} feeds were excluded due to invalid dates (e.g., Unix Epoch 1970 artifacts)."
            plt.figtext(0.5, 0.02, note_text, wrap=True, horizontalalignment='center', fontsize=10, style='italic', color='gray')
            plt.subplots_adjust(bottom=0.15) # Make room for text
        else:
            plt.tight_layout()
            
        plt.savefig(f"{OUTPUT_DIR}Fig7_Growth_Timeline.png", dpi=300); plt.close()
        
    except Exception as e:
        print(f"   ⚠️ Timeline error: {e}")

    print("Generating Fig 8: Top Keywords...")
    all_txt = " ".join(df['name'].apply(clean_text_for_keywords))
    words = [w for w in all_txt.split() if w not in STOPWORDS and len(w) > 2]
    common = pd.DataFrame(Counter(words).most_common(15), columns=['word', 'count'])
    plt.figure(figsize=(10, 8))
    sns.barplot(data=common, y='word', x='count', palette='magma', hue='word', legend=False)
    plt.title('Top 15 Keywords in Feed Names', fontsize=14)
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}Fig8_Top_Keywords.png", dpi=300); plt.close()

    print("Generating Fig 9: Creator Productivity...")
    feeds_per_creator = df.groupby('creator_did').size()
    def categorize(n):
        if n == 1: return '1 Feed'
        if n == 2: return '2 Feeds'
        if n <= 5: return '3-5 Feeds'
        return '6+ (Power User)'
    cats = feeds_per_creator.apply(categorize).value_counts()
    order = ['1 Feed', '2 Feeds', '3-5 Feeds', '6+ (Power User)']
    cats = cats.reindex(order).dropna()
    
    plt.figure(figsize=(8, 6))
    ax = sns.barplot(x=cats.index, y=cats.values, palette='viridis', hue=cats.index, legend=False)
    plt.title('Creator Productivity', fontsize=14)
    total = len(feeds_per_creator)
    for p in ax.patches:
        h = p.get_height()
        pct = (h / total) * 100
        ax.annotate(f'{pct:.1f}%', (p.get_x()+p.get_width()/2., h), ha='center', va='bottom', fontweight='bold')
    plt.savefig(f"{OUTPUT_DIR}Fig9_Creator_Productivity.png", dpi=300); plt.close()

    print(f"\n✅ SUCCESS! All 9 plots saved in: {OUTPUT_DIR}")
    
if __name__ == "__main__":
    main()