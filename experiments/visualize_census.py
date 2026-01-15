# .../experiments/visualize_census.py
"""
Description: 
    Loads the census data (bluesky_feed_census.csv) and generates statistical 
    visualizations for the thesis:
    1. Distribution of Feed Likes (The Long Tail).
    2. Scatter Plot: Creator Followers vs. Feed Likes (Correlation).
    3. Temporal Analysis: Feed creation over time.
    
    Output: Saves charts in 'results/plots/'
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

# --- CONFIGURATION ---
INPUT_CSV = "results/feed_stats/bluesky_feed_census.csv"
OUTPUT_DIR = "results/plots/"
# ---------------------

def main():
    print("--- GENERATING THESIS CHARTS ---")
    
    # 1. Load Data
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Error: File {INPUT_CSV} not found.")
        print("Please wait for 'make_feed_census.py' to finish first.")
        return

    df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(df)} feeds.")
    
    # Convert dates to datetime objects
    df['creation_date'] = pd.to_datetime(df['creation_date'])
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Set visual style
    sns.set_theme(style="whitegrid")

    # --- CHART 1: THE LONG TAIL (Distribution of Likes) ---
    print("Generating Chart 1: Like Distribution (Long Tail)...")
    plt.figure(figsize=(10, 6))
    
    # We use a Log Scale because the difference between top and bottom is huge
    sns.histplot(df['feed_likes'], bins=50, log_scale=True, color="#0085ff")
    
    plt.title("Distribution of Feed Popularity (Log Scale)", fontsize=14)
    plt.xlabel("Number of Likes (Log Scale)", fontsize=12)
    plt.ylabel("Count of Feeds", fontsize=12)
    plt.axvline(df['feed_likes'].mean(), color='red', linestyle='--', label=f"Mean Likes ({df['feed_likes'].mean():.1f})")
    plt.legend()
    
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}01_feed_likes_distribution.png", dpi=300)
    plt.close()

    # --- CHART 2: CORRELATION (Followers vs Likes) ---
    print("Generating Chart 2: Correlation Scatter Plot...")
    plt.figure(figsize=(10, 6))
    
    # Filter out extreme outliers for better visualization if needed, 
    # but for now we plot all on log scales
    plt.xscale('log')
    plt.yscale('log')
    
    # Scatter plot with some transparency (alpha) to see density
    sns.scatterplot(data=df, x='creator_followers', y='feed_likes', alpha=0.5, color="purple")
    
    plt.title("Correlation: Creator Influence vs. Feed Success", fontsize=14)
    plt.xlabel("Creator Followers (Log Scale)", fontsize=12)
    plt.ylabel("Feed Likes (Log Scale)", fontsize=12)
    
    # Calculate Correlation Coefficient (Spearman for non-linear/ranked data)
    corr = df[['creator_followers', 'feed_likes']].corr(method='spearman').iloc[0, 1]
    plt.figtext(0.15, 0.8, f"Spearman Correlation: {corr:.2f}", fontsize=12, bbox=dict(facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}02_followers_vs_likes_correlation.png", dpi=300)
    plt.close()

    # --- CHART 3: TIME SERIES (Feeds created per Month) ---
    print("Generating Chart 3: Creation Timeline...")
    plt.figure(figsize=(12, 6))
    
    # Group by Month
    per_month = df.groupby(df['creation_date'].dt.to_period("M")).size()
    per_month.index = per_month.index.astype(str) # Convert for plotting
    
    sns.lineplot(x=per_month.index, y=per_month.values, marker="o", color="green", linewidth=2)
    
    plt.title("Evolution of Feed Creation on Bluesky", fontsize=14)
    plt.xlabel("Month", fontsize=12)
    plt.ylabel("New Feeds Created", fontsize=12)
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}03_creation_timeline.png", dpi=300)
    plt.close()

    print(f"\n✅ SUCCESS! Charts saved in: {OUTPUT_DIR}")
    print("1. 01_feed_likes_distribution.png (Shows the Long Tail)")
    print("2. 02_followers_vs_likes_correlation.png (Does fame matter?)")
    print("3. 03_creation_timeline.png (Growth over time)")

if __name__ == "__main__":
    main()