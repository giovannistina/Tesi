import os
import sys
import gzip
import json
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime

# --- CONFIGURATION ---
RESULTS_DIR = 'results'
PLOTS_DIR = 'results/plots'
FEED_DATA_DIR = '../data_collection/feed_likes_data'
# ---------------------

def setup_plotting():
    """Sets up the visual style for the plots."""
    print("-> Initializing graphics engine (Seaborn/Matplotlib)...")
    sns.set_theme(style="whitegrid")
    plt.rcParams.update({'figure.figsize': (12, 7)})
    
    if not os.path.exists(PLOTS_DIR):
        os.makedirs(PLOTS_DIR)
        print(f"-> Created directory: {PLOTS_DIR}")

def load_data_frame(filename, names=None, sep=' '):
    """Helper to load compressed CSV/TXT files safely."""
    path = os.path.join(RESULTS_DIR, filename)
    if not os.path.exists(path):
        print(f"   [SKIP] File {filename} not found.")
        return None
    try:
        if filename.endswith('.gz'):
            return pd.read_csv(path, compression='gzip', sep=sep, names=names, encoding='utf-8')
        else:
            return pd.read_csv(path, sep=sep, names=names, encoding='utf-8')
    except Exception as e:
        print(f"   [ERROR] Could not load {filename}: {e}")
        return None

# --- PLOT FUNCTIONS ---

def plot_daily_activity():
    print("1. Generating 'Daily Activity' plot...", end=' ')
    df = load_data_frame('post_stats.txt.gz', names=['date', 'post_id', 'user_id'])
    if df is None or df.empty: 
        print("SKIPPED (No Data)")
        return

    daily_counts = df.groupby('date').size().reset_index(name='count')
    daily_counts['date'] = daily_counts['date'].astype(str)
    
    plt.figure()
    sns.barplot(data=daily_counts, x='date', y='count', color='royalblue')
    plt.xticks(rotation=45)
    plt.title('Daily Posts Activity')
    plt.ylabel('Number of Posts')
    plt.xlabel('Date')
    plt.tight_layout()
    outfile = os.path.join(PLOTS_DIR, 'daily_activity.png')
    plt.savefig(outfile)
    plt.close()
    print(f"DONE -> {outfile}")

def plot_languages():
    print("2. Generating 'Languages' plot...", end=' ')
    df = load_data_frame('all_langs.txt.gz', names=['lang', 'count'])
    if df is None or df.empty: 
        print("SKIPPED (No Data)")
        return

    top_langs = df.head(10)
    
    plt.figure()
    sns.barplot(data=top_langs, x='lang', y='count', palette='viridis')
    plt.title('Top 10 Languages')
    plt.yscale('log')
    plt.ylabel('Count (Log Scale)')
    plt.xlabel('Language')
    plt.tight_layout()
    outfile = os.path.join(PLOTS_DIR, 'languages_dist.png')
    plt.savefig(outfile)
    plt.close()
    print(f"DONE -> {outfile}")

def plot_sentiment_timeline():
    print("3. Generating 'Sentiment' plot...", end=' ')
    path = os.path.join(RESULTS_DIR, 'sentiment_table.csv.gz')
    if not os.path.exists(path):
        print("SKIPPED (File missing)")
        return

    try:
        df = pd.read_csv(path, compression='gzip', encoding='utf-8')
        if df.empty: 
            print("SKIPPED (Empty)")
            return
        
        df['date'] = df['date'].astype(str)
        df_melted = df.melt(id_vars=['date'], value_vars=['positive', 'negative', 'neutral'], 
                            var_name='Sentiment', value_name='Count')

        plt.figure()
        sns.lineplot(data=df_melted, x='date', y='Count', hue='Sentiment', marker='o')
        plt.xticks(rotation=45)
        plt.title('Sentiment Evolution')
        plt.tight_layout()
        outfile = os.path.join(PLOTS_DIR, 'sentiment_timeline.png')
        plt.savefig(outfile)
        plt.close()
        print(f"DONE -> {outfile}")
    except Exception as e:
        print(f"ERROR: {e}")

def plot_inter_event_time():
    print("4. Generating 'Inter-Event Time' plot...", end=' ')
    df = load_data_frame('inter-time.txt', names=['first_date', 'days'])
    if df is None or df.empty: 
        print("SKIPPED (No Data)")
        return

    x = np.sort(df['days'])
    y = np.arange(1, len(x) + 1) / len(x)

    plt.figure()
    plt.plot(x, y, marker='.', linestyle='none', color='purple')
    plt.title('User Activity Duration (ECDF)')
    plt.xlabel('Days between first and last post')
    plt.ylabel('Proportion of Users')
    plt.grid(True)
    plt.tight_layout()
    outfile = os.path.join(PLOTS_DIR, 'inter_event_ecdf.png')
    plt.savefig(outfile)
    plt.close()
    print(f"DONE -> {outfile}")

def plot_topics():
    print("5. Generating 'Topics' plot...", end=' ')
    path = os.path.join(RESULTS_DIR, 'topics_info.csv')
    if not os.path.exists(path):
        print("SKIPPED (File missing)")
        return

    try:
        df = pd.read_csv(path, encoding='utf-8')
        df = df[df['Topic'] != -1].head(10)
        
        if df.empty: 
            print("SKIPPED (Empty)")
            return

        plt.figure()
        df['Label'] = df.apply(lambda x: f"T{x['Topic']}: {x['Name'][:30]}...", axis=1)
        sns.barplot(data=df, y='Label', x='Count', color='teal')
        plt.title('Top 10 Discovered Topics')
        plt.xlabel('Number of Posts')
        plt.ylabel('Topic')
        plt.tight_layout()
        outfile = os.path.join(PLOTS_DIR, 'topics_bar.png')
        plt.savefig(outfile)
        plt.close()
        print(f"DONE -> {outfile}")
    except Exception as e:
        print(f"ERROR: {e}")

def process_feed_data():
    if not os.path.exists(FEED_DATA_DIR):
        print(f"   [WARN] Feed folder {FEED_DATA_DIR} not found.")
        return None, None

    feed_files = glob.glob(os.path.join(FEED_DATA_DIR, "*.jsonl"))
    if not feed_files:
        print(f"   [WARN] No .jsonl files in {FEED_DATA_DIR}.")
        return None, None

    print(f"   (Reading {len(feed_files)} feed files...)", end=' ')
    
    feed_totals = []
    feed_daily = []

    for fpath in feed_files:
        feed_name = os.path.basename(fpath).replace('.jsonl', '')
        count = 0
        dates = []
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        record = json.loads(line)
                        created_at = record.get('created_at')
                        if created_at:
                            dt_str = created_at.split('T')[0]
                            dates.append(dt_str)
                        count += 1
                    except: continue
        except: continue

        feed_totals.append({'feed': feed_name, 'likes': count})
        if dates:
            df_dates = pd.DataFrame({'date': dates})
            daily_counts = df_dates.groupby('date').size().reset_index(name='likes')
            daily_counts['feed'] = feed_name
            feed_daily.append(daily_counts)

    df_totals = pd.DataFrame(feed_totals).sort_values(by='likes', ascending=False)
    
    if feed_daily:
        df_daily = pd.concat(feed_daily, ignore_index=True)
    else:
        df_daily = pd.DataFrame()

    return df_totals, df_daily

def plot_feed_stats():
    print("6. Generating 'Feed Stats' plots...", end=' ')
    df_totals, df_daily = process_feed_data()
    
    if df_totals is None or df_totals.empty:
        print("SKIPPED (No Feed Data)")
        return

    # Popularity
    top_20 = df_totals.head(20)
    plt.figure(figsize=(12, 8))
    sns.barplot(data=top_20, y='feed', x='likes', palette='magma')
    plt.title('Top 20 Feeds by Likes (Sampled)')
    plt.xlabel('Total Likes')
    plt.ylabel('Feed Name')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'feeds_popularity.png'))
    plt.close()

    # Trend
    if not df_daily.empty:
        top_5_names = df_totals.head(5)['feed'].tolist()
        df_top_daily = df_daily[df_daily['feed'].isin(top_5_names)].copy()
        df_top_daily['date'] = pd.to_datetime(df_top_daily['date'])
        df_top_daily = df_top_daily.sort_values('date')

        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df_top_daily, x='date', y='likes', hue='feed', marker='o')
        plt.title('Daily Likes Trend (Top 5 Feeds)')
        plt.xlabel('Date')
        plt.ylabel('New Likes per Day')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, 'feeds_trend.png'))
        plt.close()
    
    print("DONE (2 Plots)")

if __name__ == '__main__':
    
    # Print Absolute Path to avoid confusion
    abs_plots_dir = os.path.abspath(PLOTS_DIR)
    print("="*60)
    print(f"BSKY PLOTS GENERATOR")
    print(f"Output Directory: {abs_plots_dir}")
    print("="*60)
    
    setup_plotting()
    
    try: plot_daily_activity()
    except Exception as e: print(f"FAIL: {e}")

    try: plot_languages()
    except Exception as e: print(f"FAIL: {e}")

    try: plot_sentiment_timeline()
    except Exception as e: print(f"FAIL: {e}")
    
    try: plot_inter_event_time()
    except Exception as e: print(f"FAIL: {e}")
    
    try: plot_topics()
    except Exception as e: print(f"FAIL: {e}")
    
    try: plot_feed_stats()
    except Exception as e: print(f"FAIL: {e}")

    print("\n" + "="*60)
    print("FINAL REPORT - Generated Files:")
    if os.path.exists(PLOTS_DIR):
        files = os.listdir(PLOTS_DIR)
        for f in files:
            print(f" [OK] {f}")
    print("="*60)