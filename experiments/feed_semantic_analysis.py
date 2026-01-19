# .../experiments/feed_semantic_analysis.py

import pandas as pd
import os
import torch
from datetime import datetime
from umap import UMAP
from bertopic import BERTopic

# ------------------------------------------------------------------------------
# --- 0. CONFIGURATION ---
# ------------------------------------------------------------------------------
INPUT_CSV = "../data_collection/results/feed_stats/bluesky_feed_census_v2_with_lang.csv" 
OUTPUT_DIR = "results/results_bert"
NUM_TOPICS_TO_REPORT = 10
LIKE_COL = 'feed_likes'
LOG_FILE = "filtering_log.txt"

# ------------------------------------------------------------------------------
# --- 1. WORKSPACE SETUP ---
# ------------------------------------------------------------------------------
def setup_workspace():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    print(f"📂 Workspace initialized: {OUTPUT_DIR}")

# ------------------------------------------------------------------------------
# --- 2. DATA LOADING & FILTERING (With Methodology Log) ---
# ------------------------------------------------------------------------------
def load_and_preprocess():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Error: {INPUT_CSV} not found.")
        return None, None

    print("🔄 Loading and filtering dataset...")
    df = pd.read_csv(INPUT_CSV)
    
    # Conteggi per la metodologia
    initial_count = len(df)
    
    # 1. Filtro Lingua
    lang_mask = (df['language'] == 'en')
    after_lang = len(df[lang_mask])
    
    # 2. Filtro Descrizioni Mancanti
    desc_mask = (df['description'].notna()) & (df['description'].str.strip() != "")
    after_desc = len(df[lang_mask & desc_mask])
    
    # 3. Filtro Data (Post-2022)
    df['creation_date'] = pd.to_datetime(df['creation_date'], utc=True, errors='coerce')
    cutoff = pd.Timestamp("2022-12-31").tz_localize("UTC")
    date_mask = (df['creation_date'] > cutoff)
    
    df_clean = df[lang_mask & desc_mask & date_mask].copy()
    final_count = len(df_clean)

    # Scrittura del LOG METODOLOGICO
    log_path = os.path.join(OUTPUT_DIR, LOG_FILE)
    with open(log_path, "w", encoding="utf-8") as f:
        f.write("=== METHODOLOGY LOG: DATA FILTERING ===\n")
        f.write(f"Date of analysis: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
        f.write(f"1. Initial dataset size: {initial_count} feeds\n")
        f.write(f"2. After Language Filter (English): {after_lang} feeds (Dropped: {initial_count - after_lang})\n")
        f.write(f"3. After Null Description Filter: {after_desc} feeds (Dropped: {after_lang - after_desc})\n")
        f.write(f"4. After Date Filter (> 2022-12-31): {final_count} feeds (Dropped: {after_desc - final_count})\n")
        f.write(f"\nFINAL DATASET FOR TOPIC MODELING: {final_count} feeds\n")
        f.write("="*40 + "\n")

    docs = df_clean['description'].astype(str).str.strip().tolist()
    print(f"✅ Data ready: {final_count} documents. Log saved in {LOG_FILE}")
    return docs, df_clean

# ------------------------------------------------------------------------------
# --- 3. TOPIC MODELING ---
# ------------------------------------------------------------------------------
def run_bertopic(docs):
    print(f"🤖 Training model (GPU check: {torch.cuda.is_available()})...")
    umap_model = UMAP(random_state=42)
    model = BERTopic(umap_model=umap_model, language="english", calculate_probabilities=False, verbose=True)
    model.fit_transform(docs)
    return model

# ------------------------------------------------------------------------------
# --- 4. DATA CONSOLIDATION ---
# ------------------------------------------------------------------------------
def get_enriched_topic_info(model, df_clean, docs):
    print("📊 Consolidating stats into topics_summary.csv...")
    topic_info = model.get_topic_info()
    doc_info = model.get_document_info(docs)
    df_temp = df_clean.reset_index(drop=True).copy()
    df_temp['Topic'] = doc_info['Topic'].values
    
    engagement = df_temp.groupby('Topic')[LIKE_COL].agg(['sum', 'mean']).reset_index()
    engagement.columns = ['Topic', 'Total_Likes', 'Average_Likes']
    
    enriched_info = pd.merge(topic_info, engagement, on='Topic', how='left')
    enriched_info['Average_Likes'] = enriched_info['Average_Likes'].round(2)
    enriched_info.to_csv(os.path.join(OUTPUT_DIR, "topics_summary.csv"), index=False)
    return enriched_info

# ------------------------------------------------------------------------------
# --- 5. DUAL QUALITATIVE REPORTS ---
# ------------------------------------------------------------------------------
def generate_reports(model, df_clean, enriched_info):
    df_lookup = df_clean.copy()
    df_lookup['desc_clean'] = df_lookup['description'].astype(str).str.strip()

    report_types = [
        ("topic_report_by_size.txt", "Count"),
        ("topic_report_by_engagement.txt", "Total_Likes")
    ]

    for filename, sort_col in report_types:
        print(f"📄 Generating: {filename}")
        path = os.path.join(OUTPUT_DIR, filename)
        top_data = enriched_info[enriched_info['Topic'] != -1].sort_values(by=sort_col, ascending=False).head(NUM_TOPICS_TO_REPORT)

        with open(path, "w", encoding="utf-8") as f:
            f.write(f"=== BLUESKY TOPIC ANALYSIS: ORDERED BY {sort_col.upper()} ===\n\n")
            for _, row in top_data.iterrows():
                tid = row['Topic']
                f.write(f"TOPIC ID: {tid} | SIZE: {row['Count']} | ENGAGEMENT: {row['Total_Likes']} (Avg: {row['Average_Likes']})\n")
                f.write(f"KEYWORDS: {row['Representation']}\n")
                f.write("-" * 30 + "\n")
                rep_texts = model.get_representative_docs(tid)
                for i, text in enumerate(rep_texts[:3]):
                    match = df_lookup[df_lookup['desc_clean'] == text.strip()].head(1)
                    if not match.empty:
                        name = match.iloc[0].get('display_name', match.iloc[0].get('name', 'N/A'))
                        f.write(f"  {i+1}. {name}: {text[:200].replace('\n', ' ')}...\n")
                f.write("="*60 + "\n\n")

# ------------------------------------------------------------------------------
# --- 6. MAIN ---
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    setup_workspace()
    docs, df_clean = load_and_preprocess()
    
    if docs:
        topic_model = run_bertopic(docs)
        enriched_info = get_enriched_topic_info(topic_model, df_clean, docs)
        generate_reports(topic_model, df_clean, enriched_info)
        
        try:
            topic_model.visualize_barchart(top_n_topics=15).write_html(os.path.join(OUTPUT_DIR, "topic_barchart.html"))
        except: pass
            
        print(f"\n✅ All done! Results in: {OUTPUT_DIR}")