# ==============================================================================
# PROJECT: Bluesky Feeds Topic Modeling Analysis (Master's Thesis)
# DESCRIPTION: Qualitative and Quantitative Analysis of Bluesky Feeds
# FEATURES: GPU acceleration, Metadata matching (Feed Names), 3 Representative Docs
# ==============================================================================

import pandas as pd
import os
import torch
from datetime import datetime
from umap import UMAP
from bertopic import BERTopic

# ------------------------------------------------------------------------------
# --- 0. CONFIGURATION ---
# ------------------------------------------------------------------------------
INPUT_CSV = "bluesky_feed_census_v2_with_lang.csv"  # Ensure this file is in your folder
OUTPUT_DIR = "results_topic_modeling"
REPORT_FILE = "topic_qualitative_report.txt"
NUM_TOPICS_TO_REPORT = 10

# ------------------------------------------------------------------------------
# --- 1. WORKSPACE SETUP ---
# ------------------------------------------------------------------------------
def setup_workspace():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    print(f"📂 Workspace initialized: {OUTPUT_DIR}")

# ------------------------------------------------------------------------------
# --- 2. DATA LOADING & FILTERING ---
# ------------------------------------------------------------------------------
def load_and_preprocess():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ Error: {INPUT_CSV} not found.")
        return None, None

    print("🔄 Loading and filtering dataset...")
    df = pd.read_csv(INPUT_CSV)
    
    # Date processing (UTC)
    df['creation_date'] = pd.to_datetime(df['creation_date'], utc=True, errors='coerce')
    cutoff = pd.Timestamp("2022-12-31").tz_localize("UTC")

    # Apply thesis filters
    mask = (
        (df['language'] == 'en') & 
        (df['description'].notna()) & 
        (df['creation_date'] > cutoff)
    )
    df_clean = df[mask].copy()
    
    # Prepare documents for BERTopic
    docs = df_clean['description'].astype(str).str.strip().tolist()
    
    print(f"✅ Data ready: {len(docs)} documents selected.")
    return docs, df_clean

# ------------------------------------------------------------------------------
# --- 3. TOPIC MODELING ---
# ------------------------------------------------------------------------------
def run_bertopic(docs):
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"🤖 Training model on {device.upper()}...")
    
    # Fixed random state for reproducibility in thesis results
    umap_model = UMAP(random_state=42)
    
    model = BERTopic(
        umap_model=umap_model,
        language="english",
        calculate_probabilities=False,
        verbose=True
    )

    topics, _ = model.fit_transform(docs)
    return model

# ------------------------------------------------------------------------------
# --- 4. DETAILED QUALITATIVE REPORT ---
# ------------------------------------------------------------------------------
def generate_qualitative_report(model, df_clean):
    print("📄 Generating qualitative report with Feed Names...")
    
    report_path = os.path.join(OUTPUT_DIR, REPORT_FILE)
    
    # Pre-clean descriptions in the dataframe for accurate matching
    df_lookup = df_clean.copy()
    df_lookup['desc_clean'] = df_lookup['description'].astype(str).str.strip()

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("=== BLUESKY TOPIC MODELING: QUALITATIVE ANALYSIS ===\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write("Method: 3 standard representative documents per topic\n")
        f.write("="*60 + "\n\n")

        topic_info = model.get_topic_info()
        # Filter out outlier topic (-1)
        top_topics = topic_info[topic_info['Topic'] != -1].head(NUM_TOPICS_TO_REPORT)

        for _, row in top_topics.iterrows():
            tid = row['Topic']
            keywords = ", ".join(row['Representation'][:5])
            
            f.write(f"TOPIC ID: {tid}\n")
            f.write(f"KEYWORDS: {keywords}\n")
            f.write(f"SIZE:     {row['Count']} feeds\n")
            f.write("-" * 30 + "\n")

            # Get the 3 representative texts identified by the model
            rep_texts = model.get_representative_docs(tid)

            for i, text in enumerate(rep_texts[:3]):
                # Find matching row in original dataframe to get the Name
                match = df_lookup[df_lookup['desc_clean'] == text.strip()].head(1)
                
                if not match.empty:
                    name = match.iloc[0].get('display_name', match.iloc[0].get('name', 'N/A'))
                    f.write(f"  {i+1}. FEED NAME: {name}\n")
                    f.write(f"     DESCRIPTION: {text.replace('\n', ' ').strip()}\n\n")
                else:
                    f.write(f"  {i+1}. DESCRIPTION: {text[:200]}... (Metadata missing)\n\n")
            
            f.write("="*60 + "\n\n")

    print(f"✅ Qualitative report saved: {REPORT_FILE}")

# ------------------------------------------------------------------------------
# --- 5. QUANTITATIVE EXPORTS ---
# ------------------------------------------------------------------------------
def export_visuals(model):
    print("💾 Saving visualizations and CSV data...")
    model.get_topic_info().to_csv(os.path.join(OUTPUT_DIR, "topics_summary.csv"), index=False)
    
    try:
        model.visualize_topics().write_html(os.path.join(OUTPUT_DIR, "intertopic_map.html"))
        model.visualize_barchart(top_n_topics=15).write_html(os.path.join(OUTPUT_DIR, "topic_barchart.html"))
    except Exception as e:
        print(f"⚠️ Visualization error: {e}")

# ------------------------------------------------------------------------------
# --- 6. MAIN EXECUTION ---
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    setup_workspace()
    
    # Step 1: Prep
    docs, df_clean = load_and_preprocess()
    
    if docs:
        # Step 2: Analysis
        topic_model = run_bertopic(docs)
        
        # Step 3: Reports & Visuals
        generate_qualitative_report(topic_model, df_clean)
        export_visuals(topic_model)
        
        print("\n" + "!"*40)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print(f"Find all results in: {os.path.abspath(OUTPUT_DIR)}")
        print("!"*40)