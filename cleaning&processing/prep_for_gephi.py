import gzip
import os
import networkx as nx

# --- CONFIGURATION ---
INPUT_FILE = 'results/interactions.csv.gz'
OUT_DIR = 'results'

# Files for Gephi Desktop (CSV with headers)
CSV_REPLIES = os.path.join(OUT_DIR, 'replies.csv')
CSV_REPOSTS = os.path.join(OUT_DIR, 'reposts.csv')
CSV_QUOTES  = os.path.join(OUT_DIR, 'quotes.csv')

# Files for Gephi Lite/Web (GEXF format)
GEXF_REPLIES = os.path.join(OUT_DIR, 'replies.gexf')
GEXF_REPOSTS = os.path.join(OUT_DIR, 'reposts.gexf')
GEXF_QUOTES  = os.path.join(OUT_DIR, 'quotes.gexf')
# ---------------------

if __name__ == '__main__':
    
    print(f"--- GEPHI FILES GENERATOR ---")
    print(f"Reading from: {INPUT_FILE}")
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Please run interactions.py first.")
        exit(1)

    # Initialize NetworkX graphs for GEXF export
    G_replies = nx.DiGraph()
    G_reposts = nx.DiGraph()
    G_quotes  = nx.DiGraph()

    print("Processing data...")

    # Open Input and CSV Outputs simultaneously
    with gzip.open(INPUT_FILE, 'rt', encoding='utf-8') as f_in, \
         open(CSV_REPLIES, 'w', encoding='utf-8') as csv_rep, \
         open(CSV_REPOSTS, 'w', encoding='utf-8') as csv_rt, \
         open(CSV_QUOTES, 'w', encoding='utf-8') as csv_qt:

        # 1. Write CSV Headers
        header = "Source,Target,Date\n"
        csv_rep.write(header)
        csv_rt.write(header)
        csv_qt.write(header)

        count = 0
        
        # 2. Process line by line
        for line in f_in:
            count += 1
            parts = line.strip().split(',')
            
            # Clean and parse fields
            # Convert 'None' strings to Python None, digits to integers
            p = [int(x) if (x and x != 'None' and x.isdigit()) else None for x in parts]
            
            if len(p) < 6: continue
            
            # Unpack: Source, ReplyTo, ThreadRoot, RepostOf, QuoteOf, Date
            u, reply, root, repost, quote, date = p
            
            # Format Date (YYYYMMDD)
            date_str = str(date)[:8] if date else "00000000"
            
            # --- HANDLE REPLIES ---
            if reply is not None:
                # Write to CSV
                csv_rep.write(f"{u},{reply},{date_str}\n")
                # Add to Graph Object (for GEXF)
                G_replies.add_edge(u, reply, date=date_str)

            # --- HANDLE REPOSTS ---
            if repost is not None:
                csv_rt.write(f"{u},{repost},{date_str}\n")
                G_reposts.add_edge(u, repost, date=date_str)

            # --- HANDLE QUOTES ---
            if quote is not None:
                csv_qt.write(f"{u},{quote},{date_str}\n")
                G_quotes.add_edge(u, quote, date=date_str)

    print(f"Processed {count} interactions.")
    
    print("\nExporting GEXF files (this might take a moment)...")
    
    # 3. Export GEXF files
    try:
        nx.write_gexf(G_replies, GEXF_REPLIES)
        print(f" -> Created {os.path.basename(GEXF_REPLIES)} ({G_replies.number_of_edges()} edges)")
        
        nx.write_gexf(G_reposts, GEXF_REPOSTS)
        print(f" -> Created {os.path.basename(GEXF_REPOSTS)} ({G_reposts.number_of_edges()} edges)")
        
        nx.write_gexf(G_quotes, GEXF_QUOTES)
        print(f" -> Created {os.path.basename(GEXF_QUOTES)} ({G_quotes.number_of_edges()} edges)")
        
    except Exception as e:
        print(f"Error saving GEXF: {e}")

    print("\nDONE! All files are in the 'results' folder.")
    print(" - Use .csv files for Gephi Desktop.")
    print(" - Use .gexf files for Gephi Lite (Web).")