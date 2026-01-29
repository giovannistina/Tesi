import pandas as pd

# Carica il file
df = pd.read_csv('results/feed_stats/bluesky_feed_census_v2_with_lang_and_bios.csv')

# 1. Volumetria
total_feeds = len(df)
unique_creators = df['creator_did'].nunique()

# 2. Tempo (assumendo che la colonna sia 'creation_date')
# Convertiamo in datetime per sicurezza
df['creation_date'] = pd.to_datetime(df['creation_date'], errors='coerce')
oldest_feed = df['creation_date'].min()
newest_feed = df['creation_date'].max()

# 3. Lingue
lang_counts = df['language'].value_counts()
english_feeds = lang_counts.get('en', 0)
perc_eng = (english_feeds / total_feeds) * 100

# 4. Missing Values (Descrizioni vuote)
# Contiamo quanti hanno la descrizione nulla o vuota
missing_desc = df['description'].isna().sum()

print(f"--- DATI PER SEZIONE 3.2 ---")
print(f"1. Totale Feed: {total_feeds}")
print(f"2. Creator Unici: {unique_creators}")
print(f"3. Arco Temporale: da {oldest_feed} a {newest_feed}")
print(f"4. Feed in Inglese: {english_feeds} ({perc_eng:.2f}%)")
print(f"5. Feed con descrizione mancante: {missing_desc}")
print(f"----------------------------")