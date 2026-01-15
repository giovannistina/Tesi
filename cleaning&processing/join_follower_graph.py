import os, gzip

# CHANGE 1: Relative paths to your data folder
follow_path = '../data_collection'
followers_path = '../data_collection'

if __name__ == '__main__':

    output_filename = 'edgelist.csv.gz'

    # Note: Creates the file in the current folder 'cleaning&processing'
    with gzip.open(output_filename, 'a') as outf:
        
        # --- PART 1: FOLLOWS ---
        source = follow_path
        # Controllo che la cartella esista per evitare crash se non sei nella dir giusta
        if os.path.exists(source):
            for file in sorted(os.listdir(source)):
                # CHANGE 2: Filter to select only 'follows_' files
                if file.startswith('follows_') and file.endswith('.txt'):
                    print(file)
                    # CHANGE 3: os.path.join and encoding for Windows compatibility
                    with open(os.path.join(source, file), encoding='utf-8') as f:
                        while l := f.readline():
                            try:
                                u, flws = l.rstrip().split('\t')
                                for flw in flws.split():
                                    row = f"{u},{flw}\n"
                                    outf.write(row.encode('utf8'))
                            except ValueError: # no neighs
                                continue
            
            # --- PART 2: FOLLOWERS ---
            source = followers_path
            for file in sorted(os.listdir(source)):
                # CHANGE 2: Filter to select only 'followers_' files
                if file.startswith('followers_') and file.endswith('.txt'):
                    print(file)
                    # CHANGE 3: os.path.join and encoding for Windows compatibility
                    with open(os.path.join(source, file), encoding='utf-8') as f:
                        while l := f.readline():
                            try:
                                u, flws = l.rstrip().split('\t')
                                for flw in flws.split():
                                    row = f"{flw},{u}\n"
                                    outf.write(row.encode('utf8'))
                            except ValueError: # no neighs
                                continue
        else:
            print(f"Errore: La cartella {source} non esiste. Controlla di essere in 'cleaning&processing'.")

    # OUTPUT 
    if os.path.exists(output_filename):
        print(f"creato il file {output_filename} nella cartella {os.getcwd()}")