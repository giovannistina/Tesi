import os, gzip

follow_path = '/Users/andreafailla/Desktop/users/follows'
followers_path = '/Users/andreafailla/Desktop/users/followers'

if __name__ == '__main__':

    with gzip.open(f'edgelist.csv.gz', 'a') as outf:
        source = follow_path
        for file in sorted(os.listdir(source)):
            if file[0] !='.':
                print(file)
                with open(source+'/'+file) as f:
                    while l := f.readline():
                        try:
                            u, flws = l.rstrip().split('\t')
                            for flw in flws.split():
                                row = f"{u},{flw}\n"
                                outf.write(row.encode('utf8'))
                        except ValueError: # no neighs
                            continue
        source = followers_path
        for file in sorted(os.listdir(source)):
            if file[0] !='.':
                print(file)
                with open(source+'/'+file) as f:
                    while l := f.readline():
                        try:
                            u, flws = l.rstrip().split('\t')
                            for flw in flws.split():
                                row = f"{flw},{u}\n"
                                outf.write(row.encode('utf8'))
                        except ValueError: # no neighs
                            continue

            