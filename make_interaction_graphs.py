import gzip



if __name__ == '__main__':
        
    with gzip.open('results/interactions.csv.gz') as f:

        with gzip.open('results/replies.csv.gz', 'w') as f_replies:
            with gzip.open('results/reposts.csv.gz', 'w') as f_reposts:
                with gzip.open('results/quotes.csv.gz', 'w') as f_quotes:
                    for line in f:
                        line = line.decode('utf-8').rstrip()
                        # split and convert to int
                        fields = line.split(',')
                        fields = [int(f) if f.isdigit() else None for f in fields]
                        author, reply, root, repost, quote, date = fields
                        date = str(date)[:8]# yyyymmdd
                        date = int(date)

                        if reply:
                            nline = f'{author},{reply},{date}\n'
                            f_replies.write(nline.encode('utf-8'))
                        if repost:
                            nline = f'{author},{repost},{date}\n'
                            f_reposts.write(nline.encode('utf-8'))
                        if quote:
                            nline = f'{author},{quote},{date}\n'
                            f_quotes.write(nline.encode('utf-8'))



