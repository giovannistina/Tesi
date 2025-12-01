import matplotlib.pyplot as plt
import pandas as pd
from collections import defaultdict
import numpy as np
# pip install adjustText
try:
    from adjustText import adjust_text
except ImportError:
    print('Please run pip install adjustText')

RW = 7 # rolling window

# decorator advising the user to run the required scripts
def require_script(script):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                func(*args, **kwargs)
            except FileNotFoundError:
                print(f"Please run {script} first.")
        return wrapper
    return decorator

def compute_iqr(data):
    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 75)
    mn = np.mean([x for x in data if q1 <= x <= q3])
    return q1, q3, mn # lower quartile, upper quartile, mean of the IQR

def load_post_stats():
    toiqr = defaultdict(lambda: defaultdict(int))
    with open('post_stats.txt') as f:
        head = f.readline()
        while line := f.readline():
            date, _, user = line.rstrip().split()
            try:
                date = int(date)
            except ValueError:
                continue
            user = int(user)
            toiqr[date][user] += 1
    
    dates, avg_daily_posts, daily_posts, active_users = [], [], [], []
    means, q1s, q3s = [], [], []

    for dt, users in sorted(toiqr.items(), key=lambda x: x[0]):
        post_counts = []
        for user, post_count in users.items():
            post_counts.append(post_count)
        dates.append(dt)
        active_users.append(len(post_counts))
        avg_daily_posts.append(np.mean(post_counts))
        daily_posts.append(sum(post_counts))
        
        q1, q3, mn  = compute_iqr(post_counts)
        q1s.append(q1)
        q3s.append(q3)
        means.append(mn)
    return dates, avg_daily_posts, daily_posts, active_users, q1s, q3s, means

@require_script('post_stats.py')
def plot_posts_per_user():
    dates, avg_daily_posts, _, _, q1s, q3s, means = load_post_stats()
    mns =pd.Series(np.array(means), index=pd.to_datetime(dates, format='%Y%m%d'))
    mns.rolling(RW).mean().plot(label='iqr')
    adp = pd.Series(np.array(avg_daily_posts), index=pd.to_datetime(dates, format='%Y%m%d'))
    ax = adp.rolling(RW).mean().plot(figsize=(8,3), color='red', label='global', alpha=.6)
    #mx2 =pd.Series(np.array(mx), index=pd.to_datetime(dates, format='%Y%m%d'))
    #ax = mx2.rolling(RW).mean().plot()

    eq1 = pd.Series(q1s,  index=pd.to_datetime(dates, format='%Y%m%d')).rolling(RW).mean()
    eq3 = pd.Series(q3s,  index=pd.to_datetime(dates, format='%Y%m%d')).rolling(RW).mean()
    ax.fill_between(pd.to_datetime(dates, format='%Y%m%d'),eq1, eq3, alpha=.25)
    ax.set_ylabel('posts per user',fontsize=15)
    plt.yticks(fontsize=13)
    plt.xticks(fontsize=13)
    plt.grid(alpha=.2)
    plt.ylim(-0.5, 9.3)
    plt.legend(fontsize=13, loc='best')
    plt.tight_layout()
    plt.savefig('avgposts.png')
    plt.show()

@require_script('post_stats.py')
def plot_number_of_posts():
    dates, _, daily_posts, _ , _, _, _ = load_post_stats()
    dp = pd.Series(np.array(daily_posts), index=pd.to_datetime(dates, format='%Y%m%d'))
    ax = dp.rolling(RW).mean().plot(figsize=(8,3))
    ax.set_ylabel('daily posts',fontsize=15)
    plt.yticks(fontsize=13)
    plt.xticks(fontsize=13)
    plt.grid(alpha=.2)
    plt.tight_layout()
    plt.savefig('dailyposts.png')
    plt.show()

@require_script('inter_event_time.py')
def plot_inter_event_time_ecdf():
    res = []
    with open('inter-time.txt') as f:
        while line := f.readline():
            t, delta = line.strip().split()
            res.append((int(t), int(delta)))
    df = pd.DataFrame(res, columns=['date','delta'])

    preopen = df[df.date < 20240206].delta
    afteropen = df[df.date >= 20240206].delta
    df['mo'] = df.date.apply(lambda x: str(x)[2:6])
    df.date = pd.to_datetime(df.date, format='%Y%m%d')
    plt.ecdf(df.delta, label='all')
    plt.grid(alpha=.2)
    plt.ecdf(preopen, label='invite only')
    plt.ecdf(afteropen, label='free access')
    plt.ylim(-0.05, 1.05)
    plt.ylabel('Proportion', fontsize=15)

    plt.xlabel('$\Delta$ days', fontsize=15)
    plt.yticks(fontsize=13)
    plt.xticks(fontsize=13)
    plt.legend(fontsize=15)
    plt.savefig('delta-days.png')
    plt.show()

@require_script('instances_dist.py')
def plot_instances():
    inst_p = dict()
    inst_u = dict()
    with open('instance_posts.csv') as f:
        for l in f.readlines():
            inst, freq = l.rstrip().split(',')
            inst_p[inst] = int(freq)
    with open('instance_users.csv') as f:
        for l in f.readlines():
            inst, freq = l.rstrip().split(',')
            inst_u[inst] = int(freq)
    inst = pd.DataFrame([inst_p, inst_u]).T
    inst.columns = ['posts', 'users']

    print('<100 users', len(inst[inst.users < 100]))
    inst = inst[inst.users >= 500]
    print('>=100 users', len(inst))

    plt.scatter(x=inst.posts, y=inst.users, alpha=.5)

    texts = []
    for idx, row in inst.iterrows():
        #if row['posts'] >= 100000:
        t = plt.annotate(idx, (row['posts'], row['users']), fontsize=12)
        texts.append(t)

    plt.loglog()
    plt.xticks(fontsize=15)
    plt.yticks(fontsize=15)
    plt.xlabel('# posts', fontsize=15)
    plt.ylabel('# users', fontsize=15)
    adjust_text(texts, arrowstyle='-', color='red')
    plt.savefig('instances.png', bbox_inches = "tight")
    plt.show()

@require_script('sentiment_table.py')
def plot_sentiment():

    df = pd.read_csv('sentiment_table.csv')
    df.index = pd.to_datetime(df.date.tolist(), format='%Y%m%d')
    for k in ['positive', 'negative', 'neutral']:
        df[k] = df[k]/df.total
    ax = df.positive.rolling(RW).mean().plot(figsize=(8,3))
    df.negative.rolling(RW).mean().plot()
    df.neutral.rolling(RW).mean().plot()
    ax.set_ylabel('ratio',fontsize=15)
    plt.yticks(fontsize=13)

    xt=[19539., 19570., 19601., 19631., 19662., 19692., 19723., 19754.,
       19783.]
    xtl= ['Jul','Aug','Sep','Oct','Nov','Dec','Jan\n2024','Feb','Mar']

    plt.xticks(ticks=xt, labels=xtl, rotation=False, ha='center', fontsize=13)
    plt.grid(alpha=.2)
    plt.legend(loc='best', fontsize=12)
    plt.ylim(0, .5)
    plt.tight_layout()
    plt.savefig('sent_ratio.png')

@require_script('to_topics.py')
@require_script('topic_extraction.py')
def plot_topics():
    df = pd.read_csv('topics_info.csv')
    df.index = df.Name
    toplot = df[1:6].Count
    plt.figure(figsize=(10,5))
    plt.barh(list(reversed(toplot.index)), 
            list(reversed(toplot)),
            alpha=.7)
    plt.yticks(fontsize=13)
    plt.xticks(fontsize=13)
    plt.tight_layout()
    plt.savefig('5topics_dist.png')
    plt.show()

    


if __name__ == '__main__':
    print("Hello, World!")

    # REQUIRE RUNNING post_stats.py
    plot_posts_per_user()
    plot_number_of_posts()

    # REQUIRES RUNNING inter_event_time.py
    plot_inter_event_time_ecdf()

    # REQUIRES RUNNING instances_dist.py
    plot_instances()

    # REQUIRES RUNNING sentiment_table.py 
    plot_sentiment()

    # REQUIRES RUNNING to_topics.py and topic_extraction.py
    plot_topics()
    
    