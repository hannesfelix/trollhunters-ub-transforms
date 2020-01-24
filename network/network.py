from pyspark.sql import SparkSession
from clize import run
from langdetect import detect_langs
from langdetect.lang_detect_exception import LangDetectException
from itertools import permutations
import networkx as nx
from gcsfs import GCSFileSystem

def build_spark():
    spark = SparkSession \
        .builder \
        .appName("Build Tweet Network") \
        .config("spark.jars", "/home/jovyan/work/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/jovyan/work/key.json") \
        .config("spark.driver.memory", "12g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    return spark

def confident_lang(text):
    try: 
        langs = detect_langs(text)
        top = langs[0]
        if top.prob > 0.75:
            return top.lang
        elif top.lang == 'cat' or top.lang == 'es':
            # print(f'could not find language.\n Probs: {langs}.\n Text: {text}')
            return None
    except LangDetectException:
        return None

def detect_and_filter(retweets):
    retweets = [(confident_lang(t['text']), t) for t in retweets]
    return [{**t, 'lang': lang} for lang,t in retweets if lang is not None]

def get_dat(spark, tweets):
    tweets.createOrReplaceTempView('tweets')

    df = spark.sql("""
    WITH users_retweets as 
        (WITH t as 
            (SELECT
                rehydrated.id_str as id_str, 
                rehydrated.user.id_str as user, 
                struct(
                    rehydrated.retweeted_status.text as text, 
                    rehydrated.retweeted_status.id_str, 
                    rehydrated.retweeted_status.user.id_str as user
                ) as retweeted_status 
            FROM tweets 
            WHERE rehydrated.retweeted_status is not null) 
        SELECT user, collect_list(retweeted_status) AS retweets FROM t GROUP BY user) 
    SELECT * FROM users_retweets WHERE size(retweets) > 1
    """)

    dat = df.rdd \
            .map(lambda r: r.asDict(True)) \
            .map(lambda d: {**d, 'retweets': detect_and_filter(d['retweets'])}) \
            .filter(lambda d: len(d['retweets']) > 1) \
            .cache()

    return dat

def get_tweets(dat):
    tweets = dat \
        .flatMap(lambda d: [(r['id_str'], {'retweet': r, 'users': [d['user']], 'count': 1}) 
                            for r in d['retweets']]) \
        .reduceByKey(lambda a,b: {**a, 'users': a['users'] + b['users'], 'count': a['count'] + b['count']}) \
        .map(lambda t: t[1]) \
        .filter(lambda t: t['count'] > 1) \
        .map(lambda t: {**t['retweet'], 'users': t['users'], 'count': t['count']}) \
        .cache()
    
    return tweets



def pmf(items):
    d = {}
    for i in items:
        d[i] = d.get(i, 0) + 1
    tot = sum(d.values())
    return {k:v/tot for k,v in d.items()}

def user_lang(di):
    if di.get('ca', 0) > 0.10:
        return 'ca'
    
    lang,val = None,0

    for k,v in di.items():
        if v > val:
            lang,val = k,v

    return lang

def build_user_graph(tweets):
    user_nodes = tweets \
        .flatMap(lambda d: [(u, {'langs': [d['lang']], 'count': 1}) for u in d['users']]) \
        .reduceByKey(lambda a,b: {'langs': a['langs'] + b['langs'], 'count': a['count'] + b['count']}) \
        .map(lambda a: {'user': a[0], 'langs': a[1]['langs'], 'count': a[1]['count']}) \
        .map(lambda u: {**u, 'langs': pmf(u['langs'])}) \
        .map(lambda u: {**u, 'lang': user_lang(u['langs'])}) \
        .collect()

    user_edges = tweets \
        .flatMap(lambda d: permutations(d['users'], 2)) \
        .map(lambda t: (t, 1)) \
        .reduceByKey(lambda a,b: a+b) \
        .collect()

    return user_nodes, user_edges

def build_tweet_graph(tweets, dat):
    tweet_nodes = tweets.collect()
    tweet_ids = set([n['id_str'] for n in tweet_nodes])

    tweet_edges = dat \
        .flatMap(lambda d: permutations([r['id_str'] for r in d['retweets']], 2)) \
        .filter(lambda t: t[0] in tweet_ids and t[1] in tweet_ids) \
        .map(lambda t: (t, 1)) \
        .reduceByKey(lambda a,b: a+b) \
        .collect()

    return tweet_nodes, tweet_edges


def create_graph(nodes, edges, id_key):
    G = nx.Graph()

    for n in nodes: 
        G.add_node(n[id_key], lang = n['lang'], count = n['count'])

    for edge, weight in edges:
        G.add_edge(*edge, weight=weight)

    return G


def main(month, type_, outfile):
    spark = build_spark()
    raw_dat = spark.read.parquet('gs://spain-tweets/rehydrated/lake').where(f'month = {month}')
    dat = get_dat(spark, raw_dat)
    tweets = get_tweets(dat)

    if type_ == 'tweets':
        nodes, edges = build_tweet_graph(tweets, dat)
        G = create_graph(nodes, edges, 'id_str')

    elif type_ == 'users':
        nodes, edges = build_user_graph(tweets)
        G = create_graph(nodes, edges, 'user')

    else:
        raise TypeError(f'Unrecognized type_ parameter: {type_}')

    fs = GCSFileSystem(project = 'trollhunters')
    with fs.open(outfile, 'wb') as f:
        nx.write_graphml(G, f)            


if __name__ == '__main__':
    run(main)
