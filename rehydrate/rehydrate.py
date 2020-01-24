from pyspark.sql import SparkSession
from clize import run 
import os
from os import getenv
from toolz import curry
from itertools import takewhile, islice, count, chain, tee
from time import sleep
import orjson
import json
from tweepy.utils import parse_datetime
from tweepy.error import TweepError
import tweepy
from tweepy.cache import MemoryCache, MongodbCache
from functools import reduce
from datetime import datetime, timedelta
import pyspark.sql.types as pst
from pyspark.sql.types import _merge_type
from copy import deepcopy
from pyspark.sql.types import TimestampType, LongType, ArrayType, MapType, StringType, NullType, StructType
from functools import reduce
from datetime import datetime
from pymongo import MongoClient
from gcsfs import GCSFileSystem
import os
import gcsfs
from gcsfs.utils import HttpError
import logging
import pickle



class FSCache():
    def __init__(self, fs, path):
        self.fs = fs
        self.path = path

    def _normalize_key(self, key):
        if type(key) != str:
            key = str(key)
        return key

    def set(self, key, value):
        key = self._normalize_key(key)
            
        if type(value) != str:
            value = json.dumps(value)

        if value is None:
            raise Exception('Cannot set None value!')
        
        fi = os.path.join(self.path, key)
        try:
            with self.fs.open(fi, 'w') as f:
                f.write(value)
        except HttpError as e:
            logging.error(e)
            pass
    
    def get(self, key):
        key = self._normalize_key(key)

        fi = os.path.join(self.path, key)
        if not self.fs.exists(fi):
            return None

        try:
            with self.fs.open(fi) as f:
                value = f.read()
        except Exception as e:
            logging.error(f'NETWORK FAILURE: Could not get key: {key}')
            logging.error(e)
            return None

        return json.loads(value)

def lookup(api, tweets):
    ids = [tw['id'] for tw in tweets]
    res = api.statuses_lookup(ids, include_entities=True, map_=True)
    res = [r._json for r in res]
    return tweets, res

def slow_lookup(api, cache, tweets, calls = 0):
    sleep(calls)

    try:
        tweets, res = lookup(api, tweets)

    except TweepError:
        if calls < 5:
            return slow_lookup(api, cache, tweets, calls+1)
        else:
            raise

    for tw,r in zip(tweets, res):
        cache.set(tw['id'], r)

    return tweets, res

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def assoc(di, k, v):
    di[k] = v
    return di

def combine(tw, res):
    rehydrated = res if res.get('text') else {}
    rehydrated['th_rehydrated'] = 'text' in res
    rehydrated['th_original'] = tw
    rehydrated['id'] = tw['id']
    return rehydrated

def format_result(tweets, res):
    return [combine(t,r) for t,r in 
            zip(sorted(tweets, key=lambda x: x['id']), sorted(res, key=lambda x: x['id']))]

def rehydrate(tweets):
    api = get_tweepy()
    cache = FSCache(gcsfs.GCSFileSystem(project='trollhunters'), 'spain-tweets/cache')

    tweets = ((tw, cache.get(tw['id'])) for tw in tweets)

    left, right = tee(tweets)
    cached = ((tw,c) for tw,c in left if c is not None)
    cached = (zip(*c) for c in chunk(100, cached))
    tweets = (tw for tw,c in right if c is None)

    results = (slow_lookup(api, cache, tw) for tw in chunk(100, tweets))
    results = (format_result(t,r) for t,r in chain(cached, results))
    results = (y for x in results for y in x)

    return results


def parse_dates(obj, key):
    _parse = lambda k,v: parse_dates(v, key) if k != key else parse_datetime(v)

    if isinstance(obj, dict):
        return {k:_parse(k,v) for k,v in obj.items()}
    elif isinstance(obj, list):
        return [parse_dates(vi, key) for vi in obj]
    else:
        return obj

def replace_with_type(schema, target, NewType):
    if hasattr(schema, 'fields'):
        for field in schema.fields:
            if field.name == target:
                field.dataType = NewType()
            else:
                field.dataType = replace_with_type(field.dataType, target, NewType)
        return schema
    elif hasattr(schema, 'elementType'):
        schema.elementType = replace_with_type(schema.elementType, target, NewType)
        return schema
    else:
        return schema

def replace_timestamps_in_schema(schema):
    schema = deepcopy(schema)
    schema = replace_with_type(schema, 'created_at', TimestampType)
    schema = replace_with_type(schema, 'iDate', TimestampType)
    return schema

def replace_timestamps_in_dat(dat):
    dat = parse_dates(dat, 'created_at')
    # dat['original']['iDate'] = datetime.fromisoformat(dat['original']['iDate'])
    return dat

def tweet_json_with_timestamps(spark, df):
    schema = replace_timestamps_in_schema(df.schema)
    dat = df.rdd.map(replace_timestamps_in_dat)
    return spark.createDataFrame(dat, schema)


def get_tweepy():
    auth = tweepy.OAuthHandler(getenv('T_CONSUMER_TOKEN'), getenv('T_CONSUMER_SECRET'))
    auth.set_access_token(getenv('T_ACCESS_TOKEN'), getenv('T_TOKEN_SECRET'))

    api = tweepy.API(auth, 
                 retry_errors = {420, 429, 500, 502, 503, 504},
                 retry_count = 20,
                 retry_delay = 120,
                 wait_on_rate_limit=True, 
                 wait_on_rate_limit_notify=True)

    return api

def build_spark():
    spark = SparkSession \
        .builder \
        .appName("Rehydrate Tweets") \
        .config("spark.jars", "/home/jovyan/work/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/jovyan/work/key.json") \
        .config("spark.driver.memory", "24g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

    return spark



def read_schema(path):
    fs = GCSFileSystem(project='trollhunters')
    with fs.open(path, 'rb') as f:
        schema = pickle.load(f)
    return schema

def write(month, outpath, percentage = 0.01):
    spark = build_spark()

    tweets = spark.read.parquet(f'gs://spain-tweets/ub-originals') \
                   .where(f'month = {month}')

    if percentage < 1.0:
        tweets = tweets.sample(percentage)

    tweets = tweets \
        .rdd \
        .repartition(96000) \
        .map(lambda x: x.asDict()) \
        .mapPartitions(rehydrate, preservesPartitioning=True) \
        .map(replace_timestamps_in_dat)

    schema = read_schema('gs://spain-tweets/schemas/tweet-3.pickle')

    # tdf = spark.read.json(tweets.map(lambda x: orjson.dumps(x).decode('utf-8')))
    # tweet_json_with_timestamps(spark, tdf)

    spark.createDataFrame(tweets, schema).write \
                                   .mode('overwrite') \
                                   .parquet(outpath)



if __name__ == '__main__':
    run(write)
