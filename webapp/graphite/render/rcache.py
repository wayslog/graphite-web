# -*- coding: utf-8 -*-
import time
from itertools import chain
from hashlib import md5
from graphite.render.hashing import compactHash

try:
    from graphite.settings import cache_interval
except ImportError:
    cache_interval = 10
try:
    from graphite.settings import REDIS_ADDRES
    import redis
    redis_conn = redis.StrictRedis(**REDIS_ADDRES)
except ImportError:
    redis_conn = None

try:
    import cPickle as pickle
except ImportError:
    import pickle


def hashData(targets, start, end):
    target_str = ",".join(sorted(targets))
    start_str = epoch_time(start, cache_interval)
    end_str = epoch_time(end, cache_interval)
    hash_key = target_str + "@" + start_str + ":" + end_str
    return compactHash(hash_key)

def epoch_time(t, interval):
    """
    >>> import datetime
    >>> epoch_time(datetime.datetime(2008, 11, 10, 17, 53, 59), 10)
    '1226310830'
    """
    ts = time.mktime(t.timetuple())
    return "{}".format((int(ts)/interval)*interval)


class RedisCache(object):
    def __init__(self, conn):
        "support get/add data to redis"
        self.redis = conn

    def add(self, key, data, expire=10):
        if not self.redis: return
        self.redis.set(key, pickle.dumps(data), ex=expire)

    def get(self, key):
        """get a value or get None"""
        if not self.redis:
            return None
        ds = self.redis.get(key)
        if not ds:
            return None
        return pickle.loads(ds)

cache = RedisCache(redis_conn)
