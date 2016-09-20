# -*- coding: utf-8 -*-

import time
from graphite.render.hashing import compactHash
from graphite.logger import log
import socket

try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    from graphite.settings import cache_interval
except ImportError:
    cache_interval = 10
try:
    from graphite.settings import REDIS_ADDRES
    import redis
except ImportError:
    redis = None

REDIS_RECONNECT_INTEVAL = 300
REDIS_RECONNECT_LIMIT = 16
REDIS_LOCAL_PREFIX = (socket.gethostname() or "UnKownHost") + "_SP_"

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
    def __init__(self):
        "support get/add data to redis"
        self.redis = None if not redis else redis.StrictRedis(**REDIS_ADDRES)
        self.reconnect_count = 0
        self.latest_loss_connection = time.time()
        self.suffix = "host"

    def _reconnect(self):
        try:
            self.redis = redis.StrictRedis(**REDIS_ADDRES)
            self.reconnect_count = 0
            self.latest_loss_connection = time.time()
        except redis.exceptions.ConnectionError as e:
            if self.reconnect_count == REDIS_RECONNECT_LIMIT:
                log.exception("redis-cache connect faild, error:%s" % e.message)
                self.redis = None
        finally:
            self.reconnect_count += 1

    def reconnect(self):
        now = time.time()
        if now - self.latest_loss_connection > REDIS_RECONNECT_INTEVAL \
           or self.reconnect_count <= REDIS_RECONNECT_LIMIT:
            self.reconnect()

    def add(self, key, data, expire=10):
        if not self.redis: return
        try:
            self.redis.set(REDIS_LOCAL_PREFIX + key, pickle.dumps(data), ex=expire)
        except redis.ConnectionError:
            self.reconnect()

    def get(self, key):
        """get a value or get None"""
        if not self.redis: return
        try:
            data_str = self.redis.get(REDIS_LOCAL_PREFIX + key)
        except redis.ConnectionError:
            self.reconnect()
        if not data_str: return
        try:
            return pickle.loads(data_str)
        except ValueError as e:
            log.exception("redis-cache catch an unkonw error: %s" % e)


cache = RedisCache()
