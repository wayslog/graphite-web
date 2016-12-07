# -*- coding: utf-8 -*-

import socket
import time

from graphite.render.hashing import compactHash
from graphite.logger import log

try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    from graphite.settings import DEFAULT_CACHE_DURATION
except ImportError:
    DEFAULT_CACHE_DURATION = 60

try:
    from graphite.settings import REDIS_ADDRES
    import redis
except ImportError:
    redis = None

REDIS_RECONNECT_INTEVAL = 300
REDIS_RECONNECT_LIMIT = 16
REDIS_LOCAL_PREFIX = (socket.gethostname() or "UnKownHost") + "_SP_"


def hashData(targets, start, end, prefix=False):
    target_str = ",".join(sorted(targets))
    start_str = epoch_time_str(start)
    end_str = epoch_time_str(end)
    hash_key = target_str + "@" + start_str + ":" + end_str
    prefix = REDIS_LOCAL_PREFIX if prefix else ""
    return prefix + compactHash(hash_key)


def epoch_time(timestamp):
    "caculate the up top timestamp by each interval"
    return (int(timestamp) / DEFAULT_CACHE_DURATION + 1) * DEFAULT_CACHE_DURATION


def epoch_time_str(t):
    """
    >>> import datetime
    >>> epoch_time_str(datetime.datetime(2008, 11, 10, 17, 53, 59), 10)
    '1226310830'
    """
    ts = time.mktime(t.timetuple())
    return "{}".format(epoch_time(ts))


class RedisCache(object):
    def __init__(self):
        "support get/add data to redis"
        self.redis = None if not redis else redis.StrictRedis(**REDIS_ADDRES)
        self.reconnect_count = 0
        self.latest_loss_connection = time.time()
        # clean all the lock
        # self.redis.flushall()

    def _reconnect(self):
        try:
            self.redis = redis.StrictRedis(**REDIS_ADDRES)
            self.reconnect_count = 0
            self.latest_loss_connection = time.time()
        except redis.exceptions.ConnectionError as e:
            if self.reconnect_count == REDIS_RECONNECT_LIMIT:
                log.exception("redis-cache connect faild, error:%s" %
                              e.message)
                self.redis = None
        finally:
            self.reconnect_count += 1

    def reconnect(self):
        now = time.time()
        if now - self.latest_loss_connection > REDIS_RECONNECT_INTEVAL \
           or self.reconnect_count <= REDIS_RECONNECT_LIMIT:
            self.reconnect()

    def add(self, key, data, expire_at):
        try:
            self.redis.set(key, pickle.dumps(data))
            self.redis.expireat(key, expire_at)
        except redis.ConnectionError:
            self.reconnect()

    def get(self, key):
        """get a value or get None"""
        if not self.redis: return
        try:
            data_str = self.redis.get(key)
        except redis.ConnectionError:
            self.reconnect()
        if not data_str: return
        try:
            return pickle.loads(data_str)
        except ValueError as e:
            log.exception("redis-cache catch an unkonw error: %s" % e)


cache = RedisCache()
