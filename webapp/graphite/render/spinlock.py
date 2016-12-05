# -*- coding: utf-8 -*-

try:
    from graphite.settings import SPINLOCK_ADDRESS
except ImportError:
    SPINLOCK_ADDRESS = [{"host": "localhost", "port": 6379, "db": 1}]

from redlock import Redlock

SPINLOCK_TIMEOUT = 30
SPINLOCK_INTERVAL = 0.01
SPINLOCK_PREFIX = "spinlock_"

redlock = Redlock(SPINLOCK_ADDRESS)


class SpinLock(object):
    def __init__(self, lock_key):
        "SpinLock is a destribute locker implemet by redis."
        self.lock_key = SPINLOCK_PREFIX + lock_key
        self.locker = None

    def acquire(self):
        while not self.locker:
            self.locker = redlock.lock(self.lock_key, SPINLOCK_TIMEOUT * 1000)

    def release(self):
        if self.locker:
            redlock.unlock(self.locker)
            self.locker = None

    def __del__(self):
        self.release()

    def __enter__(self):
        """ Add with support """
        self.acquire()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """ Add With support """
        self.release()
        return False
