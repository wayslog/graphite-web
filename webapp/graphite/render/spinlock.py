import time
import redis

SPINLOCK_TIMEOUT = 60
SPINLOCK_INTERVAL = 0.01

redis_client = redis.StrictRedis(host="localhost", port=6379)


class SpinLock(object):
    def __init__(self, lock_key):
        "SpinLock is a destribute locker implemet by redis."
        self.lock = 0
        self.lock_timeout = 0
        self.lock_key = lock_key

    def acquire(self):
        while self.lock != 1:
            now = int(time.time())
            self.lock_timeout = now + SPINLOCK_TIMEOUT + 1
            self.lock = redis_client.setnx(self.lock_key, self.lock_timeout)
            lock_val = redis_client.get(self.lock_key)
            if not lock_val:
                continue
            if self.lock == 1 or (now > int(lock_val)) and now > int(
                    redis_client.getset(self.lock_key, self.lock_timeout)):
                break
            else:
                time.sleep(SPINLOCK_INTERVAL)

    def release(self):
        now = int(time.time())
        if now < self.lock_timeout:
            redis_client.delete(self.lock_key)

    def __enter__(self):
        """ Add with support """
        self.acquire()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """ Add With support """
        self.release()
        return False
