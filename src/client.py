import redis
import time

from os import path
from configparser import ConfigParser

REDIS_CONFIG = path.join(path.dirname(__file__), "redis.ini")
REDIS = 'redis'

if not path.exists(REDIS_CONFIG):
    raise Exception("config file %s cannot be found." % REDIS_CONFIG)

parser = ConfigParser()
parser.read(REDIS_CONFIG)

REDIS_HOST = parser.get(REDIS, 'host')
REDIS_PORT = parser.get(REDIS, 'port')
REDIS_W_LOCK = False

redis_conn_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT)
redis_client = redis.Redis(connection_pool=redis_conn_pool)


def redis_w_lock():
    REDIS_W_LOCK = True


def redis_free_w_lock():
    REDIS_W_LOCK = False


class Queue:
    def __init__(self, list_key, timeout=0, size=100):
        self.redis = redis_client
        self.list_key = list_key
        self.timeout = timeout

    def dequeue(self):
        while REDIS_W_LOCK:
            time.sleep(1)
        result = self.redis.blpop(self.list_key, self.timeout)
        if result:
            return result[1].decode('utf-8')
        return None

    def enqueue(self, item):
        redis_w_lock()
        result = self.redis.lpush(self.list_key, item)
        redis_free_w_lock()
        return result

    def size(self):
        return self.redis.llen(self.list_key)

    def empty(self):
        return self.size() == 0


__all__ = ['redis_client', 'Queue']
