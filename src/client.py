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


def get_shard(shard_id):
    return redis_client


class Queue:
    def __init__(self, list_key, timeout=0, max_size=100):
        self.redis = redis_client
        self.list_key = list_key
        self.timeout = timeout
        self.max_size = max_size

    def dequeue(self):
        while REDIS_W_LOCK:
            time.sleep(1)
        result = self.redis.brpop(self.list_key, self.timeout)
        if result:
            return result[1].decode('utf-8')
        return None

    def enqueue(self, item):
        redis_w_lock()
        while self.full():
            print("queue full. waiting consume.")
            time.sleep(1)

        result = self.redis.lpush(self.list_key, item)
        redis_free_w_lock()
        return result

    def full(self):
        return self.size() >= self.max_size

    def size(self):
        return self.redis.llen(self.list_key)

    def empty(self):
        return self.size() == 0


__all__ = ['redis_client', 'Queue', 'get_shard']
