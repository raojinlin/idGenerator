import redis
import configparser
import random

from os import path

shard_config = path.join(path.dirname(__file__), "shard.ini")

if not path.exists(shard_config):
    raise FileNotFoundError(shard_config)

parser = configparser.ConfigParser()
parser.read(shard_config)


def get_shards():
    shard_section = parser.sections()
    shards = []
    for shard in shard_section:
        shards.append(get_redis(shard))

    return shards


def get_redis(shard):
    host = parser.get(shard, "host")
    port = parser.get(shard, "port")
    conn = redis.ConnectionPool(host=host, port=port)
    return redis.Redis(connection_pool=conn)


def get_random_shard():
    shards = get_shards()
    return shards[random.randrange(0, len(shards))]


def get_shard(shard_id):
    for shard in parser.sections():
        if shard_id == int(parser.get(shard, "id")):
            return get_redis(shard)

    raise UnknownShardException("unknown shard '%d'" % shard_id)


class UnknownShardException(Exception):
    pass
