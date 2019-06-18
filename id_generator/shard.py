import redis
import configparser

from os import path

shard_config = path.join(path.dirname(__file__), "shard.ini")

if not path.exists(shard_config):
    raise FileNotFoundError(shard_config)

parser = configparser.ConfigParser()
parser.read(shard_config)

SHARDS = []

for section in parser.sections():
    SHARDS.append(parser.items(section))


def get_shard(shard_id):
    pass