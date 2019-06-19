from id_generator.shard import get_shard


class IDGenerator:
    """redis 分布式id生成器
    共64位：0(标志位)|41(时间戳,毫秒)|10(shard_id)|12(sequence)
    """
    def __init__(self, redis_shard):
        self.REDIS_SHARD_ID = redis_shard
        self.START_TIME = 1138723200000
        self.SHARD_ID_BITS = 10
        self.SEQUENCE_BITS = 12
        self.TIMESTAMP_BITS = self.SHARD_ID_BITS + self.SEQUENCE_BITS
        self.REDIS_SHARD_ID_KEY = 'py-id-generator-shard-id'
        self.REDIS_SEQUENCE_KEY = 'py-id-generator-sequence'
        self.ONE_SECOND_OF_MICRO = 1000
        self.MAX_SEQUENCE = 1 << self.SEQUENCE_BITS
        self.MAX_SHARD_ID = 1 << self.SHARD_ID_BITS
        
        self.redis = get_shard(redis_shard)

        if not self.redis.exists(self.REDIS_SEQUENCE_KEY):
            self.sequence_init()

    def get_time(self):
        time, microsecond = self.redis.time()
        return time * self.ONE_SECOND_OF_MICRO + int(microsecond / self.ONE_SECOND_OF_MICRO) - self.START_TIME

    def sequence_full(self):
        current_sequence = int(self.redis.get(self.REDIS_SEQUENCE_KEY).decode('utf-8'))
        return current_sequence >= self.MAX_SEQUENCE

    def sequence_init(self):
        self.redis.set(self.REDIS_SEQUENCE_KEY, 0)

    def is_valid_shard_id(self):
        redis_shard_id = self.redis.get(self.REDIS_SHARD_ID_KEY).decode('utf-8')
        return int(redis_shard_id) == self.REDIS_SHARD_ID

    def get_id(self):
        return list(self.get_ids())[0]

    def get_ids(self, count=1):
        if self.sequence_full():
            self.sequence_init()

        seq_end = self.redis.incrby(self.REDIS_SEQUENCE_KEY, count)
        seq_start = seq_end - count
        time = self.get_time()

        while seq_start < seq_end:
            time_shifted = time << self.TIMESTAMP_BITS
            shard_shifted = self.REDIS_SHARD_ID << self.SHARD_ID_BITS
            sequence_shifted = seq_start << self.SEQUENCE_BITS

            yield (time_shifted | shard_shifted | sequence_shifted)
            seq_start += 1


if __name__ == '__main__':
    generator = IDGenerator(2)
    print(generator.get_id())

