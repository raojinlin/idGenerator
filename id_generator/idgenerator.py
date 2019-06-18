from src.client import get_shard


class IDGenerator:
    """redis 分布式id生成器
    共64位：0(标志位)|41(时间戳,微秒)|12(shard_id)|10(sequence)
    """
    def __init__(self, redis_shard):
        self.REDIS_SHARD_ID = redis_shard
        self.START_TIME = 1138723200000
        self.SHARD_ID_BITS = 12
        self.SEQUENCE_BITS = 10
        self.TIMESTAMP_BITS = self.SHARD_ID_BITS + self.SEQUENCE_BITS
        self.REDIS_SHARD_ID_KEY = 'py-id-generator-shard-id'
        self.REDIS_SEQUENCE_KEY = 'py-id-generator-sequence'
        self.redis = get_shard(self.REDIS_SHARD_ID)
        self.ONE_SECOND_OF_MICRO = 1000
        self.MAX_SEQUENCE = 1 >> self.SEQUENCE_BITS
        self.MAX_SHARD_ID = 1 >> self.SHARD_ID_BITS

        if not self.redis.exists(self.REDIS_SEQUENCE_KEY):
            self.sequence_init()

    def get_time(self):
        time, microsecond = self.redis.time()
        return time * self.ONE_SECOND_OF_MICRO + microsecond - self.START_TIME

    def sequence_full(self):
        current_sequence = int(self.redis.get(self.REDIS_SEQUENCE_KEY).decode('utf-8'))
        return current_sequence >= self.MAX_SEQUENCE

    def sequence_init(self):
        self.redis.set(self.REDIS_SEQUENCE_KEY, 0)

    def is_valid_shard_id(self):
        redis_shard_id = self.redis.get(self.REDIS_SHARD_ID_KEY).decode('utf-8')
        return int(redis_shard_id) == self.REDIS_SHARD_ID

    def get_id(self):
        return self.get_ids()[0]

    def get_ids(self, count=1):
        if self.sequence_full():
            self.sequence_init()
            
        seq_end = self.redis.incrby(self.REDIS_SEQUENCE_KEY, count)
        seq_start = seq_end - count
        time = self.get_time()

        ids = []
        while seq_start < seq_end:
            time_shifted = time << self.TIMESTAMP_BITS
            shard_shifted = self.REDIS_SHARD_ID << self.SHARD_ID_BITS
            sequence_shifted = seq_start << self.SEQUENCE_BITS

            ids.append(time_shifted | shard_shifted | sequence_shifted)
            seq_start += 1

        return ids


if __name__ == '__main__':
    generator = IDGenerator(1)
    res = generator.get_ids(10)
    print(res)