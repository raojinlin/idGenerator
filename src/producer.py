import time
import random
from src.client import Queue


class Producer:
    def __init__(self, list_key):
        self.queue = Queue(list_key=list_key)

    def push(self, item):
        print("push item: ", item)
        self.queue.enqueue(item)


if __name__ == '__main__':
    producer = Producer("tasks")
    while True:
        producer.push(random.randint(0, 100))
        time.sleep(1)
