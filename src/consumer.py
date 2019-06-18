from src.client import redis_client, Queue
import time


class Consumer:
    def __init__(self, handler, list_key):
        self.handler = handler
        self.queue = Queue(list_key=list_key)

    def run(self):
        while True:
            self.handler(self.queue.dequeue())


if __name__ == "__main__":
    def handler(name):
        if name:
            print("consume: " + name)
            time.sleep(1)
        else:
            print("got nothing, waiting....")

    consumer = Consumer(handler, "tasks")
    consumer.run()

