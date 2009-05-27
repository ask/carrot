from Queue import Queue
from carrot.backends import BaseMessage, BaseBackend

mqueue = Queue()

class Message(BaseMessage):
    pass


class Backend(BaseBackend):

    def get(self, *args, **kwargs):
        if not mqueue.qsize():
            return None
        return Message(backend=self, body=mqueue.get())

    def consume(self, queue, no_ack, callback, consumer_tag):
        yield callback(mqueue.get())

    def prepare_message(self, message_data, delivery_mode):
        return message_data

    def publish(self, message, exchange, routing_key):
        mqueue.put(message)
        
