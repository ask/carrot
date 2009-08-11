from stomp import Stomp
from carrot.backends.base import BaseBackend
from uuid import uuid4 as gen_unique_id


class Backend(BaseBackend):
    Stomp = Stomp

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.stomp = _establish_connection(self.connection)
        self._queues = {}

    def _establish_connection(self, connection_info):
        stomp = self.Stomp(connection_info.hostname, connection_info.port)
        stomp.connect()
        return stomp

    def queue_exists(self, queue):
        return True

    def declare_consumer(self, queue, no_ack, callback, consumer_tag,
            **kwargs):
        ack = "auto" if no_ack else "client"
        qid = self.stomp.subscribe({"destination": queue, "ack": ack})
        self._queues[queue] = qid

    def get(self, queue, no_ack=False):

    def ack(self, delivery_tag):
        self.stomp.ack(delivery_tag)

    def prepare_message(self, message_data, delivery_mode, **kwargs):
        content_type = kwargs.get("content_type")
        content_encoding = kwargs.get("content_encoding")
        persistent = True if delivery_mode == 2 else False
        return {"body": message_data, "persistent": persistent,
                "content_encoding": content_encoding,
                "content_type": content_type}

    def publish(self, message, exchange, routing_key, mandatory):
        message["destination"] = exchange
        self.stomp.send(message)

    def cancel(self, consumer_tag):
        

    def close(self):
        for queue in _queues.keys():
            self.stomp.unsubscribe({"destination": queue})
