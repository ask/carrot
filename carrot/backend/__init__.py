from carrot.serialize import deserialize

class BaseMessage(object):
    """Base class for received messages."""

    def __init__(self, backend, **kwargs):
        self.backend = backend
        self.body = kwargs.get("body")
        self.delivery_tag = kwargs.get("delivery_tag")
        self.decoder = kwargs.get("decoder", deserialize)

    def decode(self):
        """Deserialize the message body, returning the original
        python structure sent by the publisher."""
        return self.decoder(self.body)

    def ack(self):
        """Acknowledge this message as being processed.,
        
        This will remove the message from the queue."""
        return self.backend.ack(self.delivery_tag)

    def reject(self):
        """Reject this message.

        The message will be discarded by the server.

        """
        return self.backend.reject(self.delivery_tag, requeue=False)

    def requeue(self):
        """Reject this message and put it back on the queue.

        You must not use this method as a means of selecting messages
        to process.
        
        """
        return self.backend.reject(self.delivery_tag, requeue=True)


class BaseBackend(object):

    def __init__(self, connection):
        self.connection = connection

    def queue_declare(self, *args, **kwargs):
        pass

    def exchange_declare(self, *args, **kwargs):
        pass

    def queue_bind(self, *args, **kwargs):
        pass

    def get(self, *args, **kwargs):
        pass

    def consume(self, *args, **kwargs):
        pass

    def cancel(self, *args, **kwargs):
        pass

    def ack(self, delivery_tag):
        pass

    def reject(self, delivery_tag, requeue):
        pass

    def requeue(self, delivery_tag):
        pass

    def prepare_message(self, message_data, delivery_mode):
        return message_data

    def publish(self, message, exchange, routing_key):
        pass 

    def close(self):
        pass
