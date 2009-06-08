"""

Backend base classes.

"""
from carrot.serialization import serialize, deserialize


class BaseMessage(object):
    """Base class for received messages."""
    _state = None

    def __init__(self, backend, **kwargs):
        self.backend = backend
        self.body = kwargs.get("body")
        self.delivery_tag = kwargs.get("delivery_tag")
        self.decoder = kwargs.get("decoder", deserialize)
        self._state = "RECEIVED"

    def decode(self):
        """Deserialize the message body, returning the original
        python structure sent by the publisher."""
        return self.decoder(self.body)

    def ack(self):
        """Acknowledge this message as being processed.,

        This will remove the message from the queue."""
        self.backend.ack(self.delivery_tag)
        self._state = "ACK"

    def reject(self):
        """Reject this message.

        The message will be discarded by the server.

        """
        self.backend.reject(self.delivery_tag)
        self._state = "REJECTED"

    def requeue(self):
        """Reject this message and put it back on the queue.

        You must not use this method as a means of selecting messages
        to process.

        """
        self.backend.requeue(self.delivery_tag)
        self._state = "REQUEUED"


class BaseBackend(object):
    """Base class for backends."""
    encoder = serialize
    decoder = deserialize

    def __init__(self, connection, **kwargs):
        self.connection = connection

    def queue_declare(self, *args, **kwargs):
        """Declare a queue by name."""
        pass

    def exchange_declare(self, *args, **kwargs):
        """Declare an exchange by name."""
        pass

    def queue_bind(self, *args, **kwargs):
        """Bind a queue to an exchange."""
        pass

    def get(self, *args, **kwargs):
        """Pop a message off the queue."""
        pass

    def consume(self, *args, **kwargs):
        """Start a consumer and return a iterator that can iterate over new
        messages."""
        pass

    def cancel(self, *args, **kwargs):
        """Cancel the consumer."""
        pass

    def ack(self, delivery_tag):
        """Acknowledge the message."""
        pass

    def reject(self, delivery_tag):
        """Reject the message."""
        pass

    def requeue(self, delivery_tag):
        """Requeue the message."""
        pass

    def message_to_python(self, raw_message):
        """Convert received message body to a python datastructure."""
        return raw_message

    def prepare_message(self, message_data, delivery_mode, **kwargs):
        """Prepare message for sending."""
        return message_data

    def publish(self, message, exchange, routing_key, **kwargs):
        """Publish a message."""
        pass

    def close(self):
        """Close the backend."""
        pass
