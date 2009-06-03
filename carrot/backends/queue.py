from Queue import Queue
from carrot.backends.base import BaseMessage, BaseBackend
import time

mqueue = Queue()


class Message(BaseMessage):
    """Message received from the backend.

    See :class:`carrot.backends.base.BaseMessage`.

    """


class Backend(BaseBackend):
    """Backend using the Python :mod:`Queue` library. Usually only
    used while executing unit tests.

    Please not that this backend does not support queues, exchanges
    or routing keys, so *all messages will be sent to all consumers*.

    """

    def get(self, *args, **kwargs):
        """Get the next waiting message from the queue.

        :returns: A :class:`Message` instance, or ``None`` if there is
            no messages waiting.

        """
        if not mqueue.qsize():
            return None
        return Message(backend=self, body=mqueue.get(), decoder=self.decoder)

    def consume(self, queue, no_ack, callback, consumer_tag):
        """Go into consume mode."""
        while True:
            message = mqueue.get()
            if message:
                callback(message.decode(), message)
            else:
                time.sleep(0.3)

    def prepare_message(self, message_data, delivery_mode, **kwargs):
        return message_data

    def publish(self, message, exchange, routing_key, **kwargs):
        """Publish a message to the queue."""
        mqueue.put(message)
