from stomp import Stomp
from carrot.backends.base import BaseMessage, BaseBackend
from uuid import uuid4 as gen_unique_id


class Message(BaseMessage):
    """A message received by the STOMP broker.

    Usually you don't insantiate message objects yourself, but receive
    them using a :class:`carrot.messaging.Consumer`.

    :param backend: see :attr:`backend`.
    :param frame: see :attr:`_frame`.

    .. attribute:: body

        The message body.

    .. attribute:: delivery_tag

        The message delivery tag, uniquely identifying this message.

    .. attribute:: backend

        The message backend used.
        A subclass of :class:`carrot.backends.base.BaseBackend`.

    .. attribute:: _frame

        The frame received by the STOMP client. This is considered a private
        variable and should never be used in production code.

    """

    def __init__(self, backend, frame, **kwargs):
        self._frame = frame
        self.backend = backend

        kwargs["body"] = frame.body
        kwargs["delivery_tag"] = frame.headers["message-id"]
        kwargs["content_type"] = frame.headers.get("content_type")
        kwargs["content_encoding"] = frame.headers.get("content_encoding")

        super(Message, self).__init__(backend, **kwargs)

    def ack(self):
        """Acknowledge this message as being processed.,
        This will remove the message from the queue.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.backend.ack(self.frame)
        self._state = "ACK"

    def reject(self):
        raise NotImplementedError(
            "The STOMP backend does not implement basic.reject")

    def requeue(self):
        raise NotImplementedError(
            "The STOMP backend does not implement requeue")


class Backend(BaseBackend):
    Stomp = Stomp

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self._channel = None
        self._queues = {}

    def establish_connection(self):
        conninfo = self.connection
        stomp = self.Stomp(conninfo.hostname, conninfo.port)
        stomp.connect()
        return stomp

    def queue_exists(self, queue):
        return True

    def declare_consumer(self, queue, no_ack, callback, consumer_tag,
            **kwargs):
        ack = "auto" if no_ack else "client"
        qid = self.channel.subscribe({"destination": queue, "ack": ack})
        self._queues[queue] = qid

    def get(self, queue, no_ack=False):
        frame = self.channel.receive_frame()
        if not frame:
            return None
        return self.message_to_python(frame)

    def ack(self, frame):
        self.channel.ack(frame)

    def message_to_python(self, raw_message):
        """Convert encoded message body back to a Python value."""
        return Message(backend=self, frame=raw_message)

    def prepare_message(self, message_data, delivery_mode, **kwargs):
        content_type = kwargs.get("content_type")
        content_encoding = kwargs.get("content_encoding")
        persistent = True if delivery_mode == 2 else False
        return {"body": message_data, "persistent": persistent,
                "content_encoding": content_encoding,
                "content_type": content_type}

    def publish(self, message, exchange, routing_key, mandatory):
        message["destination"] = exchange
        self.channel.send(message)

    #def cancel(self, consumer_tag):
        
    def close(self):
        for queue in _queues.keys():
            self.channel.unsubscribe({"destination": queue})

    @property
    def channel(self):
        if not self._channel:
            self._channel = self.connection.connection
        return self._channel
