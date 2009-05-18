from amqplib import client_0_8 as amqp
import warnings

# Try to import a module that provides json parsing and emitting, starting
# with the fastest alternative and falling back to the slower ones.
try:
    # cjson is the fastest
    import cjson
    serialize = cjson.encode
    deserialize = cjson.decode
except ImportError:
    try:
        # Then try to find simplejson. Later versions has C speedups which
        # makes it pretty fast.
        import simplejson
        serialize = simplejson.dumps
        deserialize = simplejson.loads
    except ImportError:
        try:
            # Then try to find the python 2.6 stdlib json module.
            import json
            serialize = json.dumps
            deserialize = json.loads
        except ImportError:
            # If all of the above fails, fallback to the simplejson
            # embedded in Django.
            from django.utils import simplejson
            serialize = simplejson.dumps
            deserialize = simplejson.loads


class Message(object):
    """Wrapper around amqplib.client_0_8.Message."""
    def __init__(self, amqp_message, channel):
        self.amqp_message = amqp_message
        self.channel = channel

    def ack(self):
        """Acknowledge this message as being processed.,

        This will remove the message from the queue."""
        return self.channel.basic_ack(self.delivery_tag)

    def reject(self):
        """Reject this message.
        The message will then be discarded by the server.
        """
        return self.channel.basic_reject(self.delivery_tag, requeue=False)

    def requeue(self):
        """Reject this message and put it back on the queue.

        You must not use this method as a means of selecting messages
        to process."""
        return self.channel.basic_reject(self.delivery_tag, requeue=True)

    @property
    def body(self):
        return self.amqp_message.body

    @property
    def delivery_tag(self):
        return self.amqp_message.delivery_tag


class Consumer(object):
    """Superclass for all consumers. Subclasses need to implement the
    receive method"""
    queue = ""
    exchange = ""
    routing_key = ""
    durable = True
    exclusive = False
    auto_delete = False
    exchange_type = "direct"
    channel_open = False

    def __init__(self, connection, queue=None, exchange=None, routing_key=None,
            **kwargs):
        self.connection = connection
        self.queue = queue or self.queue

        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.decoder = kwargs.get("decoder", deserialize)
        self.durable = kwargs.get("durable", self.durable)
        self.exclusive = kwargs.get("exclusive", self.exclusive)
        self.auto_delete = kwargs.get("auto_delete", self.auto_delete)
        self.exchange_type = kwargs.get("exchange_type", self.exchange_type)
        self.channel = self._build_channel()

    def _build_channel(self):
        channel = self.connection.connection.channel()
        if self.queue:
            channel.queue_declare(queue=self.queue, durable=self.durable,
                                  exclusive=self.exclusive,
                                  auto_delete=self.auto_delete)
        if self.exchange:
            channel.exchange_declare(exchange=self.exchange,
                                     type=self.exchange_type,
                                     durable=self.durable,
                                     auto_delete=self.auto_delete)
        if self.queue:
            channel.queue_bind(queue=self.queue, exchange=self.exchange,
                               routing_key=self.routing_key)
        return channel

    def _receive_callback(self, message):
        message_data = self.decoder(message.body)
        self.receive(message_data, message)

    def fetch(self):
        if not self.channel.connection:
            self.channel = self._build_channel()
        raw_message = self.channel.basic_get(self.queue)
        if not raw_message:
            return None
        return Message(raw_message, channel=self.channel)

    def receive(self, message_data, message):
        """Called when a new message is received. Must be implemented in
        subclasses"""
        raise NotImplementedError(
                "Consumers must implement the receive method")

    def process_next(self, ack=True):
        """Returns a pending message from the queue.
        By default, an ack is sent to the server signifying that the
        message has been accepted. This means that the ack and reject
        methods on the message object are no longer valid. If the ack argument
        is set to False, this behaviour is disabled and applications are
        required to handle ack themselves."""
        message = self.fetch()
        if message:
            self._receive_callback(message)
            if ack:
                message.ack()
        return message

    def discard_all(self):
        """Discard all waiting messages.

        Returns the number of messages discarded.
        *WARNING*: All incoming messages will be ignored and not processed.
        """
        discarded_count = 0
        while True:
            message = self.fetch()
            if message is None:
                return discarded_count
            message.ack()
            discarded_count = discarded_count + 1

    def next(self):
        """DEPRECATED: Return the next pending message. Deprecated in favour
        of process_next"""
        warnings.warn("next() is deprecated, use process_next() instead.",
                DeprecationWarning)
        return self.process_next()

    def wait(self):
        if not self.channel.connection:
            self.channel = self._build_channel()
        self.channel_open = True
        self.channel.basic_consume(queue=self.queue, no_ack=True,
                callback=self._receive_callback,
                consumer_tag=self.__class__.__name__)
        while True:
            self.channel.wait()

    def iterqueue(self, limit=None):
        """Iterator that yields all pending messages, at most limit
        messages. If limit is not set, return all messages"""
        for items_since_start in itertools.count():
            item = self.next()
            if item is None or (limit and items_since_start > limit):
                raise StopIteration
            yield item

    def close(self):
        """Close the connection to the queue. Any operation that requires
        a connection will re-establish the connection even if close was
        called explicitly."""
        if self.channel_open:
            self.channel.basic_cancel(self.__class__.__name__)
        if getattr(self, "channel") and self.channel.is_open:
            self.channel.close()


class Publisher(object):
    """Superclass for Publishers. Subclasses are not required to implement
    any methods"""
    exchange = ""
    routing_key = ""
    delivery_mode = 2 # Persistent

    def __init__(self, connection, exchange=None, routing_key=None, **kwargs):
        self.connection = connection
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.encoder = kwargs.get("encoder", serialize)
        self.delivery_mode = kwargs.get("delivery_mode", self.delivery_mode)
        self.channel = self._build_channel()

    def _build_channel(self):
        return self.connection.connection.channel()

    def create_message(self, message_data):
        message_data = self.encoder(message_data)
        message = amqp.Message(message_data)
        message.properties["delivery_mode"] = self.delivery_mode
        return message

    def send(self, message_data, delivery_mode=None):
        """Send message data to queue"""
        message = self.create_message(message_data)

        # Recreate channel if connection lost.
        if not self.channel.connection:
            self.channel = self._build_channel()

        self.channel.basic_publish(message, exchange=self.exchange,
                                              routing_key=self.routing_key)
    def close(self):
        """Close connection to queue. Note, whenever send() is called, the
        connection is re-established, even if close() was called explicitly."""
        if getattr(self, "channel") and self.channel.is_open:
            self.channel.close()


class Messaging(object):
    """Superclass for clases that are able to both receive and send messages"""
    queue = ""
    exchange = ""
    routing_key = ""
    publisher_cls = Publisher
    consumer_cls = Consumer

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.exchange = kwargs.get("exchange", self.exchange)
        self.queue = kwargs.get("queue", self.queue)
        self.routing_key = kwargs.get("routing_key", self.routing_key)
        self.publisher = self.publisher_cls(connection,
                exchange=self.exchange, routing_key=self.routing_key)
        self.consumer = self.consumer_cls(connection, queue=self.queue,
                exchange=self.exchange, routing_key=self.routing_key)
        self.consumer.receive = self._receive_callback

    def _receive_callback(self, message_data, message):
        self.receive(message_data, message)

    def send(self, message_data, delivery_mode=None):
        self.publisher.send(message_data, delivery_mode=delivery_mode)

    def receive(self, message_data, message):
        raise NotImplementedError(
                "Messaging classes must implement the receive method")

    def next(self):
        return self.consumer.next()

    def close(self):
        self.consumer.close()
        self.publisher.close()
