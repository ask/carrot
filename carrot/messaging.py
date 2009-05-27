"""

Creating a connection
---------------------

    If you're using Django you can use the
    :class:`carrot.connection.DjangoAMQPConnection` class, by setting the
    following variables in your ``settings.py``::

       AMQP_SERVER = "localhost"
       AMQP_PORT = 5672
       AMQP_USER = "test"
       AMQP_PASSWORD = "secret"
       AMQP_VHOST = "/test"

    Then create a connection by doing:

        >>> from carrot.connection import DjangoAMQPConnection
        >>> amqpconn = DjangoAMQPConnection()

    If you're not using Django, you can create a connection manually by using
    :class:`carrot.messaging.AMQPConnection`.

    >>> from carrot.connection import AMQPConnection
    >>> amqpconn = AMQPConnection(hostname="localhost", port=5672,
    ...                           userid="test", password="test",
    ...                           vhost="test")


Sending messages using a Publisher
----------------------------------

    >>> from carrot.messaging import Publisher
    >>> publisher = Publisher(connection=amqpconn,
    ...                       exchange="feed", routing_key="importer")
    >>> publisher.send({"import_feed": "http://cnn.com/rss/edition.rss"})
    >>> publisher.close()

Receiving messages using a Consumer
-----------------------------------

    >>> from carrot.messaging import Consumer
    >>> consumer = Consumer(connection=amqpconn, queue="feed",
                            exchange="feed", routing_key="importer")
    >>> def import_feed_callback(message_data, message)
    ...     feed_url = message_data.get("import_feed")
    ...     if not feed_url:
    ...         message.reject()
    ...     # something importing this feed url
    ...     # import_feed(feed_url)
    ...     message.ack()
    >>> consumer.register_callback(import_feed_callback)
    >>> consumer.wait() # Go into the consumer loop.


Subclassing the messaging classes
---------------------------------

The :class:`Consumer`, and :class:`Publisher` classes are also designed
for subclassing. Another way of defining the publisher and consumer is

    >>> from carrot.messaging import Publisher, Consumer
    >>> class FeedPublisher(Publisher):
    ...     exchange = "feed"
    ...     routing_key = "importer"
    ... 
    ...     def feed_import(feed_url):
    ...         return self.send({"action": "import_feed",
    ...                           "feed_url": feed_url})
    >>> class FeedConsumer(Consumer):
    ...     queue = "feed"
    ...     exchange = "feed"
    ...     routing_key = "importer"
    ...
    ...     def receive(self, message_data, message):
    ...         action = message_data.get("action")
    ...         if not action:
    ...             message.reject()
    ...         if action == "import_feed":
    ...             # something importing this feed
    ...             # import_feed(message_data["feed_url"])
    ...         else:
    ...             raise Exception("Unknown action: %s" % action)
    >>> publisher = FeedPublisher(connection=amqpconn)
    >>> publisher.import_feed("http://cnn.com/rss/edition.rss")
    >>> publisher.close()
    >>> consumer = FeedConsumer(connection=amqpconn)
    >>> consumer.wait() # Go into the consumer loop.

"""
from carrot.backends import DefaultBackend
from carrot.serialize import serialize, deserialize
import warnings



class Consumer(object):
    """Message consumer.

    :param connection: see :attr:`connection`.
    :param queue: see :attr:`queue`.
    :param exchange: see :attr:`exchange`.
    :param routing_key: see :attr:`routing_key`.

    :keyword durable: see :attr:`durable`.
    :keyword auto_delete: see :attr:`auto_delete`.
    :keyword exclusive: see :attr:`exclusive`.
    :keyword exchange_type: see :attr:`exchange_type`.
    :keyword decoder: see :attr:`decoder`.

    
    .. attribute:: connection

        A :class:`carrot.connection.AMQPConnection` instance.

    .. attribute:: queue

       Name of the queue.

    .. attribute:: exchange

        Name of the exchange the queue binds to.

    .. attribute:: routing_key

        The routing key (if any). The interpretation of the routing key
        depends on the value of the :attr:`exchange_type` attribute:

            * direct exchange

                Matches if the routing key property of the message and
                the :attr:`routing_key` attribute are identical.

            * fanout exchange

                Always matches, even if the binding does not have a key.

            * topic exchange

                Matches the routing key property of the message by a primitive
                pattern matching scheme. The message routing key then consists
                of words separated by dots (``"."``, like domain names), and
                two special characters are available; star (``"*"``) and hash
                (``"#"``). The star matches any word, and the hash matches
                zero or more words. For example ``"*.stock.#"`` matches the
                routing keys ``"usd.stock"`` and ``"eur.stock.db"`` but not
                ``"stock.nasdaq"``.
                

    .. attribute:: durable

        Durable exchanges remain active when a server restarts. Non-durable
        exchanges (transient exchanges) are purged when a server restarts.
        Default is ``True``.

    .. attribute:: auto_delete

        If set, the exchange is deleted when all queues have finished
        using it. Default is ``False``.

    .. attribute:: exclusive

        Exclusive queues may only be consumed from by the current connection.
        When :attr:`exclusive` is on, this also implies :attr:`auto_delete`.
        Default is ``False``.

    .. attribute:: exchange_type

        AMQP defines four default exchange types (routing algorithms) that
        covers most of the common messaging use cases. An AMQP broker can
        also define additional exchange types, so see your message brokers
        manual for more information about available exchange types.

            *direct

                Direct match between the routing key in the message, and the
                routing criteria used when a queue is bound to this exchange.

            *topic

                Wildcard match between the routing key and the routing pattern
                specified in the binding. The routing key is treated as zero
                or more words delimited by ``"."`` and supports special
                wildcard characters. ``"*"`` matches a single word and ``"#"``
                matches zero or more words.

            *fanout

                Queues are bound to this exchange with no arguments. Hence any
                message sent to this exchange will be forwarded to all queues
                bound to this exchange.

            *headers

                Queues are bound to this exchange with a table of arguments
                containing headers and values (optional). A special argument
                named "x-match" determines the matching algorithm, where
                ``"all"`` implies an ``AND`` (all pairs must match) and
                ``"any"`` implies ``OR`` (at least one pair must match).

                *NOTE*: carrot has poor support for header exchanges at
                    this point.

            This description of AMQP exchange types was shamelessly stolen
            from the blog post `AMQP in 10 minutes: Part 4`_ by
            Rajith Attapattu. Recommended reading.

            .. _`AMQP in 10 minutes: Part 4`: http://bit.ly/amqp-exchange-types

    .. attribute:: decoder

        A function able to deserialize the message body.

    .. attribute:: callbacks

        List of registered callbacks to trigger when a message is received
        by :meth:`wait`, :meth:`process_next` or :meth:`iterqueue`.

    :raises `amqplib.client_0_8.channel.AMQPChannelException`: if the queue is
        exclusive and the queue already exists and is owned by another
        connection.

    
    Example Usage

        >>> consumer = Consumer(connection=DjangoAMQPConnection(),
        ...                     queue="foo", exchange="foo", routing_key="foo") 
        >>> def process_message(message_data, message):
        ...     print("Got message %s: %s" % (
        ...             message.delivery_tag, message_data))
        >>> consumer.register_callback(process_message)
        >>> consumer.wait() # Go into receive loop
    
    """
    queue = None
    exchange = None
    routing_key = None
    durable = True
    exclusive = False
    auto_delete = False
    exchange_type = "direct"
    channel_open = False

    def __init__(self, connection, queue=None, exchange=None, routing_key=None,
            **kwargs):
        self.connection = connection
        self.backend = kwargs.get("backend")
        self.decoder = kwargs.get("decoder", deserialize)
        if not self.backend:
            self.backend = DefaeultBackend(connection=connection,
                                           decoder=self.decoder)
        self.queue = queue or self.queue

        # Binding.
        self.queue = queue or self.queue
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key

        self.durable = kwargs.get("durable", self.durable)
        self.exclusive = kwargs.get("exclusive", self.exclusive)
        self.auto_delete = kwargs.get("auto_delete", self.auto_delete)
        self.exchange_type = kwargs.get("exchange_type", self.exchange_type)
        self.callbacks = []
        self._build_channel()

        # durable implies auto-delete.
        if self.durable:
            self.auto_delete = True

    def _build_channel(self):
        if self.queue:
            self.backend.queue_declare(queue=self.queue, durable=self.durable,
                                       exclusive=self.exclusive,
                                       auto_delete=self.auto_delete)
        if self.exchange:
            self.backend.exchange_declare(exchange=self.exchange,
                                          type=self.exchange_type,
                                          durable=self.durable,
                                          auto_delete=self.auto_delete)
        if self.queue:
            self.backend.queue_bind(queue=self.queue, exchange=self.exchange,
                                    routing_key=self.routing_key)

    def _receive_callback(self, message):
        message_data = self.message_to_python(message)
        self.receive(message_data, message)

    def message_to_python(self, message):
        """Decode encoded message back to python.
       
        :param message: A :class:`carrot.backends.base.BaseMessage` instance.

        """
        return self.decoder(message.body)

    def fetch(self):
        """Receive the next message waiting on the queue.

        :returns: A :class:`carrot.backends.base.BaseMessage` instance,
            or ``None`` if there's no messages to be received.

        """
        message = self.backend.get(self.queue)
        return message

    def receive(self, message_data, message):
        """This method is called when a new message is received by 
        running :meth:`wait`, :meth:`process_next` or :meth:`iterqueue`.

        When a message is received, it passes the message on to the
        callbacks listed in the :attr:`callbacks` attribute.
        You can register callbacks using :meth:`register_callback`.

        :param message_data: The deserialized message data.

        :param message: The :class:`carrot.backends.base.BaseMessage` instance.
        
        :raises NotImplementedError: If no callbacks has been registered.

        """
        if not self.callbacks:
            raise NotImplementError("No consumer callbacks registered")
        for callback in self.callbacks:
            callback(message_data, message)

    def register_callback(self, callback):
        """Register a callback function to be triggered by :meth:`receive`.

        The ``callback`` function must take two arguments:

            * message_data

                The deserialized message data

            * message

                The :class:`carrot.backends.base.BaseMessage` instance.
        """
        self.callbacks.append(callback)

    def process_next(self, ack=True):
        """Processes the next pending message on the queue.

        This function tries to fetch a message from the queue, and
        if successful passes the message on to :meth:`receive`.

        :keyword ack: By default, an ack is sent to the server
            signifying that the message has been accepted. This means
            that the :meth:`carrot.backends.base.BaseMessage.ack` and
            :meth:`carrot.backends.base.BaseMessage.reject` methods
            on the message object are no longer valid.
            If the ack argument is set to ``False``, this behaviour is
            disabled and the receiver is required to manually handle
            acknowledgment.

        :returns: The resulting :class:`carrot.backends.base.BaseMessage` object.

        """
        message = self.fetch()
        if message:
            self._receive_callback(message)
            if ack:
                message.ack()
        return message

    def discard_all(self, filter=None):
        """Discard all waiting messages.

        :param filter: A filter function to only discard the messages this
            filter returns.

        :returns: the number of messages discarded.

        *WARNING*: All incoming messages will be ignored and not processed.

        Example using filter:

            >>> def waiting_feeds_only(message):
            ...     try:
            ...         message_data = simplejson.loads(message.body)
            ...     except: # Should probably be more specific.
            ...         pass
            ...
            ...     if message_data.get("type") == "feed":
            ...         return True
            ...     else:
            ...         return False
        """
        discarded_count = 0
        while True:
            message = self.fetch()
            if message is None:
                return discarded_count

            discard_message = True
            if filter and not filter(message):
                discard_message = False

            if discard_message:
                message.ack()
                discarded_count = discarded_count + 1

    def next(self):
        """*DEPRECATED*: Process the next pending message.
        Deprecated in favour of :meth:`process_next`"""
        warnings.warn("next() is deprecated, use process_next() instead.",
                DeprecationWarning)
        return self.process_next()

    def wait(self):
        """Go into consume mode.

        This runs an infinite loop, processing all incoming messages
        using :meth:`receive` to apply the message to all registered
        callbacks.

        """
        self.channel_open = True
        self.backend.consume(queue=self.queue, no_ack=True,
                             callback=self._receive_callback,
                             consumer_tag=self.__class__.__name__)

    def iterqueue(self, limit=None):
        """Infinite iterator yielding pending messages.

        Obviously you shouldn't consume the whole iterator at
        once, without using a ``limit``.

        :keyword limit: If set, the iterator stops when it has processed
            this number of messages in total.
        """
        for items_since_start in itertools.count():
            item = self.process_next()
            if item is None or (limit and items_since_start > limit):
                raise StopIteration
            yield item

    def close(self):
        """Close the channel to the queue.
        
        Any operation that requires a connection will re-establish the
        connection even if close was called explicitly.  """
        if self.channel_open:
            self.backend.cancel(self.__class__.__name__)
        self.backend.close()


class Publisher(object):
    """Message publisher.

    :param connection: see :attr:`connection`.

    :param exchange: see :attr:`exchange`.

    :param routing_key: see :attr:`routing_key`.

    :param encoder: see :attr:`encoder`.


    .. attribute:: connection

        The AMQP connection. A :class:`carrot.connection.AMQPConnection`
        instance.

    .. attribute:: exchange

        Name of the exchange we send messages to.

    .. attribute:: routing_key

        The routing key added to all messages sent using this publisher.
        See :attr:`Consumer.routing_key` for more information.

    .. attribute:: delivery_mode

        The default delivery mode used for messages. The value is an integer.
        The following delivery modes are supported by (at least) RabbitMQ:

            * 1

                The message is non-persistent. Which means it is stored in
                memory only, and is lost if the server dies or restarts.

            * 2
                The message is persistent. Which means the message is
                stored both in-memory, and on disk, and therefore
                preserved if the server dies or restarts.

        The default value is ``2`` (persistent).

    .. attribute:: encoder

        The function responsible for encoding the message data passed
        to :meth:`send`. Note that any consumer of the messages sent
        must have a decoder supporting the serialization scheme.

    """

    exchange = ""
    routing_key = ""
    delivery_mode = 2 # Persistent

    def __init__(self, connection, exchange=None, routing_key=None, **kwargs):
        self.connection = connection
        self.backend = kwargs.get("backend")
        self.encoder = kwargs.get("encoder", serialize)
        if not self.backend:
            self.backend = DefaultBackend(connection=connection,
                                          encoder=self.encoder)
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.delivery_mode = kwargs.get("delivery_mode", self.delivery_mode)

    def create_message(self, message_data):
        """With any data, serialize it and encapsulate it in a AMQP
        message with the proper headers set."""
        message_data = self.encoder(message_data)
        return self.backend.prepare_message(message_data, self.delivery_mode)

    def send(self, message_data, delivery_mode=None):
        """Send a message.
       
        :param message_data: The message data to send. Can be a list,
            dictionary or a string.

        :keyword delivery_mode: Override the default :attr:`delivery_mode`.

        """
        message = self.create_message(message_data)
        self.backend.publish(message, exchange=self.exchange,
                                      routing_key=self.routing_key)

    def close(self):
        """Close connection to queue.
        
        *Note* Whenever :meth:`send` is called, the connection is
        re-established, even if :meth:`close` was called explicitly.
        
        """
        self.backend.close()


class Messaging(object):
    """A message publisher and consumer."""
    queue = ""
    exchange = ""
    routing_key = ""
    publisher_cls = Publisher
    consumer_cls = Consumer

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.backend = kwargs.get("backend")
        self.exchange = kwargs.get("exchange", self.exchange)
        self.queue = kwargs.get("queue", self.queue)
        self.routing_key = kwargs.get("routing_key", self.routing_key)
        self.publisher = self.publisher_cls(connection,
                exchange=self.exchange, routing_key=self.routing_key,
                backend=self.backend)
        self.consumer = self.consumer_cls(connection, queue=self.queue,
                exchange=self.exchange, routing_key=self.routing_key,
                backend=self.backend)
        self.consumer.register_callback(self.receive)
        self.callbacks = []

    def register_callback(self, callback):
        self.callbacks.append(callback)

    def receive(self, message_data, message):
        if not self.callbacks:
            raise NotImplementError("No consumer callbacks registered")
        for callback in self.callbacks:
            callback(message_data, message)

    def send(self, message_data, delivery_mode=None):
        self.publisher.send(message_data, delivery_mode=delivery_mode)

    def fetch(self):
        return self.consumer.fetch()

    def next(self):
        return self.consumer.next()

    def fetch(self):
        return self.consumer.fetch()

    def close(self):
        self.consumer.close()
        self.publisher.close()

    @property
    def encoder(self):
        return self.publisher.encoder

    @property
    def decoder(self):
        return self.consumer.decoder
