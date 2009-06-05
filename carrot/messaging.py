"""carrot.messaging"""
from carrot.backends import DefaultBackend
from carrot.serialization import serialize, deserialize
import itertools
import warnings
import uuid


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
    :keyword backend_cls: see :attr:`backend_cls`.
    :keyword auto_ack: see :attr:`auto_ack`.
    :keyword no_ack: see :attr:`no_ack`.


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

            * Direct

                Direct match between the routing key in the message, and the
                routing criteria used when a queue is bound to this exchange.

            * Topic

                Wildcard match between the routing key and the routing pattern
                specified in the binding. The routing key is treated as zero
                or more words delimited by ``"."`` and supports special
                wildcard characters. ``"*"`` matches a single word and ``"#"``
                matches zero or more words.

            * Fanout

                Queues are bound to this exchange with no arguments. Hence any
                message sent to this exchange will be forwarded to all queues
                bound to this exchange.

            * Headers

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

    .. attribute:: backend_cls

        The messaging backend class used. Defaults to the ``pyamqplib``
        backend.

    .. attribute:: callbacks

        List of registered callbacks to trigger when a message is received
        by :meth:`wait`, :meth:`process_next` or :meth:`iterqueue`.

    .. attribute:: warn_if_exists

        Emit a warning if the queue has already been declared. If a queue
        already exists, and you try to redeclare the queue with new settings,
        the new settings will be silently ignored, so this can be
        useful if you've recently changed the :attr:`routing_key` attribute
        or other settings.

    .. attribute:: auto_ack

        Acknowledgement is handled automatically once messages are received.
        This means that the :meth:`carrot.backends.base.BaseMessage.ack` and
        :meth:`carrot.backends.base.BaseMessage.reject` methods
        on the message object are no longer valid.
        By default :attr:`auto_ack` is set to ``False``, and the receiver is
        required to manually handle acknowledgment.

    .. attribute:: no_ack

        Disable acknowledgement on the server-side. This is different from
        :attr:`auto_ack` in that acknowledgement is turned off altogether.
        This functionality increases performance but at the cost of
        reliability. Messages can get lost if a client dies before it can
        deliver them to the application.


    :raises `amqplib.client_0_8.channel.AMQPChannelException`: if the queue is
        exclusive and the queue already exists and is owned by another
        connection.


    Example Usage

        >>> consumer = Consumer(connection=DjangoAMQPConnection(),
        ...               queue="foo", exchange="foo", routing_key="foo")
        >>> def process_message(message_data, message):
        ...     print("Got message %s: %s" % (
        ...             message.delivery_tag, message_data))
        >>> consumer.register_callback(process_message)
        >>> consumer.wait() # Go into receive loop

    """
    queue = ""
    exchange = ""
    routing_key = ""
    durable = True
    exclusive = False
    auto_delete = False
    exchange_type = "direct"
    channel_open = False
    warn_if_exists = False
    backend_cls = DefaultBackend
    auto_ack = False
    no_ack = False
    _closed = True

    def __init__(self, connection, queue=None, exchange=None,
            routing_key=None, **kwargs):
        self.connection = connection
        self.decoder = kwargs.get("decoder", deserialize)
        self.backend_cls = kwargs.get("backend_cls", self.backend_cls)
        self.backend = self.backend_cls(connection=connection,
                                        decoder=self.decoder)
        self.queue = queue or self.queue

        # Binding.
        self.queue = queue or self.queue
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.callbacks = []

        # Options
        self.durable = kwargs.get("durable", self.durable)
        self.exclusive = kwargs.get("exclusive", self.exclusive)
        self.auto_delete = kwargs.get("auto_delete", self.auto_delete)
        self.exchange_type = kwargs.get("exchange_type", self.exchange_type)

        self.warn_if_exists = kwargs.get("warn_if_exists",
                                         self.warn_if_exists)
        self.auto_ack = kwargs.get("auto_ack", self.auto_ack)

        # exclusive implies auto-delete.
        if self.exclusive:
            self.auto_delete = True

        self.consumer_tag = self._generate_consumer_tag()
        self._declare_channel(self.queue, self.routing_key)

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def _generate_consumer_tag(self):
        return "%s.%s-%s" % (
                self.__class__.__module__,
                self.__class__.__name__,
                str(uuid.uuid4()))

    def _declare_channel(self, queue_name, routing_key):
        if self.queue:
            self.backend.queue_declare(queue=queue_name, durable=self.durable,
                                       exclusive=self.exclusive,
                                       auto_delete=self.auto_delete,
                                       warn_if_exists=self.warn_if_exists)
        if self.exchange:
            self.backend.exchange_declare(exchange=self.exchange,
                                          type=self.exchange_type,
                                          durable=self.durable,
                                          auto_delete=self.auto_delete)
        if self.queue:
            self.backend.queue_bind(queue=queue_name, exchange=self.exchange,
                                    routing_key=routing_key)
        self._closed = False

    def _receive_callback(self, raw_message):
        message = self.backend.message_to_python(raw_message)
        if self.auto_ack:
            message.ack()
        self.receive(message.decode(), message)

    def fetch(self, no_ack=None, auto_ack=None, enable_callbacks=False):
        """Receive the next message waiting on the queue.

        :returns: A :class:`carrot.backends.base.BaseMessage` instance,
            or ``None`` if there's no messages to be received.

        :keyword enable_callbacks: Enable callbacks. The message will be
            processed with all registered callbacks. Default is disabled.
        :keyword auto_ack: Override the default :attr:`auto_ack` setting.
        :keyword no_ack: Override the default :attr:`no_ack` setting.

        """
        no_ack = no_ack or self.no_ack
        auto_ack = auto_ack or self.auto_ack
        message = self.backend.get(self.queue, no_ack=no_ack)
        if message:
            if auto_ack:
                message.ack()
            if enable_callbacks:
                self._receive_callback(message)
        return message

    def process_next(self):
        """**DEPRECATED** Use :meth:`fetch` like this instead:

            >>> message = self.fetch(enable_callbacks=True)

        """
        warnings.warn(DeprecationWarning(
            "Consumer.process_next has been deprecated in favor of \
            Consumer.fetch(enable_callbacks=True)"))
        return self.fetch(enable_callbacks=True)

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
            raise NotImplementedError("No consumer callbacks registered")
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

    def discard_all(self, filter=None):
        """Discard all waiting messages.

        :param filter: A filter function to only discard the messages this
            filter returns.

        :returns: the number of messages discarded.

        *WARNING*: All incoming messages will be ignored and not processed.

        Example using filter:

            >>> def waiting_feeds_only(message):
            ...     try:
            ...         message_data = message.decode()
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

    def iterconsume(self, limit=None):
        """Iterator processing new messages as they arrive.
        Every new message will be passed to the callbacks, and the iterator
        returns ``True``. The iterator is infinite unless the ``limit``
        argument is specified or someone closes the consumer.

        :meth:`iterconsume` uses transient requests for messages on the
        server, while :meth:`iterequeue` uses synchronous access. In most
        cases you want :meth:`iterconsume`, but if your environment does not
        support this behaviour you can resort to using :meth:`iterqueue`
        instead.

        Also, :meth:`iterconsume` does not return the message
        at each step, something which :meth:`iterqueue` does.

        :keyword limit: Maximum number of messages to process.

        :raises StopIteration: if limit is set and the message limit has been
        reached.

        """
        self.channel_open = True
        return self.backend.consume(queue=self.queue, no_ack=True,
                                    callback=self._receive_callback,
                                    consumer_tag=self.consumer_tag,
                                    limit=limit)

    def wait(self, limit=None):
        """Go into consume mode.

        Mostly for testing purposes and simple programs, you probably
        want :meth:`iterconsume` or :meth:`iterqueue` instead.

        This runs an infinite loop, processing all incoming messages
        using :meth:`receive` to apply the message to all registered
        callbacks.

        """
        it = self.iterconsume(limit)
        while True:
            it.next()

    def iterqueue(self, limit=None, infinite=False):
        """Infinite iterator yielding pending messages, by using
        synchronous direct access to the queue (``basic_get``).

        :meth:`iterqueue` is used where synchronous functionality is more
        important than performance. If you can, use :meth:`iterconsume`
        instead.

        :keyword limit: If set, the iterator stops when it has processed
            this number of messages in total.

        :keyword infinite: Don't raise :exc:`StopIteration` if there is no
            messages waiting, but return ``None`` instead. If infinite you
            obviously shouldn't consume the whole iterator at once without
            using a ``limit``.

        :raises StopIteration: If there is no messages waiting, and the
            iterator is not infinite.

        """
        for items_since_start in itertools.count():
            item = self.fetch()
            if (not infinite and item is None) or \
                    (limit and items_since_start >= limit):
                raise StopIteration
            yield item

    def close(self):
        """Close the channel to the queue."""
        if self.channel_open:
            try:
                self.backend.cancel(self.consumer_tag)
            except KeyError:
                pass
        self.backend.close()
        self._closed = True


class Publisher(object):
    """Message publisher.

    :param connection: see :attr:`connection`.
    :param exchange: see :attr:`exchange`.
    :param routing_key: see :attr:`routing_key`.

    :keyword encoder: see :attr:`encoder`.
    :keyword backend_cls: see :attr:`backend_cls`.


    .. attribute:: connection

        The AMQP connection. A :class:`carrot.connection.AMQPConnection`
        instance.

    .. attribute:: exchange

        Name of the exchange we send messages to.

    .. attribute:: routing_key

        The default routing key for messages sent using this publisher.
        See :attr:`Consumer.routing_key` for more information.
        You can override the routing key by passing an explicit
        ``routing_key`` argument to :meth:`send`.

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

    .. attribute:: backend_cls

        The messaging backend class used. Defaults to the ``pyamqplib``
        backend.

    """

    exchange = ""
    routing_key = ""
    delivery_mode = 2 # Persistent
    backend_cls = DefaultBackend
    _closed = True

    def __init__(self, connection, exchange=None, routing_key=None, **kwargs):
        self.connection = connection
        self.encoder = kwargs.get("encoder", serialize)
        self.backend_cls = kwargs.get("backend_cls", self.backend_cls)
        self.backend = self.backend_cls(connection=connection,
                                        encoder=self.encoder)
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.delivery_mode = kwargs.get("delivery_mode", self.delivery_mode)
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def create_message(self, message_data, priority=None):
        """With any data, serialize it and encapsulate it in a AMQP
        message with the proper headers set."""
        message_data = self.encoder(message_data)
        return self.backend.prepare_message(message_data, self.delivery_mode,
                                            priority=priority)

    def send(self, message_data, routing_key=None, delivery_mode=None,
            mandatory=False, immediate=False, priority=0):
        """Send a message.

        :param message_data: The message data to send. Can be a list,
            dictionary or a string.

        :keyword routing_key: A custom routing key for the message.
            If not set, the default routing key set in the :attr:`routing_key`
            attribute is used.

        :keyword mandatory: If set, the message has mandatory routing.
            By default the message is silently dropped by the server if it
            can't be routed to a queue. However - If the message is mandatory,
            an exception will be raised instead.

        :keyword immediate: Request immediate delivery.
            If the message cannot be routed to a queue consumer immediately,
            an exception will be raised. This is instead of the default
            behaviour, where the server will accept and queue the message,
            but with no guarantee that the message will ever be consumed.

        :keyword delivery_mode: Override the default :attr:`delivery_mode`.

        :keyword priority: The message priority, ``0`` to ``9``.

        """
        if not routing_key:
            routing_key = self.routing_key
        message = self.create_message(message_data, priority=priority)
        self.backend.publish(message,
                             exchange=self.exchange, routing_key=routing_key,
                             mandatory=mandatory, immediate=immediate)

    def close(self):
        """Close connection to queue."""
        self.backend.close()
        self._closed = True


class Messaging(object):
    """A combined message publisher and consumer."""
    queue = ""
    exchange = ""
    routing_key = ""
    publisher_cls = Publisher
    consumer_cls = Consumer
    _closed = True

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.backend_cls = kwargs.get("backend_cls")
        self.exchange = kwargs.get("exchange", self.exchange)
        self.queue = kwargs.get("queue", self.queue)
        self.routing_key = kwargs.get("routing_key", self.routing_key)
        self.publisher = self.publisher_cls(connection,
                exchange=self.exchange, routing_key=self.routing_key,
                backend_cls=self.backend_cls)
        self.consumer = self.consumer_cls(connection, queue=self.queue,
                exchange=self.exchange, routing_key=self.routing_key,
                backend_cls=self.backend_cls)
        self.consumer.register_callback(self.receive)
        self.callbacks = []
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def register_callback(self, callback):
        self.callbacks.append(callback)

    def receive(self, message_data, message):
        if not self.callbacks:
            raise NotImplementedError("No consumer callbacks registered")
        for callback in self.callbacks:
            callback(message_data, message)

    def send(self, message_data, delivery_mode=None):
        self.publisher.send(message_data, delivery_mode=delivery_mode)

    def fetch(self, **kwargs):
        return self.consumer.fetch(**kwargs)

    def close(self):
        self.consumer.close()
        self.publisher.close()
        self._closed = True

    @property
    def encoder(self):
        return self.publisher.encoder

    @property
    def decoder(self):
        return self.consumer.decoder
