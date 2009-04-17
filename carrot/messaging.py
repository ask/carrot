from amqplib import client_0_8 as amqp

try:
    # cjson is the fastest
    import cjson
    serialize = cjson.encode
    deserialize = cjson.decode
except ImportError:
    try:
        # Then try to find the latest version of simplejson.
        # Later versions has C speedups which makes it pretty fast.
        import simplejson
        serialize = simplejson.dumps
        deserialize = simplejson.loads
    except ImportError:
        # If all of the above fails, fallback to the simplejson
        # embedded in Django.
        from django.utils import simplejson
        serialize = simplejson.dumps
        deserialize = simplejson.loads


class Consumer(object):
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
        self.connection = connection()
        self.queue = queue or self.queue

        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.decoder = kwargs.get("decoder", deserialize)
        self.durable = kwargs.get("durable", self.durable)
        self.exclusive = kwargs.get("exclusive", self.exclusive)
        self.auto_delete = kwargs.get("auto_delete", self.auto_delete)
        self.exchange_type = kwargs.get("exchange_type", self.exchange_type)
        self.channel = self.build_channel()

    def build_channel(self):
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

    def receive_callback(self, message):
        message_data = self.decoder(message.body)
        self.receive(message_data, message)

    def receive(self, message_data, message):
        raise NotImplementedError(
                "Consumers must implement the receive method")

    def process_next(self):
        if not self.channel.connection:
            self.channel = self.build_channel()
        message = self.channel.basic_get(self.queue)
        if message:
            self.receive_callback(message)
            self.channel.basic_ack(message.delivery_tag)
        return message

    def next(self):
        raise DeprecationWarning(
                "next() is deprecated, use process_next() instead.")
        return self.process_next()

    def wait(self):
        if not self.channel.connection:
            self.channel = self.build_channel()
        self.channel_open = True
        self.channel.basic_consume(queue=self.queue, no_ack=True,
                callback=self.receive_callback,
                consumer_tag=self.__class__.__name__)
        yield self.channel.wait()

    def iterqueue(self, limit=None):
        for items_since_start in itertools.count():
            item = self.next()
            if item is None or (limit and items_since_start > limit):
                raise StopIteration
            yield item

    def close(self):
        if self.channel_open:
            self.channel.basic_cancel(self.__class__.__name__)
        if getattr(self, "channel") and self.channel.is_open:
            self.channel.close()


class Publisher(object):
    exchange = ""
    routing_key = ""
    delivery_mode = 2 # Persistent

    def __init__(self, connection, exchange=None, routing_key=None, **kwargs):
        self.connection = connection()
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.encoder = kwargs.get("encoder", serialize)
        self.delivery_mode = kwargs.get("delivery_mode", self.delivery_mode)
        self.channel = self.build_channel()

    def build_channel(self):
        return self.connection.connection.channel()

    def create_message(self, message_data):
        # Recreate channel if connection lost.
        if not self.channel.connection:
            self.channel = self.build_channel()

        message_data = self.encoder(message_data)
        message = amqp.Message(message_data)
        message.properties["delivery_mode"] = self.delivery_mode
        return message

    def send(self, message_data, delivery_mode=None):
        message = self.create_message(message_data)
        self.channel.basic_publish(message, exchange=self.exchange,
                                              routing_key=self.routing_key)

    def close(self):
        if getattr(self, "channel") and self.channel.is_open:
            self.channel.close()


class Messaging(object):
    queue = ""
    exchange = ""
    routing_key = ""
    publisher_cls = Publisher
    consumer_cls = Consumer

    def __init__(self, connection_cls, **kwargs):
        self.connection_cls = connection_cls
        self.exchange = kwargs.get("exchange", self.exchange)
        self.queue = kwargs.get("queue", self.queue)
        self.routing_key = kwargs.get("routing_key", self.routing_key)
        self.publisher = self.publisher_cls(connection_cls,
                exchange=self.exchange, routing_key=self.routing_key)
        self.consumer = self.consumer_cls(connection_cls, queue=self.queue,
                exchange=self.exchange, routing_key=self.routing_key)
        self.consumer.receive = self.receive_callback

    def send(self, message_data, delivery_mode=None):
        self.publisher.send(message_data, delivery_mode=delivery_mode)

    def receive_callback(self, message_data, message):
        self.receive(message_data, message)

    def receive(self, message_data, message):
        raise NotImplementedError(
                "Messaging classes must implement the receive method")

    def next(self):
        return self.consumer.next()

    def close(self):
        self.consumer.close()
        self.publisher.close()
