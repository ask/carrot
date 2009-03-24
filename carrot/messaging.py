from amqplib import client_0_8 as amqp
from django.conf import settings
import sys

try:
    # cjson is the fastest
    import cjson
    serialize = cjson.encode
    deserialize = cjson.decode
except ImportError:
    try:
        # Then try to find the lastest version of simplejson.
        # Later versions has C speedups which is pretty fast.
        import simplejson
        serialize = simplejson.dumps
        deserialize = simplejson.loads
    except ImportError:
        # If all of the above fails, fallback to the simplejson
        # embedded in Django.
        from django.utils import simplejson
        serialize = simplejson.dumps
        deserailize = simplejson.loads


class DjangoAMQPConnection(object):
    hostname = getattr(settings, "AMQP_SERVER", None)
    port = getattr(settings, "AMQP_PORT", 5672)
    userid = getattr(settings, "AMQP_USER", None)
    password = getattr(settings, "AMQP_PASSWORD", None)
    virtual_host = getattr(settings, "AMQP_VHOST", "/")
    insist = False

    @property
    def host(self):
        return ":".join([self.hostname, str(self.port)])

    def __init__(self, hostname=None, port=None, userid=None, password=None,
            virtual_host=None, **kwargs):
        self.hostname = hostname or self.hostname
        self.port = port or self.port
        self.userid = userid or self.userid
        self.password = password or self.password
        self.virtual_host = virtual_host or self.virtual_host
        self.insist = kwargs.get("insist", self.insist)

        self.connect()

    def connect(self):
        self.connection = amqp.Connection(host=self.host,
                                          userid=self.userid,
                                          password=self.password,
                                          virtual_host=self.virtual_host,
                                          insist=self.insist)
        self.channel = self.connection.channel()

    def close(self):
        self.channel.close()
        self.connection.close()

    def __del__(self):
        self.close()


class Consumer(object):
    connection_cls = DjangoAMQPConnection
    decoder = deserialize
    durable = True
    exclusive = False
    auto_delete = False
    exchange_type = "direct"

    def __init__(self, queue=None, exchange=None, routing_key=None, connection=None,
            **kwargs):
        self.queue = queue or self.queue
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.connection_cls = kwargs.get("connection_cls",
                self.connection_cls)
        self.decoder = kwargs.get("decoder", self.decoder)
        self.durable = kwargs.get("durable", self.durable)
        self.exclusive = kwargs.get("exclusive", self.exclusive)
        self.auto_delete = kwargs.get("auto_delete", self.auto_delete)
        self.exchange_type = kwargs.get("exchange_type", self.exchange_type)
        self.connection = connection or self.connection_cls()
        channel = self.connection.channel

        channel.queue_declare(queue=self.queue, durable=self.durable,
                              exclusive=self.exclusive,
                              auto_delete=self.auto_delete)
        channel.exchange_declare(exchange=self.exchange,
                                 type=self.exchange_type,
                                 durable=self.durable,
                                 auto_delete=self.auto_delete)
        channel.queue_bind(queue=self.queue, exchange=self.exchange,
                           routing_key=self.routing_key)


    def receive_callback(self, message):
        message_data = self.decoder(message.body)
        self.receive(message_data, message)

    def receive(self, message_data, message):
        raise NotImplementedError(
                "Consumers must implement the receive method")

    def next(self):
        message = self.connection.channel.basic_get(self.queue)
        if message:
            self.receive_callback(message)
            self.connection.channel.basic_ack(message.delivery_tag)
   
    def wait(self):
        channel.basic_consume(queue=self.queue, no_ack=True,
                callback=self.receive_callback,
                consumer_tag=self.__class__.__name__)
        while True:
            self.connection.channel.wait()
       
    def __del__(self):
        self.connection.channel.basic_cancel(self.__class__.__name__)
    

class Publisher(object):
    exchange = None
    routing_key = None
    connection_cls = DjangoAMQPConnection
    delivery_mode = 2 # Persistent
    encoder = serialize

    def __init__(self, exchange=None, routing_key=None, connection=None,
            **kwargs):
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.connection_cls = kwargs.get("connection_cls",
                self.connection_cls)
        self.encoder = kwargs.get("encoder", self.encoder)
        self.delivery_mode = kwargs.get("delivery_mode", self.delivery_mode)
        self.connection = connection or self.connection_cls()

    def create_message(self, message_data):
        message_data = self.encoder(message_data)
        message = amqp.Message(message_data)
        message.properties["delivery_mode"] = self.delivery_mode
        return message

    def send(self, message_data, delivery_mode=None):
        message = self.create_message(message_data)
        self.connection.channel.basic_publish(message, exchange=self.exchange,
                                              routing_key=self.routing_key)
