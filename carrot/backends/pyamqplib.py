from amqplib import client_0_8 as amqp
from carrot.backends import BaseMessage, BaseBackend
from carrot.serialize import serialize, deserialize

class Message(BaseMessage):
    """A message received by the broker.

    Usually you don't insantiate message objects yourself, but receive
    them using a :class:`carrot.messaging.Consumer`.

    :param backend: see :attr:`backend`.
   
    :param amqp_message: see :attr:`amqp_message`.

    :param channel: see :attr:`channel`.

    :param decoder: see :attr:`decoder`.

   
    .. attribute:: body
        The message body. This data is serialized,
        so you probably want to deserialize it using
        :meth:`carrot.backends.BaseMessage.decode()`.

    .. attribute:: delivery_tag
        The message delivery tag, uniquely identifying this message.

    .. attribute:: backend
        The message backend used.
        A subclass of :class:`carrot.backends.BaseBackend`.

    .. attribute:: amqp_message

        A :class:`amqplib.client_0_8.basic_message.Message` instance.

    .. attribute:: channel

        The AMQP channel. A :class:`amqplib.client_0_8.channel.Channel` instance.

    .. attribute:: decoder

        A function able to deserialize the serialized message data.
    
    """
    def __init__(self, backend, amqp_message, **kwargs):
        self.amqp_message = amqp_message
        self.backend = backend
        kwargs.update({
            "body": amqp_message.body,
            "delivery_tag": amqp_message.delivery_tag})

        super(BaseMessage, self).__init__(backend, **kwargs)


class Backend(BaseBackend):

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.channel = self.connection.connection.channel()
        self.encoder = kwargs.get("encoder", serialize)
        self.decoder = kwargs.get("decoder", deserialize)

    def queue_declare(self, queue, durable, exclusive, auto_delete):
        self.channel.queue_declare(queue=queue, durable=durable,
                                   exclusive=exclusive,
                                   auto_delete=auto_delete)

    def exchange_declare(self, exchange, type, durable, auto_delete):
        self.channel.exchange_declare(exchange=exchange, type=type,
                                      durable=durable,
                                      auto_delete=auto_delete)

    def queue_bind(self, queue, exchange, routing_key):
        self.channel.queue_bind(queue=queue, exchange=exchange,
                                routing_key=routing_key)

    def get(self, queue):
        message = self.channel.basic_get(queue)
        if not message:
            return None
        return Message(backend=self,
                       amqp_message=message,
                       decoder=self.decoder)

    def consume(self, queue, no_ack, callback, consumer_tag):
        self.channel.basic_consume(queue=queue, no_ack=no_ack,
                                   callback=callback,
                                   consumer_tag=consumer_tag)
        while True:
            self.channel.wait()

    def cancel(self, consumer_tag):
        self.channel.basic_cancel(consumer_tag)

    def close(self):
        if getattr(self, "channel") and self.channel.is_open:
            self.channel.close()

    def ack(self, delivery_tag):
        return self.channel.basic_ack(delivery_tag)

    def reject(self, delivery_tag):
        return self.channel.basic_reject(delivery_tag, requeue=False)

    def requeue(self, delivery_tag):
        return self.channel.basic_reject(delivery_tag, requeue=True)

    def prepare_message(self, message_data, delivery_mode):
        message = amqp.Message(message_data)
        message.properties["delivery_mode"] = delivery_mode
        return message

    def publish(self, message, exchange, routing_key):
        return self.channel.basic_publish(message, exchange=exchange,
                                          routing_key=routing_key)
