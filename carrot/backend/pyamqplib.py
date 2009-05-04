from amqplib import client_0_8 as amqp
from carrot.backend import BaseMessage, BaseBackend

class Message(BaseMessage):
    def __init__(self, backend, amqp_message):
        self.amqp_message = amqp_message
        self.backend = backend
        super(BaseMessage, self).__init__(backend, {
            "body": amqp_message.body,
            "delivery_tag": amqp_message.delivery_tag})


class Backend(BaseBackend):

    def __init__(self, connection):
        self.connection = connection
        self.channel = self.connection.connection.channel()


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
        return Message(backend=self, amqp_message=message)

    def consume(self, queue, no_ack, callback, consumer_tag):
        self.channel.basic_consume(queue=queue, no_ack=no_ack,
                                   callback=callback,
                                   consumer_tag=consumer_tag)
        yield self.channel.wait()

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
