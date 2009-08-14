import os
import sys
import unittest
import uuid
sys.path.insert(0, os.pardir)
sys.path.append(os.getcwd())

from carrot.backends.pystomp import Message as StompMessage
from carrot.backends.pystomp import Backend as StompBackend
from carrot.connection import BrokerConnection
from carrot.messaging import Publisher, Consumer
from tests.utils import test_stomp_connection_args, STOMP_QUEUE
from stomp.frame import Frame
from carrot.serialization import encode


def create_connection():
    return BrokerConnection(backend_cls=StompBackend,
                            **test_stomp_connection_args())


def create_backend():
    return create_connection().create_backend()


class MockFrame(Frame):

    def mock(self, command=None, headers=None, body=None):
        self.command = command
        self.headers = headers
        self.body = body
        return self


class TestStompMessage(unittest.TestCase):

    def test_message(self):
        b = create_backend()
        self.assertTrue(b)

        message_body = "George Constanza"
        delivery_tag = str(uuid.uuid4())

        frame = MockFrame().mock(body=message_body, headers={
            "message-id": delivery_tag,
            "content_type": "text/plain",
            "content_encoding": "utf-8",
        })

        m1 = StompMessage(backend=b, frame=frame)
        m2 = StompMessage(backend=b, frame=frame)
        m3 = StompMessage(backend=b, frame=frame)
        self.assertEquals(m1.body, message_body)
        self.assertEquals(m1.delivery_tag, delivery_tag)

        #m1.ack()
        self.assertRaises(NotImplementedError, m2.reject)
        self.assertRaises(NotImplementedError, m3.requeue)


class TestPyStompMessaging(unittest.TestCase):

    def setUp(self):
        self.conn = create_connection()
        self.queue = STOMP_QUEUE
        self.exchange = STOMP_QUEUE
        self.routing_key = STOMP_QUEUE

    def create_consumer(self, **options):
        return Consumer(connection=self.conn,
                        queue=self.queue, exchange=self.exchange,
                        routing_key=self.routing_key, **options)

    def create_publisher(self, **options):
        return Publisher(connection=self.conn,
                exchange=self.exchange,
                routing_key=self.routing_key, **options)

    def test_backend(self):
        publisher = self.create_publisher()
        consumer = self.create_consumer()
        for i in range(100):
            publisher.send({"foo%d" % i: "bar%d" % i})
        publisher.close()

        discarded = consumer.discard_all()
        self.assertEquals(discarded, 100)
        publisher.close()
        consumer.close()

        publisher = self.create_publisher()
        for i in range(100):
            publisher.send({"foo%d" % i: "bar%d" % i})

        consumer = self.create_consumer()
        for i in range(100):
            while True:
                message = consumer.fetch()
                if message:
                    break
            self.assertTrue("foo%d" % i in message.payload)
            message.ack()

        publisher.close()
        consumer.close()


        consumer = self.create_consumer()
        discarded = consumer.discard_all()
        self.assertEquals(discarded, 0)

    def create_raw_message(self, publisher, body, delivery_tag):
        content_type, content_encoding, payload = encode(body)
        frame = MockFrame().mock(body=payload, headers={
            "message-id": delivery_tag,
            "content-type": content_type,
            "content-encoding": content_encoding,
        })
        return frame
