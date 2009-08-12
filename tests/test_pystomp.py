import os
import sys
import unittest
import uuid
sys.path.insert(0, os.pardir)
sys.path.append(os.getcwd())

from carrot.backends.pystomp import Message as StompMessage
from carrot.backends.pystomp import Backend as StompBackend
from carrot.connection import BrokerConnection
from carrot.messaging import Messaging
from tests.utils import test_stomp_connection_args, STOMP_QUEUE
from stomp.frame import Frame
from tests.backend import BackendMessagingCase
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


class TestPyStompMessaging(BackendMessagingCase):

    def setUp(self):
        self.conn = create_connection()
        self.queue = STOMP_QUEUE
        self.exchange = STOMP_QUEUE
        self.routing_key = STOMP_QUEUE

    def create_raw_message(self, publisher, body, delivery_tag):
        content_type, content_encoding, payload = encode(body)
        frame = MockFrame().mock(body=payload, headers={
            "message-id": delivery_tag,
            "content-type": content_type,
            "content-encoding": content_encoding,
        })
        return frame

    def test_consumer_discard_all(self):
        pass

    def test_empty_queue_returns_None(self):
        # TODO get is currently blocking.
        pass

BackendMessagingCase = None # Don't run tests on the base class.

"""


class TMessaging(Messaging):
    exchange = "test"
    routing_key = "test"
    queue = "test"


class XXXMessaging(unittest.TestCase):

    def test_messaging(self):
        m = TMessaging(connection=BrokerConnection(backend_cls=StompBackend))
        self.assertTrue(m)

        self.assertEquals(m.fetch(), None)
        mdata = {"name": "Cosmo Kramer"}
        m.send(mdata)
        next_msg = m.fetch()
        next_msg_data = next_msg.decode()
        self.assertEquals(next_msg_data, mdata)
        self.assertEquals(m.fetch(), None)

if __name__ == '__main__':
    unittest.main()

"""
