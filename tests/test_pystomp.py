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
from tests.utils import test_stomp_connection_args
from stomp.frame import Frame


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

class TestPyQueueBackend(unittest.TestCase):

    def xxx_backend(self):
        b = create_backend()
        message_body = "Vandelay Industries"
        b.publish(b.prepare_message(message_body, "direct",
                                    content_type='text/plain',
                                    content_encoding="ascii"),
                  exchange="test",
                  routing_key="test")
        m_in_q = b.get()
        self.assertTrue(isinstance(m_in_q, PyQueueMessage))
        self.assertEquals(m_in_q.body, message_body)

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
