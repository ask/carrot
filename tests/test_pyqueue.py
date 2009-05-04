import os
import sys
import unittest
import uuid
sys.path.append(os.getcwd())

from carrot.backend.queue import Message as PyQueueMessage
from carrot.backend.queue import Backend as PyQueueBackend
from carrot.connection import DummyConnection
from carrot.messaging import Messaging


def create_backend():
    return PyQueueBackend(connection=DummyConnection)


class TestPyQueueMessage(unittest.TestCase):

    def test_message(self):
        b = create_backend()
        self.assertTrue(b)

        message_body = "George Constanza"
        delivery_tag = str(uuid.uuid4())

        m1 = PyQueueMessage(backend=b,
                            body=message_body,
                            delivery_tag=delivery_tag)
        m2 = PyQueueMessage(backend=b,
                            body=message_body,
                            delivery_tag=delivery_tag)
        m3 = PyQueueMessage(backend=b,
                            body=message_body,
                            delivery_tag=delivery_tag)
        self.assertEquals(m1.body, message_body)
        self.assertEquals(m1.delivery_tag, delivery_tag)
        
        m1.ack()
        m2.reject()
        m3.requeue()


class TestPyQueueBackend(unittest.TestCase):

    def test_backend(self):
        b = create_backend()
        message_body = "Vandelay Industries"
        b.publish(b.prepare_message(message_body, "direct"), exchange="test",
                routing_key="test")
        m_in_q = b.get()
        self.assertTrue(isinstance(m_in_q, PyQueueMessage))
        self.assertEquals(m_in_q.body, message_body)


class TMessaging(Messaging):
    exchange = "test"
    routing_key = "test"
    queue = "test"


class TestMessaging(unittest.TestCase):

    def test_messaging(self):
        b = create_backend()
        m = TMessaging(connection_cls=DummyConnection, backend=b)
        self.assertTrue(m)

        mdata = {"name": "Cosmo Kramer"}
        m.send(mdata)
        next_msg = m.fetch()
        next_msg_data = m.decoder(next_msg.body)
        self.assertEquals(next_msg_data, mdata)

