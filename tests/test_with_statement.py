import os
import sys
import unittest
sys.path.insert(0, os.pardir)
sys.path.append(os.getcwd())

from tests.utils import test_connection_args
from carrot.connection import AMQPConnection
from carrot.messaging import Consumer, Publisher


class TestTransactioned(unittest.TestCase):

    def test_with_statement(self):

        with AMQPConnection(**test_connection_args()) as conn:
            self.assertFalse(conn._closed)
            with Publisher(connection=conn, exchange="F", routing_key="G") \
                    as publisher:
                        self.assertFalse(publisher._closed)
        self.assertTrue(conn._closed)
        self.assertTrue(publisher._closed)

        with AMQPConnection(**test_connection_args()) as conn:
            self.assertFalse(conn._closed)
            with Consumer(connection=conn, queue="E", exchange="F",
                    routing_key="G") as consumer:
                        self.assertFalse(consumer._closed)
        self.assertTrue(conn._closed)
        self.assertTrue(consumer._closed)
