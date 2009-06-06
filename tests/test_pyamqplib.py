import os
import sys
import unittest
import pickle
import time
sys.path.insert(0, os.pardir)
sys.path.append(os.getcwd())

from tests.utils import establish_test_connection
from carrot.connection import AMQPConnection
from carrot.messaging import Consumer, Publisher
from carrot.backends.pyamqplib import Backend as AMQPLibBackend
from carrot.backends.pyamqplib import Message as AMQPLibMessage

TEST_QUEUE = "carrot.unittest"
TEST_EXCHANGE = "carrot.unittest"
TEST_ROUTING_KEY = "carrot.unittest"


class AdvancedDataType(object):

    def __init__(self, something):
        self.data = something


def fetch_next_message(consumer):
    while True:
        message = consumer.fetch()
        if message:
            return message


class TestMessaging(unittest.TestCase):

    def setUp(self):
        self.conn = establish_test_connection()

    def create_consumer(self, **options):
        return Consumer(connection=self.conn, backend_cls=AMQPLibBackend,
                        queue=TEST_QUEUE, exchange=TEST_EXCHANGE,
                        routing_key=TEST_ROUTING_KEY, **options)

    def create_publisher(self, **options):
        return Publisher(connection=self.conn, backend_cls=AMQPLibBackend,
                        exchange=TEST_EXCHANGE, routing_key=TEST_ROUTING_KEY,
                        **options)

    def test_regression_implied_auto_delete(self):
        consumer = self.create_consumer(exclusive=True)
        self.assertTrue(consumer.auto_delete, "exclusive implies auto_delete")
        consumer.close()

        consumer = self.create_consumer(durable=True, auto_delete=False)
        self.assertFalse(consumer.auto_delete,
            """durable does *not* imply auto_delete.
            regression: http://github.com/ask/carrot/issues/closed#issue/2""")
        consumer.close()

    def test_consumer_options(self):
        opposite_defaults = {
                "queue": "xyxyxyxy",
                "exchange": "xyxyxyxy",
                "routing_key": "xyxyxyxy",
                "durable": False,
                "exclusive": True,
                "auto_delete": True,
                "exchange_type": "topic",
        }
        consumer = Consumer(connection=self.conn, **opposite_defaults)
        for opt_name, opt_value in opposite_defaults.items():
            self.assertEquals(getattr(consumer, opt_name), opt_value)
        consumer.close()

    def test_consumer_backend(self):
        consumer = self.create_consumer()
        self.assertTrue(isinstance(consumer.backend, consumer.backend_cls))
        self.assertTrue(consumer.backend.connection is self.conn)
        self.assertTrue(consumer.backend.decoder is consumer.decoder)
        consumer.close()

    def test_consumer_queue_declared(self):
        consumer = self.create_consumer()
        self.assertTrue(consumer.backend.queue_exists(consumer.queue))
        consumer.close()

    def test_consumer_callbacks(self):
        consumer = self.create_consumer()
        publisher = self.create_publisher()

        # raises on no callbacks
        self.assertRaises(NotImplementedError, consumer.receive, {}, {})

        callback1_scratchpad = {}

        def callback1(message_data, message):
            callback1_scratchpad["message_data"] = message_data

        callback2_scratchpad = {}

        def callback2(message_data, message):
            callback2_scratchpad.update({"delivery_tag": message.delivery_tag,
                                         "message_body": message.body})

        self.assertFalse(consumer.callbacks, "no default callbacks")
        consumer.register_callback(callback1)
        consumer.register_callback(callback2)
        self.assertEquals(len(consumer.callbacks), 2, "callbacks registered")

        self.assertTrue(consumer.callbacks[0] is callback1,
                "callbacks are ordered")
        self.assertTrue(consumer.callbacks[1] is callback2,
                "callbacks are ordered")

        body = {"foo": "bar"}
        raw_message = publisher.create_message(body)
        raw_message.delivery_tag = "Elaine was here"
        message = AMQPLibMessage(amqp_message=raw_message,
                backend=consumer.backend, decoder=consumer.decoder)
        consumer._receive_callback(message)

        self.assertEquals(callback1_scratchpad.get("message_data"), body,
                "callback1 was called")
        self.assertEquals(callback2_scratchpad.get("delivery_tag"),
                "Elaine was here")
        self.assertEquals(callback2_scratchpad.get("message_body"),
                publisher.encoder(body), "callback2 was called")

        consumer.close()
        publisher.close()

    def test_empty_queue_returns_None(self):
        consumer = self.create_consumer()
        consumer.discard_all()
        self.assertFalse(consumer.fetch())
        consumer.close()

    def test_custom_serialization_scheme(self):
        consumer = self.create_consumer(decoder=pickle.loads)
        publisher = self.create_publisher(encoder=pickle.dumps)
        consumer.discard_all()

        data = {"string": "The quick brown fox jumps over the lazy dog",
                "int": 10,
                "float": 3.14159265,
                "unicode": u"The quick brown fox jumps over the lazy dog",
                "advanced": AdvancedDataType("something"),
                "set": set(["george", "jerry", "elaine", "cosmo"]),
                "exception": Exception("There was an error"),
        }

        publisher.send(data)
        message = fetch_next_message(consumer)
        self.assertTrue(isinstance(message, AMQPLibMessage))

        decoded_data = message.decode()

        self.assertEquals(decoded_data.get("string"),
                "The quick brown fox jumps over the lazy dog")
        self.assertEquals(decoded_data.get("int"), 10)
        self.assertEquals(decoded_data.get("float"), 3.14159265)
        self.assertEquals(decoded_data.get("unicode"),
                u"The quick brown fox jumps over the lazy dog")
        self.assertEquals(decoded_data.get("set"),
            set(["george", "jerry", "elaine", "cosmo"]))
        self.assertTrue(isinstance(decoded_data.get("exception"), Exception))
        self.assertEquals(decoded_data.get("exception").args[0],
            "There was an error")
        self.assertTrue(isinstance(decoded_data.get("advanced"),
            AdvancedDataType))
        self.assertEquals(decoded_data["advanced"].data, "something")

        consumer.close()
        publisher.close()

    def test_consumer_fetch(self):
        consumer = self.create_consumer()
        publisher = self.create_publisher()
        consumer.discard_all()

        data = {"string": "The quick brown fox jumps over the lazy dog",
                "int": 10,
                "float": 3.14159265,
                "unicode": u"The quick brown fox jumps over the lazy dog",
        }

        publisher.send(data)
        message = fetch_next_message(consumer)
        self.assertTrue(isinstance(message, AMQPLibMessage))

        self.assertEquals(message.decode(), data)

        consumer.close()
        publisher.close()

    def test_consumer_process_next(self):
        consumer = self.create_consumer()
        publisher = self.create_publisher()
        consumer.discard_all()

        scratchpad = {}

        def callback(message_data, message):
            scratchpad["delivery_tag"] = message.delivery_tag
        consumer.register_callback(callback)

        publisher.send({"name_discovered": {
                            "first_name": "Cosmo",
                            "last_name": "Kramer"}})

        while True:
            message = consumer.fetch(enable_callbacks=True)
            if message:
                break

        self.assertEquals(scratchpad.get("delivery_tag"),
                message.delivery_tag)

        consumer.close()
        publisher.close()

    def test_consumer_discard_all(self):
        consumer = self.create_consumer()
        publisher = self.create_publisher()
        consumer.discard_all()

        for i in xrange(100):
            publisher.send({"foo": "bar"})
        time.sleep(0.5)

        self.assertEquals(consumer.discard_all(), 100)

        consumer.close()
        publisher.close()

    def test_iterqueue(self):
        consumer = self.create_consumer()
        publisher = self.create_publisher()
        consumer.discard_all()

        it = consumer.iterqueue(limit=100)
        consumer.register_callback(lambda *args: args)

        for i in xrange(100):
            publisher.send({"foo%d" % i: "bar%d" % i})
        time.sleep(0.5)

        for i in xrange(100):
            try:
                message = it.next()
                data = message.decode()
                self.assertTrue("foo%d" % i in data)
                self.assertEquals(data.get("foo%d" % i), "bar%d" % i)
            except StopIteration:
                self.assertTrue(False, "iterqueue fails StopIteration")

        self.assertRaises(StopIteration, it.next)

        # no messages on queue raises StopIteration if infinite=False
        it = consumer.iterqueue()
        self.assertRaises(StopIteration, it.next)

        it = consumer.iterqueue(infinite=True)
        self.assertTrue(it.next() is None,
                "returns None if no messages and inifite=True")

        consumer.close()
        publisher.close()

    def test_publisher_message_priority(self):
        consumer = self.create_consumer()
        publisher = self.create_publisher()
        consumer.discard_all()

        m = publisher.create_message("foo", priority=9)
        self.assertEquals(m.priority, 9)

        publisher.send({"foo": "bar"}, routing_key="nowhere", priority=9,
                mandatory=False, immediate=False)

        consumer.close()
        publisher.close()

    def test_consumer_test_auto_ack(self):
        consumer = self.create_consumer(auto_ack=True)
        publisher = self.create_publisher()
        consumer.discard_all()

        publisher.send({"foo": "Baz"})
        message = fetch_next_message(consumer)
        self.assertEquals(message._state, "ACK")

        consumer.close()
        consumer = self.create_consumer(auto_ack=False)
        publisher.send({"foo": "Baz"})
        message = fetch_next_message(consumer)
        self.assertEquals(message._state, "RECEIVED")

        consumer.close()
        publisher.close()

    def test_consumer_consume(self):
        consumer = self.create_consumer(auto_ack=True)
        publisher = self.create_publisher()
        consumer.discard_all()

        data = {"foo": "Baz"}
        publisher.send(data)
        try:
            data2 = {"company": "Vandelay Industries"}
            publisher.send(data2)
            scratchpad = {}

            def callback(message_data, message):
                scratchpad["data"] = message_data
            consumer.register_callback(callback)

            it = consumer.iterconsume()
            it.next()
            self.assertEquals(scratchpad.get("data"), data)
            it.next()
            self.assertEquals(scratchpad.get("data"), data2)

            # Cancel consumer/close and restart.
            consumer.close()
            consumer = self.create_consumer(auto_ack=True)
            consumer.register_callback(callback)
            consumer.discard_all()
            scratchpad = {}


            # Test limits
            it = consumer.iterconsume(limit=4)
            publisher.send(data)
            publisher.send(data2)
            publisher.send(data)
            publisher.send(data2)
            publisher.send(data)

            it.next()
            self.assertEquals(scratchpad.get("data"), data)
            it.next()
            self.assertEquals(scratchpad.get("data"), data2)
            it.next()
            self.assertEquals(scratchpad.get("data"), data)
            it.next()
            self.assertEquals(scratchpad.get("data"), data2)
            self.assertRaises(StopIteration, it.next)


        finally:
            consumer.close()
            publisher.close()
