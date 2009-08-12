import os

from carrot.connection import BrokerConnection


AMQP_HOST = os.environ.get('AMQP_HOST', "localhost")
AMQP_PORT = os.environ.get('AMQP_PORT', 5672)
AMQP_VHOST = os.environ.get('AMQP_VHOST', "/")
AMQP_USER = os.environ.get('AMQP_USER', "guest")
AMQP_PASSWORD = os.environ.get('AMQP_PASSWORD', "guest")

STOMP_HOST = os.environ.get('STOMP_HOST', 'localhost')
STOMP_PORT = os.environ.get('STOMP_PORT', 61613)
STOMP_QUEUE = os.environ.get('STOMP_QUEUE', '/queue/testcarrot')


def test_connection_args():
    return {"hostname": AMQP_HOST, "port": AMQP_PORT,
            "virtual_host": AMQP_VHOST, "userid": AMQP_USER,
            "password": AMQP_PASSWORD}


def test_stomp_connection_args():
    return {"hostname": STOMP_HOST, "port": STOMP_PORT}


def establish_test_connection():
    return BrokerConnection(**test_connection_args())
