from carrot.connection import AMQPConnection

AMQP_HOST = "localhost"
AMQP_PORT = 5672
AMQP_VHOST = "/"
AMQP_USER = "guest"
AMQP_PASSWORD = "guest"


def test_connection_args():
    return {"hostname": AMQP_HOST, "port": AMQP_PORT,
            "virtual_host": AMQP_VHOST, "userid": AMQP_USER,
            "password": AMQP_PASSWORD}


def establish_test_connection():
    return AMQPConnection(**test_connection_args())
