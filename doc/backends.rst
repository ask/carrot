Backend design ramblings.


client == backend + connection
===============================

    * carrot/
        * backends/
            * pyamqplib
            * pyqueue
            * txamqplib
        * connection/
            * AMQPConnection
            * DummyConnection
        * client
            * DjangoClient
            * Client


``DjangoClient`` selects backends using the ``CARROT_BACKEND`` setting:
    
    >>> CARROT_BACKEND = "pyamqplib"


backends chooses connection. ``pyqueue`` uses ``DummyConnection``,
``pyamqplib`` uses ``AMQPConnection``.

::

    class DjangoClient(object):

        def __init__(self):
            self.host = settings.AMQP_SERVER
            self.port = settings.AMQP_PORT
            self.vhost = settings.AMQP_VHOST
            self.username = settings.AMQP_USER
            self.password = settings.AMQP_PASSWORD
            self.backend_cls = get_backend_by_name(settings.CARROT_BACKEND)
            self.backend = self.backend_cls(host=self.host, ....)
    
    
Using the DjangoClient
----------------------

    >>> from carrot.client import DjangoClient
    >>> publisher = Publisher(client=DjangoClient())

    
Using the Regular Client
------------------------

    >>> from carrot.client import Client
    >>> client = Client(host="localhost", vhost="testing", username="x",
    ...                 password="x")
    >>> publisher = Publisher(client=client)

Using a different backend with the regular client.
--------------------------------------------------

    >>> client = Client(backend="pyqueue", ...)
