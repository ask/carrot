##############################################
 carrot - AMQP Messaging Framework for Python
##############################################

:Version: 0.4.5

**NOTE** This release contains backward-incompatible changes and
important bugfixes. Please read the `Changelog`_ for more information.

.. _`Changelog`: http://ask.github.com/carrot/changelog.html


Introduction
------------

`carrot` is an `AMQP`_ messaging queue framework. AMQP is the Advanced Message
Queuing Protocol, an open standard protocol for message orientation, queuing,
routing, reliability and security.

The aim of `carrot` is to make messaging in Python as easy as possible by
providing a high-level interface for producing and consuming messages. At the
same time it is a goal to re-use what is already available as much as possible.

`carrot` has pluggable messaging back-ends, so it is possible to support
several messaging systems. At the time of release, the `py-amqplib`_ based
backend is considered suitable for production use.

Several AMQP message broker implementations exists, including `RabbitMQ`_,
`ZeroMQ`_ and `Apache ActiveMQ`_. You'll need to have one of these installed,
personally we've been using `RabbitMQ`_.

Before you start playing with ``carrot``, you should probably read up on
AMQP, and you could start with the excellent article about using RabbitMQ
under Python, `Rabbits and warrens`_. For more detailed information, you can
refer to the `Wikipedia article about AMQP`_.

.. _`RabbitMQ`: http://www.rabbitmq.com/
.. _`ZeroMQ`: http://www.zeromq.org/
.. _`AMQP`: http://amqp.org
.. _`Apache ActiveMQ`: http://activemq.apache.org/
.. _`Django`: http://www.djangoproject.com/
.. _`Rabbits and warrens`: http://blogs.digitar.com/jjww/2009/01/rabbits-and-warrens/
.. _`py-amqplib`: http://barryp.org/software/py-amqplib/
.. _`Wikipedia article about AMQP`: http://en.wikipedia.org/wiki/AMQP

Documentation
-------------

Carrot is using Sphinx, and the latest documentation is available at GitHub:

    http://github.com/ask/carrot/

Installation
============

You can install ``carrot`` either via the Python Package Index (PyPI)
or from source.

To install using ``pip``,::

    $ pip install carrot


To install using ``easy_install``,::

    $ easy_install carrot


If you have downloaded a source tarball you can install it
by doing the following,::

    $ python setup.py build
    # python setup.py install # as root


Terminology
===========

There are some concepts you should be familiar with before starting:

    * Publishers

        Publishers sends messages to an exchange.

    * Exchanges

        Messages are sent to exchanges. Exchanges are named and can be
        configured to use one of several routing algorithms. The exchange
        routes the messages to consumers by matching the routing key in the
        message with the routing key the consumer provides when binding to
        the exchange.

    * Consumers

        Consumers declares a queue, binds it to a exchange and receives
        messages from it.

    * Queues

        Queues receive messages sent to exchanges. The queues are declared
        by consumers.

    * Routing keys

        Every message has a routing key.  The interpretation of the routing
        key depends on the exchange type. There are four default exchange
        types defined by the AMQP standard, and vendors can define custom
        types (so see your vendors manual for details).

        These are the default exchange types defined by AMQP/0.8:

            * Direct exchange

                Matches if the routing key property of the message and
                the ``routing_key`` attribute of the consumer are identical.

            * Fan-out exchange

                Always matches, even if the binding does not have a routing
                key.

            * Topic exchange

                Matches the routing key property of the message by a primitive
                pattern matching scheme. The message routing key then consists
                of words separated by dots (``"."``, like domain names), and
                two special characters are available; star (``"*"``) and hash
                (``"#"``). The star matches any word, and the hash matches
                zero or more words. For example ``"*.stock.#"`` matches the
                routing keys ``"usd.stock"`` and ``"eur.stock.db"`` but not
                ``"stock.nasdaq"``.


Examples
========

Creating a connection
---------------------

    You can set up a connection by creating an instance of
    ``carrot.messaging.AMQPConnection``, with the appropriate options for
    your AMQP server:

    >>> from carrot.connection import AMQPConnection
    >>> amqpconn = AMQPConnection(hostname="localhost", port=5672,
    ...                           userid="test", password="test",
    ...                           vhost="test")


    If you're using Django you can use the
    ``carrot.connection.DjangoAMQPConnection`` class instead, which loads the
    connection settings from your ``settings.py``::

       AMQP_SERVER = "localhost"
       AMQP_PORT = 5672
       AMQP_USER = "test"
       AMQP_PASSWORD = "secret"
       AMQP_VHOST = "/test"

    Then create a connection by doing:

        >>> from carrot.connection import DjangoAMQPConnection
        >>> amqpconn = DjangoAMQPConnection()



Receiving messages using a Consumer
-----------------------------------

First we open up a Python shell and start a message consumer.

This consumer declares a queue named ``"feed"``, receiving messages with
the routing key ``"importer"`` from the ``"feed"`` exchange.

The example then uses the consumers ``wait()`` method to go into consume
mode, where it continuously polls the queue for new messages, and when a
message is received it passes the message to all registered callbacks.

    >>> from carrot.messaging import Consumer
    >>> consumer = Consumer(connection=amqpconn, queue="feed",
    ...                     exchange="feed", routing_key="importer")
    >>> def import_feed_callback(message_data, message)
    ...     feed_url = message_data["import_feed"]
    ...     print("Got feed import message for: %s" % feed_url)
    ...     # something importing this feed url
    ...     # import_feed(feed_url)
    ...     message.ack()
    >>> consumer.register_callback(import_feed_callback)
    >>> consumer.wait() # Go into the consumer loop.

Sending messages using a Publisher
----------------------------------

Then we open up another Python shell to send some messages to the consumer
defined in the last section.

    >>> from carrot.messaging import Publisher
    >>> publisher = Publisher(connection=amqpconn,
    ...                       exchange="feed", routing_key="importer")
    >>> publisher.send({"import_feed": "http://cnn.com/rss/edition.rss"})
    >>> publisher.close()


Look in the first Python shell again (where ``consumer.wait()`` is running),
where the following text has been printed to the screen::

   Got feed import message for: http://cnn.com/rss/edition.rss  


By default every message is encoded using `JSON`_, so sending
Python data structures like dictionaries and lists works. If you want
to support more complicated data, you might want to configure the publisher
and consumer to use something like ``pickle``, by providing them with
an ``encoder`` and ``decoder`` respectively.

.. _`JSON`: http://www.json.org/


Receiving messages without a callback
--------------------------------------

You can also poll the queue manually, by using the ``fetch`` method.
This method returns a ``Message`` object, from where you can get the
message body, de-serialize the body to get the data, acknowledge, reject or
re-queue the message.

    >>> consumer = Consumer(connection=amqpconn, queue="feed",
    ...                     exchange="feed", routing_key="importer")
    >>> message = consumer.fetch()
    >>> if message:
    ...    message_data = message.decode()
    ...    message.ack()
    ... else:
    ...     # No messages waiting on the queue.
    >>> consumer.close()

Sub-classing the messaging classes
----------------------------------

The ``Consumer``, and ``Publisher`` classes can also be subclassed. Thus you
can define the above publisher and consumer like so:

    >>> from carrot.messaging import Publisher, Consumer

    >>> class FeedPublisher(Publisher):
    ...     exchange = "feed"
    ...     routing_key = "importer"
    ...
    ...     def feed_import(feed_url):
    ...         return self.send({"action": "import_feed",
    ...                           "feed_url": feed_url})

    >>> class FeedConsumer(Consumer):
    ...     queue = "feed"
    ...     exchange = "feed"
    ...     routing_key = "importer"
    ...
    ...     def receive(self, message_data, message):
    ...         action = message_data["action"]
    ...         if action == "import_feed":
    ...             # something importing this feed
    ...             # import_feed(message_data["feed_url"])
                    message.ack()
    ...         else:
    ...             raise Exception("Unknown action: %s" % action)

    >>> publisher = FeedPublisher(connection=amqpconn)
    >>> publisher.import_feed("http://cnn.com/rss/edition.rss")
    >>> publisher.close()

    >>> consumer = FeedConsumer(connection=amqpconn)
    >>> consumer.wait() # Go into the consumer loop.

Getting Help
============

Mailing list
------------

Join the `carrot-users`_ mailing list.

.. _`carrot-users`: http://groups.google.com/group/carrot-users/

Bug tracker
===========

If you have any suggestions, bug reports or annoyances please report them
to our issue tracker at http://github.com/ask/carrot/issues/

Contributing
============

Development of ``carrot`` happens at Github: http://github.com/ask/carrot

You are highly encouraged to participate in the development. If you don't
like Github (for some reason) you're welcome to send regular patches.

License
=======

This software is licensed under the ``New BSD License``. See the ``LICENCE``
file in the top distribution directory for the full license text.
