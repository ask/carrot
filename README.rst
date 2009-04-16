==================================================================================
carrot - Simple RabbitMQ/ZeroMQ (AMQP) messaging queue support for Python/Django.
==================================================================================

:Authors:
    Ask Solem (askh@opera.com)
:Version: 0.3.3

Simple `RabbitMQ`_/`ZeroMQ`_ (`AMQP`_) messaging queue with optional support
for `Django`_.

*Note* This software is not finished. And is likely to change drastically
in the near future.

.. _`RabbitMQ`: http://www.rabbitmq.com/
.. _`ZeroMQ`: http://www.zeromq.org/
.. _`AMQP`: http://amqp.org
.. _`Django`: http://www.djangoproject.com/

Introduction
------------

`carrot` is a messaging queue framework with optional support for `Django_`, built on top of
`py-amqplib`_. Before you start playing with ``carrot``, you should
read the excellent article on RabbitMQ under python by Jason: `Rabbits and
warrens`_.

.. _`Rabbits and warrens`: http://blogs.digitar.com/jjww/2009/01/rabbits-and-warrens/
.. _`py-amqplib`: http://barryp.org/software/py-amqplib/

Installation
=============

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

Example
=======

    This is a `carrot` implementation of the example publisher and
    consumer described in the article `Rabbits and warrens`_

        >>> from carrot.messaging import Publisher, Consumer

        >>> class PostOfficePublisher(Publisher):
        ...     exchange = "sorting_room"
        ...     routing_key = "jason"
        
        >>> class PostOfficeConsumer(Consumer):
        ...     queue = "po_box"
        ...     exchange = "sorting_room"
        ...     routing_key = "json"
        ...
        ...     def receieve(self, message_data, message):
        ...         """This is the method that is called whenever we
        ...         receieve a message in this queue."""
        ...         print("Received: %s" % message_data)

    By default every message is encoded using `JSON`_, if you don't want
    this you have to set the ``encoding`` attribute in the producer and
    the ``decoding`` attribute in the consumer to ``lambda x: x``.

    There are lots of other options for producers and consumers for which
    the only documentation right now is the source code (sorry!)

    To start sending and receveing messages with these classes, you first
    have to configure the AMQP server for your django app. If you have a
    `RabbitMQ`_ server running on localhost you can add these settings 
    to your ``settings.py``,::

        AMQP_SERVER = "localhost"
        AMQP_PORT = 5678
        AMQP_USER = "my_rabbitmq_user"
        AMQP_PASSWORD = "my_rabbitmq_user_password"
        AMQP_VHOST = "virtual_host_to_use"

    First we have to set up the connection instance, and map it to the
    publishers and consumers:

        >>> from carrot.connection import DjangoAMQPConnection
        >>> po_publisher = PostOfficePublisher(connection=DjangoAMQPConnection)
        >>> po_consumer = PostOfficeConsumer(connection=DjangoAMQPConnection)


    Then, finally, we can send and receive some messages:

        >>> po_publisher.send({"My message": ["foo", "bar", "baz"]})
        >>> po_publisher.close()
        >>> po_consumer.next()
        Receieved: {"My message": ["foo", "bar", "baz"]}
        

.. _`JSON`: http://www.json.org/

License
=======

This software is licensed under the ``New BSD License``. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround
