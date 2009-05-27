from amqplib import client_0_8 as amqp


class AMQPConnection(object):
    """A network/socket connection to an AMQP message broker.
    
    :param hostname: see :attr:`hostname`.
    :param userid: see :attr:`userid`.
    :param password: see :attr:`password`.
    
    :keyword virtual_host: see :attr:`virtual_host`.
    :keyword port: see :attr:`port`.
    :keyword insist: see :attr:`insist`.
    :keyword connect_timeout: see :attr:`connect_timeout`.


    .. attribute:: hostname

        The hostname to the AMQP server

    .. attribute:: userid

        The name of the user we authenticate to the server as.

    .. attribute:: password

        The password for the :attr:`userid`.

    .. attribute:: virtual_host

        The name of the virtual host to work with. This virtual host must
        exist on the server, and the user must have access to it. Consult
        your brokers manual for help with creating, and mapping
        users to virtual hosts.
        Default is ``"/"``.

    .. attribute:: port

        The port of the AMQP server.  Default is ``5672`` (amqp).

    .. attribute:: insist

        Insist on connecting to a server. In a configuration with multiple
        load-sharing servers, the insist option tells the server that the
        client is insisting on a connection to the specified server.
        Default is ``False``.

    .. attribute:: connect_timeout

        The timeout in seconds before we give up on connecting to the server.
        The default is no timeout.

    .. attribute:: ssl

        Use SSL to connect to the server.
        The default is ``False``.

    """
    virtual_host = "/"
    port = 5672
    insist = False
    connect_timeout = None
    ssl = False

    @property
    def host(self):
        return ":".join([self.hostname, str(self.port)])

    def __init__(self, hostname, userid, password,
            virtual_host=None, port=None, **kwargs):
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = kwargs.get("insist", self.insist)
        self.connect_timeout = kwargs.get("connect_timeout",
                                          self.connect_timeout)
        self.ssl = kwargs.get("ssl", self.ssl)
        self.connection = None

        self.connect()

    def connect(self):
        self.connection = amqp.Connection(host=self.host,
                                        userid=self.userid,
                                        password=self.password,
                                        virtual_host=self.virtual_host,
                                        insist=self.insist,
                                        ssl=self.ssl,
                                        connect_timeout=self.connect_timeout)

    def close(self):
        if self.connection:
            self.connection.close()


class DummyConnection(object):
    def __init__(self, *args, **kwargs):
        pass

    def connect(self):
        pass

    def close(self):
        pass

    @property
    def host(self):
        pass


class DjangoAMQPConnection(AMQPConnection):

    def __init__(self, *args, **kwargs):
        from django.conf import settings
        kwargs["hostname"] = kwargs.get("hostname",
                getattr(settings, "AMQP_SERVER"))
        kwargs["userid"] = kwargs.get("userid",
                getattr(settings, "AMQP_USER"))
        kwargs["password"] = kwargs.get("password",
                getattr(settings, "AMQP_PASSWORD"))
        kwargs["virtual_host"] = kwargs.get("virtual_host",
                getattr(settings, "AMQP_VHOST"))
        kwargs["port"] = kwargs.get("port",
                getattr(settings, "AMQP_PORT", self.port))

        super(DjangoAMQPConnection, self).__init__(*args, **kwargs)
