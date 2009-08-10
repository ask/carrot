"""

Getting a connection to the AMQP server.

"""
from amqplib import client_0_8 as amqp
from socket import error as SocketError

DEFAULT_CONNECT_TIMEOUT = 5 # seconds


class ConnectionError(SocketError):
    """An error occured while trying to establish a connection to the 
    AMQP server."""


class AMQPConnection(object):
    """A network/socket connection to an AMQP message broker.

    :param hostname: see :attr:`hostname`.
    :param userid: see :attr:`userid`.
    :param password: see :attr:`password`.

    :keyword virtual_host: see :attr:`virtual_host`.
    :keyword port: see :attr:`port`.
    :keyword insist: see :attr:`insist`.
    :keyword connect_timeout: see :attr:`connect_timeout`.
    :keyword ssl: see :attr:`ssl`.

    .. attribute:: hostname

        The hostname to the AMQP server

    .. attribute:: userid

        A valid username used to authenticate to the server.

    .. attribute:: password

        The password used to authenticate to the server.

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

        The timeout in seconds before we give up connecting to the server.
        The default is no timeout.

    .. attribute:: ssl

        Use SSL to connect to the server.
        The default is ``False``.
    
    
    :raises ConnectionError: If the connection couldn't be established.
        This is a subclass of :exc:`socket.error`, with its ``errno``
        attribute set to its corresponding OS error code.

    **Handling errors**

    The ``errno`` attribute of the :exc:`ConnectionError` instance can be used
    to find out why the connection could not be established, e.g.:

        >>> from carrot.connection import AMQPConnection, ConnectionError
        >>> import errno
        >>> try:
        ...     conn = AMQPConnection(...).connection
        ... except ConnectionError, exc:
        ...     if exc.errno == errno.ECONNREFUSED:
        ...         # Connection refused
        ...     if exc.errno == errno.EPIPE:
        ...         # Connection lost

    """
    virtual_host = "/"
    port = 5672
    insist = False
    connect_timeout = DEFAULT_CONNECT_TIMEOUT
    ssl = False
    _closed = True

    @property
    def host(self):
        """The host as a hostname/port pair separated by colon."""
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
        self._closed = None
        self._connection = None

    @property
    def connection(self):
        if not self._connection:
            self._connection = self._establish()
            self._closed = False
        return self._connection

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def _establish(self):
        try:
            conn = amqp.Connection(host=self.host,
                               userid=self.userid,
                               password=self.password,
                               virtual_host=self.virtual_host,
                               insist=self.insist,
                               ssl=self.ssl,
                               connect_timeout=self.connect_timeout)
        except SocketError, exc:
            raise ConnectionError(*exc.args)
        else:
            return conn

    def connect(self):
        """Establish a connection to the AMQP server."""
        return self.connection

    def close(self):
        """Close the currently open connection."""
        if self._connection:
            self._connection.close()
        self._closed = True


class DummyConnection(object):
    """A connection class that does nothing, for non-networked backends."""
    _closed = True

    def __init__(self, *args, **kwargs):
        self._closed = False

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    def connect(self):
        """Doesn't do anything. Just for API compatibility."""
        pass

    def close(self):
        """Doesn't do anything. Just for API compatibility."""
        self._closed = True

    @property
    def host(self):
        """Always empty string."""
        return ""


class DjangoAMQPConnection(AMQPConnection):
    """A version of :class:`AMQPConnection` that takes configuration
    from the Django ``settings.py`` module.

    :keyword hostname: The hostname of the AMQP server to connect to,
        if not provided this is taken from ``settings.AMQP_SERVER``.

    :keyword userid: The username of the user to authenticate to the server
        as. If not provided this is taken from ``settings.AMQP_USER``.

    :keyword password: The users password. If not provided this is taken
        from ``settings.AMQP_PASSWORD``.

    :keyword vhost: The name of the virtual host to work with.
        This virtual host must exist on the server, and the user must
        have access to it. Consult your brokers manual for help with
        creating, and mapping users to virtual hosts. If not provided
        this is taken from ``settings.AMQP_VHOST``.

    :keyword port: The port the AMQP server is running on. If not provided
        this is taken from ``settings.AMQP_PORT``, or if that is not set,
        the default is ``5672`` (amqp).

    """

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
