"""

Getting a connection to the AMQP server.

"""
import socket
import warnings
import threading
from Queue import Queue

from amqplib.client_0_8.connection import AMQPConnectionException

from carrot.backends import get_backend_cls

DEFAULT_CONNECT_TIMEOUT = 5 # seconds
SETTING_PREFIX = "BROKER"
COMPAT_SETTING_PREFIX = "AMQP"
ARG_TO_DJANGO_SETTING = {
        "hostname": "HOST",
        "userid": "USER",
        "password": "PASSWORD",
        "virtual_host": "VHOST",
        "port": "PORT",
}
SETTING_DEPRECATED_FMT = "Setting %s has been renamed to %s and is " \
                         "scheduled for removal in version 1.0."


class BrokerConnection(object):
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

    .. attribute:: backend_cls

        The messaging backend class used. Defaults to the ``pyamqplib``
        backend.

    """
    virtual_host = "/"
    port = None
    insist = False
    ssl = False
    backend_cls = None
    _closed = False
    _released = False
    connect_timeout = DEFAULT_CONNECT_TIMEOUT

    ConnectionException = AMQPConnectionException

    def __init__(self, hostname=None, userid=None, password=None,
            virtual_host=None, port=None, pool=None, **kwargs):
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = kwargs.get("insist", self.insist)
        self.connect_timeout = kwargs.get("connect_timeout",
                                          self.connect_timeout)
        self.ssl = kwargs.get("ssl", self.ssl)
        self.backend_cls = kwargs.get("backend_cls", None)
        self._pool = pool
        self._closed = False
        self._connection = None

    def connect(self):
        """Establish a connection to the AMQP server."""
        self._closed = False
        self.evaluate_connection()

    def evaluate_connection(self):
        if self._closed:
            return
        if self._released:
            raise ReferenceError("Connection already released")
        if not self._connection:
            self._connection = self._establish_connection()
            self._closed = False
        return self._connection

    def reconnect(self):
        """Immediately close the current connection and establish a new."""
        self.force_close_connection()
        self.connect()

    def close(self):
        """Close/release the connection."""
        if self._pool:
            if self._released:
                raise ReferenceError("Connection already released.")
            self._pool.release(self)
        else:
            self.force_close_connection()

    def create_backend(self):
        """Create a new instance of the current backend in
        :attr:`backend_cls`."""
        backend_cls = self.get_backend_cls()
        return backend_cls(connection=self)

    def get_channel(self):
        """Request a new AMQP channel."""
        return self.connection.channel()

    def get_backend_cls(self):
        """Get the currently used backend class."""
        backend_cls = self.backend_cls
        if not backend_cls or isinstance(backend_cls, basestring):
            backend_cls = get_backend_cls(backend_cls)
        return backend_cls

    def force_close_connection(self):
        """Force close the connection.

        If you're not sure, please use :meth:`close` instead.

        """
        try:
            if self._connection:
                backend = self.create_backend()
                backend.close_connection(self._connection)
        except socket.error:
            pass
        finally:
            self._connection = None

    def _establish_connection(self):
        return self.create_backend().establish_connection()

    def __repr__(self):
        infodict = self.info

        def by_stringtype(k):
            return isinstance(infodict[k], basestring) and "a" or "z"

        prettyinfo = ", ".join("%s=%s" % (key, repr(infodict[key]))
                                    for key in sorted(infodict.keys(),
                                                      key=by_stringtype))
        connected = self._connection is not None
        state = connected and "connected" or "not connected"
        return "<%s: %s (info->%s)>" % (self.__class__.__name__,
                                        state,
                                        prettyinfo)

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        self.close()

    @property
    def connection(self):
        """Internal connection object."""
        return self.evaluate_connection()

    @property
    def host(self):
        """The host as a hostname/port pair separated by colon."""
        return ":".join([self.hostname, str(self.port)])

    @property
    def info(self):
        return {"userid": self.userid,
                "password": self.password,
                "hostname": self.hostname,
                "backend_cls": self.backend_cls,
                "virtual_host": self.virtual_host,
                "connect_timeout": self.connect_timeout,
                "insist": self.insist,
                "port": self.port,
                "ssl": self.ssl}

# For backwards compatibility.
AMQPConnection = BrokerConnection


class ConnectionPool(object):

    def __init__(self, connection_cls, size=4, maxsize=None, preconnect=0,
            lost_errors=(IOError, socket.error), buffersize=2, initial=[], **kwargs):
        self.connection_cls = connection_cls
        self.kwargs = kwargs
        self.maxsize = maxsize
        self.size = size
        self.cursize = 0
        self.preconnect = 0
        self.buffersize = buffersize
        self.lost_errors = lost_errors
        self.initial = initial

        self._ready = Queue()
        self._dirty = {}
        self._add_lock = threading.RLock()

        self._prepopulate()


    def _prepopulate(self):
        for i in range(max(self.preconnect, self.buffersize) - len(self.initial)):
            self.add_connection(eager=i < self.preconnect)

    @classmethod
    def from_connection(cls, connection, **kwargs):
        return cls(connection.__class__, **dict(connection.info, **kwargs))

    def create_connection(self):
        return self.connection_cls(**dict(self.kwargs, pool=self))

    def add_connection(self, eager=False):
        connection = self.create_connection()
        self._ready.put_nowait(self.create_connection())
        if eager:
            connection.evaluate_connection()
        self.cursize += 1

    def is_alive(self, connection):
        try:
            return True # TODO
        except self.lost_errors, exc:
            return False

    def _maintain_buffer(self):
        if self.maxsize and len(self._dirty) >= self.maxsize:
            return
        missing = max(self.buffersize - len(self._ready.queue), 0)
        if missing:
            for i in range(missing + 1):
                self.add_connection()

    def _next(self, block, timeout):
        while 1:
            self._maintain_buffer()
            connection = self._ready.get(block=block, timeout=timeout)
            if self.is_alive(connection):
                return connection

    def close(self):
        for connection in frozenset(self._ready.queue + self._dirty.keys()):
            connection.force_close_connection()

    def acquire(self, block=True, timeout=None):
        return self._next(block, timeout)

    def release(self, connection):
        assert connection not in self._ready.queue
        self._dirty.pop(connection, None)
        if len(self._ready.queue) < self.size:
            self._ready.put_nowait(connection)


def _get_django_conninfo():
    # FIXME can't wait to remove this mess in 1.0 [askh]
    ci = {}
    from django.conf import settings as django_settings

    ci["backend_cls"] = getattr(django_settings, "CARROT_BACKEND", None)

    for arg_name, setting_name in ARG_TO_DJANGO_SETTING.items():
        setting = "%s_%s" % (SETTING_PREFIX, setting_name)
        compat_setting = "%s_%s" % (COMPAT_SETTING_PREFIX, setting_name)
        if hasattr(django_settings, setting):
            ci[arg_name] = getattr(django_settings, setting, None)
        elif hasattr(django_settings, compat_setting):
            ci[arg_name] = getattr(django_settings, compat_setting, None)
            warnings.warn(DeprecationWarning(SETTING_DEPRECATED_FMT % (
                compat_setting, setting)))

    if "hostname" not in ci:
        if hasattr(django_settings, "AMQP_SERVER"):
            ci["hostname"] = django_settings.AMQP_SERVER
            warnings.warn(DeprecationWarning(
                "AMQP_SERVER has been renamed to BROKER_HOST and is"
                "scheduled for removal in version 1.0."))

    return ci


class DjangoBrokerConnection(BrokerConnection):
    """A version of :class:`BrokerConnection` that takes configuration
    from the Django ``settings.py`` module.

    :keyword hostname: The hostname of the AMQP server to connect to,
        if not provided this is taken from ``settings.BROKER_HOST``.

    :keyword userid: The username of the user to authenticate to the server
        as. If not provided this is taken from ``settings.BROKER_USER``.

    :keyword password: The users password. If not provided this is taken
        from ``settings.BROKER_PASSWORD``.

    :keyword virtual_host: The name of the virtual host to work with.
        This virtual host must exist on the server, and the user must
        have access to it. Consult your brokers manual for help with
        creating, and mapping users to virtual hosts. If not provided
        this is taken from ``settings.BROKER_VHOST``.

    :keyword port: The port the AMQP server is running on. If not provided
        this is taken from ``settings.BROKER_PORT``, or if that is not set,
        the default is ``5672`` (amqp).

    """


    def __init__(self, *args, **kwargs):
        kwargs = dict(_get_django_conninfo(), **kwargs)
        super(DjangoBrokerConnection, self).__init__(*args, **kwargs)

# For backwards compatability.
DjangoAMQPConnection = DjangoBrokerConnection
