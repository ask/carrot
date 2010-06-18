"""

Carrot In-Memory Webserver Backend

Backend opening up a Tornado web-server for clients to publish messages.

This would be a lot better if written using the ghettoq tools,
as then you'd get QoS, acks, different callbacks per queue. etc.

Proof of concept only!

For celeryd set::

    CARROT_BACKEND = "carrotweb.Backend"

To publish a task::

    >>> from simplejson import dumps
    >>> from urllib2 import Request, urlopen
    >>> task = {"task": "tasks.add",
    ...         "args": (2, 2),
    ...         "kwargs": {}
    ...         "id": "some-message-id"}
    >>> req = Request("http://localhost:6968/publish/celery",
    ...                data=simplejson.dumps(data),
    ...                headers={"Content-Type": "application/json"})
    >>> urlopen(req)



$ telnet localhost 6968
POST http://localhost/publish/celery HTTP/1.1
Host: http://localhost
Content-Type: application/javascript

{"task": "tasks.add", "args": [2, 2]}




"""
import threading

from itertools import count

from carrot.backends.queue import Message, Backend as QueueBackend
from carrot.utils import partition

from tornado import httpserver
from tornado import ioloop
from tornado.web import Application, RequestHandler

next_delivery_tag = count(1).next


class PublishHandler(RequestHandler):

    def __init__(self, *args, **kwargs):
        self.backend = kwargs.pop("backend")
        super(PublishHandler, self).__init__(*args, **kwargs)

    def post(self, queue):
        headers = self.request.headers
        body = self.request.body
        content_type, _, _ = partition(headers["Content-Type"], ";")
        content_encoding = headers.get("Content-Encoding")
        message = self.backend.prepare_message(body, 1, content_type,
                                               content_encoding)
        self.backend.publish(message, queue, queue)
        return next_delivery_tag()


class WebServerThread(threading.Thread):
    port = 6968

    def __init__(self, backend, port=None):
        threading.Thread.__init__(self)
        self.setDaemon(True)
        self.backend = backend
        self.port = port or self.port
        self.app = Application([
            (r"/publish/(.+?)", PublishHandler, {"backend": self.backend}),
        ])

    def run(self):
        server = httpserver.HTTPServer(self.app)
        server.listen(self.port)
        ioloop.IOLoop.instance().start()


class Backend(QueueBackend):
    """Webserver Backend"""
    server = None
    _instance = None

    def __new__(cls, *args, **kwargs):
        # Yes. It's a singleton!
        if cls._instance is None:
            cls._instance = super(Backend, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def establish_connection(self):
        conninfo = self.connection
        if not self.server:
            self.server = WebServerThread(self, port=conninfo.port)
            self.server.start()
        return self; # for drain_events

    def close_connection(self, connection):
        if self.server:
            self.server.join()


class ClientBackend(QueueBackend):
    """Backend using the webserver to publish messages"""

    def publish(self, message, exchange, routing_key, **kwargs):
        from urllib2 import urlopen, Request
        conninfo = self.connection
        url = "%s:%s/publish/%s" % (
                conninfo.hostname, conninfo.port, exchange)
        req = Request(url, data=message, headers={
            "Content-Type": content_type,
            "Content-Encoding": content_encoding})
        urlopen(req).read()
