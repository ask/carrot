from Queue import Empty
from itertools import cycle


class Queue(object):

    def __init__(self, connection, name):
        self.name = name
        self.connection = connection

    def put(self, payload):
        self.connection.put(self.name, payload)

    def get(self):
        payload = self.connection.get(self.name)
        if payload is not None:
            return payload
        raise Empty

    def purge(self):
        self.connection.purge(self.name)

    def __repr__(self):
        return "<Queue: %s>" % repr(self.name)


class QueueSet(object):
    """A set of queues that operates as one."""

    def __init__(self, connection, queues):
        self.connection = connection
        self.queue_names = list(queues)

        # queues could be a PriorityQueue as well to support
        # priorities.
        self.queues = map(self.backend.Queue, self.queue_names)

        # an infinite cycle through all the queues.
        self.cycle = cycle(self.queues)

        # A set of all the queue names, so we can match when we've
        # tried all of them.
        self.all = frozenset(self.queue_names)

    def get(self):
        """Get the next message avaiable in the queue.

        :returns: The message and the name of the queue it came from as
            a tuple.
        :raises Empty: If there are no more items in any of the queues.

        """

        # A set of queues we've already tried.
        tried = set()

        while True:
            # Get the next queue in the cycle, and try to get an item off it.
            queue = self.cycle.next()
            try:
                item = queue.get()
            except Empty:
                # raises Empty when we've tried all of them.
                tried.add(queue.name)
                if tried == self.all:
                    raise
            else:
                return item, queue.name

    def __repr__(self):
        return "<QueueSet: %s>" % repr(self.queue_names)


class Connection(object):

    def __init__(self, host=None, port=None, user=None, password=None,
            database=None, timeout=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.timeout = timeout
        self.connection = None

    def Queue(self, name):
        return Queue(self, name)

    def QueueSet(self, names):
        return QueueSet(self, names)

    @property
    def client(self):
        if self.connection is None:
            self.connection = self.establish_connection()
        return self.connection

    def close(self):
        pass
