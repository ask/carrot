"""

    Backend for unit-tests, using the Python :mod:`Queue` module.

"""
from collections import defaultdict
from Queue import Queue

from carrot.backends.emulation import EmulationBase

# Global message queue
_messages = defaultdict(lambda: Queue())


class Backend(EmulationBase):
    """Backend using the Python :mod:`Queue` library. Usually only
    used while executing unit tests.

    Please not that this backend does not support queues, exchanges
    or routing keys, so *all messages will be sent to all consumers*.

    """

    def _get(self, queue):
        return _messages[queue].get_nowait()

    def _put(self, queue, message):
        _messages[queue].put_nowait(message)

    def _purge(self, queue, **kwargs):
        qsize = _messages[queue].qsize()
        _messages[queue].queue.clear()
        return qsize
