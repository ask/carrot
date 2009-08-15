"""

Working with Backends.

"""
import sys
from functools import partial

DEFAULT_BACKEND = "pyamqplib"


BACKEND_ALIASES = {
    "amqp": "pyamqplib",
    "amqplib": "pyamqplib",
    "stomp": "pystomp",
    "stompy": "pystomp",
    "memory": "queue",
    "mem": "queue",
}


def get_backend_cls(backend=None):
    """Get backend class by name.

    If the name does not include "``.``" (is not fully qualified),
    ``"carrot.backends."`` will be prepended to the name. e.g.
    ``"pyqueue"`` becomes ``"carrot.backends.pyqueue"``.

    """
    if not backend:
        backend = DEFAULT_BACKEND

    if backend.find(".") == -1:
        alias_to = BACKEND_ALIASES.get(backend.lower(), None)
        backend = "carrot.backends.%s" % (alias_to or backend)

    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, "Backend")
