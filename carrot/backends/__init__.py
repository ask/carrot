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


def resolve_backend(backend=None):
    backend = backend or DEFAULT_BACKEND
    if "." not in backend:
        return "carrot.backends.%s" % (
                    BACKEND_ALIASES.get(backend.lower(), backend))
    return backend


def get_backend_cls(backend=None):
    """Get backend class by name.

    If the name does not include "``.``" (is not fully qualified),
    ``"carrot.backends."`` will be prepended to the name. e.g.
    ``"pyqueue"`` becomes ``"carrot.backends.pyqueue"``.

    """
    backend = resolve_backend(backend)

    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, "Backend")
