"""carrot.backends"""
import sys
from functools import partial

"""
.. data:: DEFAULT_BACKEND

    Name of the default backend used.

    If running under Django, this will be the value of
    ``settings.CARROT_BACKEND``.
"""
DEFAULT_BACKEND = "pyamqplib"

try:
    from django.conf import settings
    DEFAULT_BACKEND = getattr(settings, "CARROT_BACKEND", DEFAULT_BACKEND)
except ImportError:
    pass


def get_backend_cls(backend):
    """Get backend class by name.

    If the name does not include "``.``" (is not fully qualified),
    ``"carrot.backends."`` will be prepended to the name. e.g.
    ``"pyqueue"`` becomes ``"carrot.backends.pyqueue"``.

    """
    if backend.find(".") == -1:
        backend = "carrot.backends.%s" % backend
    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, "Backend")


"""
.. function:: get_default_backend_cls()

    Get the default backend class.

    Default is ``DEFAULT_BACKEND``.

"""
get_default_backend_cls = partial(get_backend_cls, DEFAULT_BACKEND)


"""
.. class:: DefaultBackend

    The default backend class.
    This is the class specified in ``DEFAULT_BACKEND``.
"""
DefaultBackend = get_default_backend_cls()
