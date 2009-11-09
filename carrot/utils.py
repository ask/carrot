from uuid import UUID, uuid4, _uuid_generate_random
try:
    import ctypes
except ImportError:
    ctypes = None


def gen_unique_id():
    """Generate a unique id, having - hopefully - a very small chance of
    collission.

    For now this is provided by :func:`uuid.uuid4`.
    """
    # Workaround for http://bugs.python.org/issue4607
    if ctypes and _uuid_generate_random:
        buffer = ctypes.create_string_buffer(16)
        _uuid_generate_random(buffer)
        return str(UUID(bytes=buffer.raw))
    return str(uuid4())
