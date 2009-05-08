DEFAULT_BACKEND = "pyamqplib"

class Client(object):
    pass


class DjangoClient(Client):

    def __init__(self, *args, **kwargs):
        from django.conf import settings
        hostname = kwargs.get("hostname",
                getattr(settings, "AMQP_SERVER"))
        userid = kwargs.get("userid",
                getattr(settings, "AMQP_USER"))
        password = kwargs.get("password",
                getattr(settings, "AMQP_PASSWORD"))
        virtual_host = kwargs.get("virtual_host",
                getattr(settings, "AMQP_VHOST"))
        port = kwargs.get("port",
                getattr(settings, "AMQP_PORT", None))
        backend = kwargs.get("backend",
                getattr(settings, "CARROT_BACKEND", DEFAULT_BACKEND))

        backend_cls = get_backend_by_name(backend)
        self.backend = backend_cls(hostname=hostname,
                                   userid=userid,
                                   password=password,
                                   virtual_host=virtual_host,
                                   port=port)

def get_backend_by_name(backend_name):
    if backend_name.find(".") == -1:
        backend_name = ".".join(("carrot.backend", backend_name))
    backend_module = __import__(backend_name, {}, {}, ["Backend"])
    return backend_module.Backend
