from amqplib import client_0_8 as amqp


class AMQPConnection(object):
    virtual_host = "/"
    port = 5672
    insist = False

    @property
    def host(self):
        return ":".join([self.hostname, str(self.port)])

    def __init__(self, hostname, userid, password,
            virtual_host=None, port=None, **kwargs):
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = kwargs.get("insist", self.insist)

        self.connect()

    def connect(self):
        self.connection = amqp.Connection(host=self.host,
                                          userid=self.userid,
                                          password=self.password,
                                          virtual_host=self.virtual_host,
                                          insist=self.insist)

    def close(self):
        if getattr(self, "connection"):
            self.connection.close()


class DjangoAMQPConnection(AMQPConnection):

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
