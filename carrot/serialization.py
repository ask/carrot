"""

Support for encoding/decoding.  Requires a json library. 
Optionally installs support for HessianPy and YAML.

"""

"""

.. function:: serialize(obj)

    Serialize the object to JSON.

.. function:: deserialize(obj)

    Deserialize JSON-encoded object to a Python object.

"""

import codecs

class DecoderNotInstalled(StandardError):
    """Support for the requested serialization type is not installed"""
    

class SerializerRegistry(object):
    
    def __new__(type):
        """
        Make the registry a singleton.
        """
        if not '_sr_instance' in type.__dict__:
            type._sr_instance = object.__new__(type)
        return type._sr_instance

    def __init__(self):
        self._encoders = {}
        self._decoders = {}
        self._default_encode = None
        self._default_mimetype = None
        
    def register(self, name, encoder, decoder, mimetype):
        if encoder: 
            self._encoders[name] = (mimetype, encoder)
        if decoder:
            self._decoders[mimetype] = decoder

    def set_default_serializer(self, name):
        try:
            self._default_mimetype, self._default_encode = self._encoders[name]
        except KeyError: 
            raise DecoderNotInstalled(
                "No decoder installed for %s" % name)

    def encode(self, message, serializer=None):
        content_encoding = 'utf-8'

        # If a raw string was sent, assume binary encoding 
        # (it's likely either ASCII or a raw binary file, but 'binary' 
        # charset will encompass both, even if not ideal.
        if isinstance(message, str) and not serializer:
            # In Python 3+, this would be "bytes"; allow binary data to be 
            # sent as a message without getting encoder errors
            content_type = 'application/data'
            content_encoding = 'binary'
            payload = message
        # For unicode objects, force it into 
        elif isinstance(message, unicode) and not serializer: 
            content_type = 'text/plain'
            payload = message.encode(content_encoding)
        elif serializer == 'raw': 
            content_type = 'application/data'
            payload = message
            if isinstance(payload, unicode): 
                payload = payload.encode('utf-8')
            else:
                content_encoding = 'binary'
        else:
            if serializer: 
                mimetype, encoder = self._encoders[encoding]
            else:
                encoder = self._default_encode
                content_type = self._default_mimetype
            payload = encoder(message)

        return (content_type, content_encoding, payload)

    def decode(self, message, content_type, content_encoding):
        content_type = content_type or 'application/data'
        content_encoding = (content_encoding or 'utf-8').lower()

        # Don't decode 8-bit strings
        if content_encoding not in ('binary','ascii-8bit'):
            message = codecs.decode(message, content_encoding)

        try:
            decoder = self._decoders[content_type]
        except KeyError: 
            return message
            
        return decoder(message)


registry = SerializerRegistry()


def register_json():
    # Try to import a module that provides json parsing and emitting, starting
    # with the fastest alternative and falling back to the slower ones.
    try:
        # cjson is the fastest
        import cjson
        json_serialize = cjson.encode
        json_deserialize = cjson.decode
    except ImportError:
        try:
            # Then try to find simplejson. Later versions has C speedups which
            # makes it pretty fast.
            import simplejson
            json_serialize = simplejson.dumps
            json_deserialize = simplejson.loads
        except ImportError:
            try:
                # Then try to find the python 2.6 stdlib json module.
                import json
                json_serialize = json.dumps
                json_deserialize = json.loads
            except ImportError:
                # If all of the above fails, fallback to the simplejson
                # embedded in Django.
                from django.utils import simplejson
                json_serialize = simplejson.dumps
                json_deserialize = simplejson.loads

    registry.register('json', json_serialize, json_deserialize, 
                      'application/json')


def register_hessian():
    from hessian.hessian import (writeObject as hessian_write, 
                                 readObject as hessian_read)
    def h_encode(body):
        payload_s = StringIO()
        hessian_write(payload_s, message)
        payload = payload_s.getvalue()
        return payload
        
    def h_decode(body):
        payload = StringIO(body)
        decoded = hessian_read(payload)
        return decoded

    registry.register('hessian', h_encode, h_decode, 'application/x-hessian')


def register_yaml():
    import yaml
    registry.register('yaml', yaml.safe_dump, yaml.safe_load, 
                      'application/x-yaml')
    

def register_pickle():
    import cPickle
    registry.register('pickle', cPickle.dumps, cPickle.loads, 
                      'application/x-python-serialize')


# Basic support
register_pickle()

# For backwards compatability
register_json()
registry.set_default_serializer('json')

# Register optional encoders, if possible
for optional in (register_hessian, register_yaml): 
    try:
        optional()
    except ImportError: 
        pass


