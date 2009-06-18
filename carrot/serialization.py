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

utf8_encoder = codecs.getencoder('utf-8')

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
        self._default_encoder = None
        self._default_mimetype = None
        
    def register(self, name, encoder, decoder, mimetype):
        if encoder: 
            self._encoders[name] = (mimetype, encoder)
        if decoder:
            self._decoders[mimetype] = decoder

    def set_default_encoder(self, name):
        try:
            self._default_encoder, self._default_mimetype = self._encoders[name]
        except KeyError: 
            raise DecoderNotInstalled(
                "No decoder installed for %s" % name)

    def encode(self, message, encoding=None):
        content_encoding = 'UTF-8'

        if isinstance(message, string) and not encoding:
            # In Python 3+, this would be "bytes"; allow binary data to be 
            # sent as a message without getting encoder errors
            content_type = 'application/data'
            content_encoding = 'binary'
            payload = message
        elif isinstance(message, unicode) and not encoding: 
            content_type = 'text/plain'
            payload = message      
        elif encoding == 'raw': 
            content_type = 'application/data'
            payload = message
            if isinstance(payload, unicode): 
                payload = utf8_encoder(payload)
            else:
                content_encoding = 'binary'
        else:
            if encoding: 
                mimetype, encoder = self._encoders[encoding]
            else:
                encoder = self._default_encoder
                content_type = self._default_mimetype

        return (content_type, content_encoding, payload)

    def decode(message, content_type, content_encoding):
        content_type = content_type or 'application/data'
        content_encoding = (content_encoding or 'utf-8').lower()

        try:
            decoder = self._decoders[content_type]
        except KeyError: 
            raise DecoderNotInstalled(
                'No decoder installed for content-type: %s' % content_type)

        # Don't decode 8-bit strings
        if content_encoding not in ('binary','ascii-8bit'):
            message = codecs.decode(message, content_encoding)

        return decoder()


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
registry.set_default_encoder('json')

# Register optional encoders, if possible
for optional in (register_hessian, register_yaml): 
    try:
        optional()
    except ImportError: 
        pass


