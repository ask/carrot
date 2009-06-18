#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import unittest
import uuid
sys.path.insert(0, os.pardir)
sys.path.append(os.getcwd())

from carrot.serialization import registry

unicode_string = u'abcdé\u8463'
unicode_string_as_utf8 = unicode_string.encode('utf-8')
latin_string = u'abcdé'
latin_string_as_latin1 = latin_string.encode('latin-1')
latin_string_as_utf8 = latin_string.encode('utf-8')

class TestSerialization(unittest.TestCase):

    def test_content_type_decoding(self):
        content_type = 'plain/text'

        self.assertTrue(unicode_string == registry.decode(
                            unicode_string_as_utf8, 
                            content_type='plain/text', 
                            content_encoding='utf-8'))
        self.assertTrue(latin_string == registry.decode(
                            latin_string_as_latin1, 
                            content_type='application/data', 
                            content_encoding='latin-1'))

    def test_content_type_encoding(self):
        
        self.assertTrue(unicode_string_as_utf8 == registry.encode(
                            unicode_string, serializer="raw")[-1])
        self.assertTrue(latin_string_as_utf8 == registry.encode(
                            latin_string, serializer="raw")[-1])


if __name__ == '__main__':
    unittest.main()