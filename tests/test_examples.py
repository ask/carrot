import os
import sys
import unittest
import uuid
sys.path.insert(0, os.pardir)
sys.path.append(os.getcwd())

from tests.utils import establish_test_connection


class TestExamples(unittest.TestCase):
    
    def setUp(self):
        self.conn = establish_test_connection()

    def test_connection(self):
        self.assertTrue(self.conn)
