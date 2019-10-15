#!/usr/bin/env python2

import unittest
from mock import patch, mock_open

import despydb.oracon as oracon
import despydb.errors as errors

class TestErrors(unittest.TestCase):
    def test_missingDBid(self):
        msg = 'Bad identifier.'
        exc = errors.MissingDBId()
        self.assertTrue('No database' in exc.args[0])
        exc = errors.MissingDBId(msg)
        self.assertEqual(msg, exc.args[0])

    def test_unknownDBTypeError(self):
        msg = 'Unknown DB type'
        exc = errors.UnknownDBTypeError('xyz')
        self.assertTrue('xyz' in exc.args[0])
        exc = errors.UnknownDBTypeError('xyz', msg)
        self.assertEqual(exc.args[0], msg)

    def test_unknownCaseSensitiveError(self):
        msg = 'Bad case value'
        exc = errors.UnknownCaseSensitiveError('xyz')
        self.assertTrue('xyz' in exc.args[0])
        exc = errors.UnknownCaseSensitiveError('xyz', msg)
        self.assertEqual(exc.args[0], msg)

#class TestOracon(unittest.TestCase):
#    @patch('despydb.oracon.cx_Oracle')
#    def test_init(self, cxo):
#        data = {'user': 'non-user',
#                'passwd': 'non-passwd'}
#        with self.assertRaises()