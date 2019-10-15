#!/usr/bin/env python2

import unittest
from mock import patch, mock_open

import despydb.oracon as oracon
import despydb.errors as errors

class TestErrors(unittest.TestCase):
    def test_missingDBid(self):
        msg = 'Bad identifier.'
        try:
            raise errors.MissingDBId()
        except errors.MissingDBId as exc:
            self.assertTrue('No database' in exc.args[0])
        try:
            raise errors.MissingDBId(msg)
        except errors.MissingDBId as exc:
            self.assertEqual(msg, exc.args[0])

    def test_unknownDBTypeError(self):
        msg = 'Unknown DB type'
        try:
            raise errors.UnknownDBTypeError('xyz')
        except errors.UnknownDBTypeError as exc:
            self.assertTrue('xyz' in exc.args[0])
        try:
            raise errors.UnknownDBTypeError('xyz', msg)
        except errors.UnknownDBTypeError as exc:
            self.assertEqual(exc.args[0], msg)

    def test_unknownCaseSensitiveError(self):
        msg = 'Bad case value'
        try:
            raise errors.UnknownCaseSensitiveError('xyz')
        except errors.UnknownCaseSensitiveError as exc:
            self.assertTrue('xyz' in exc.args[0])
        try:
            raise errors.UnknownCaseSensitiveError('xyz', msg)
        except errors.UnknownCaseSensitiveError as exc:
            self.assertEqual(exc.args[0], msg)

#class TestOracon(unittest.TestCase):
#    @patch('despydb.oracon.cx_Oracle')
#    def test_init(self, cxo):
#        data = {'user': 'non-user',
#                'passwd': 'non-passwd'}
#        with self.assertRaises()