#!/usr/bin/env python2

import unittest
from mock import patch

import despydb.oracon as ora
import despydb.errors as errors
import cx_Oracle as cxo

class TestErrors(unittest.TestCase):
    def test_missingDBid(self):
        msg = 'Bad identifier.'
        try:
            raise errors.MissingDBId()
        except errors.MissingDBId as exc:
            self.assertTrue('No database' in exc.args[0])
        else:
            self.assertTrue(False)
        try:
            raise errors.MissingDBId(msg)
        except errors.MissingDBId as exc:
            self.assertEqual(msg, exc.args[0])
        else:
            self.assertTrue(False)

    def test_unknownDBTypeError(self):
        msg = 'Unknown DB type'
        try:
            raise errors.UnknownDBTypeError('xyz')
        except errors.UnknownDBTypeError as exc:
            self.assertTrue('xyz' in exc.args[0])
        else:
            self.assertTrue(False)
        try:
            raise errors.UnknownDBTypeError('xyz', msg)
        except errors.UnknownDBTypeError as exc:
            self.assertEqual(exc.args[0], msg)
        else:
            self.assertTrue(False)

    def test_unknownCaseSensitiveError(self):
        msg = 'Bad case value'
        try:
            raise errors.UnknownCaseSensitiveError('xyz')
        except errors.UnknownCaseSensitiveError as exc:
            self.assertTrue('xyz' in exc.args[0])
        else:
            self.assertTrue(False)
        try:
            raise errors.UnknownCaseSensitiveError('xyz', msg)
        except errors.UnknownCaseSensitiveError as exc:
            self.assertEqual(exc.args[0], msg)
        else:
            self.assertTrue(False)

class TestOracon(unittest.TestCase):

    def test_init(self):
        data = {'user': 'non-user',
                'passwd': 'non-passwd',
                'server': 'non-server',
                'port': 0}
        with patch('despydb.oracon.cx_Oracle') as ocx:
            with patch('despydb.oracon.cx_Oracle.Connection') as occ:
                with self.assertRaises(errors.MissingDBId):
                    _ = ora.OracleConnection(data)

                data['name'] = 'myDB'
                try:
                    _ = ora.OracleConnection(data)
                except cxo.InterfaceError:
                    pass
                data['sid'] = 123456
                try:
                    _ = ora.OracleConnection(data)
                except cxo.InterfaceError:
                    pass

                data['service'] = 'non-service'
                try:
                    _ = ora.OracleConnection(data)
                except cxo.InterfaceError:
                    pass

                data['threaded'] = True
                try:
                    _ = ora.OracleConnection(data)
                except cxo.InterfaceError:
                    pass

        with patch('despydb.oracon.cx_Oracle.Connection', side_effect=TypeError("'module' is an invalid keyword")) as ptch:
            try:
                _ = ora.OracleConnection(data)
            except cxo.InterfaceError:
                pass

        with patch('despydb.oracon.cx_Oracle.Connection', side_effect=TypeError('')) as ptch:
            try:
                _ = ora.OracleConnection(data)
            except cxo.InterfaceError:
                pass
