#!/usr/bin/env python2

import unittest
from mock import patch

import despydb.oracon as ora
import despydb.errors as errors
#import cx_Oracle as cxo

def raiseMissing():
    raise errors.MissingDBId()

def raiseMissingMsg(msg):
    raise errors.MissingDBId(msg)

def raiseUnknown():
    raise errors.UnknownDBTypeError('xyz')

def raiseUnknownMsg(msg):
    raise errors.UnknownDBTypeError('xyz', msg)

def raiseUnCase():
    raise errors.UnknownCaseSensitiveError('xyz')

def raiseUnCaseMsg(msg):
    raise errors.UnknownCaseSensitiveError('xyz', msg)

class TestErrors(unittest.TestCase):
    def test_missingDBid(self):
        msg = 'Bad identifier.'
        self.assertRaisesRegexp(errors.MissingDBId, 'No database', raiseMissing)
        self.assertRaisesRegexp(errors.MissingDBId, msg, raiseMissingMsg, msg)


    def test_unknownDBTypeError(self):
        msg = 'Unknown DB type'
        self.assertRaisesRegexp(errors.UnknownDBTypeError, 'xyz', raiseUnknown)
        self.assertRaisesRegexp(errors.UnknownDBTypeError, msg, raiseUnknownMsg, msg)

    def test_unknownCaseSensitiveError(self):
        msg = 'Bad case value'
        self.assertRaisesRegexp(errors.UnknownCaseSensitiveError, 'xyz', raiseUnCase)
        self.assertRaisesRegexp(errors.UnknownCaseSensitiveError, msg, raiseUnCaseMsg, msg)


class TestOracon(unittest.TestCase):

    def test_init(self):
        data = {'user': 'non-user',
                'passwd': 'non-passwd',
                'server': 'non-server',
                'port': 0}
        #with patch('despydb.oracon.cx_Oracle') as ocx:
        #    with patch('despydb.oracon.cx_Oracle.Connection') as occ:
        with self.assertRaises(errors.MissingDBId):
            _ = ora.OracleConnection(data, True)

        data['name'] = 'myDB'
        _ = ora.OracleConnection(data, True)

        data['sid'] = 123456
        _ = ora.OracleConnection(data, True)

        data['service'] = 'non-service'
        _ = ora.OracleConnection(data, True)

        data['threaded'] = True
        _ = ora.OracleConnection(data, True)

        #with patch('despydb.oracon.cx_Oracle.Connection', side_effect=TypeError("'module' is an invalid keyword")) as ptch:
        #    try:
        #        _ = ora.OracleConnection(data)
        #    except cxo.InterfaceError:
        #        pass

        #with patch('despydb.oracon.cx_Oracle.Connection', side_effect=TypeError('')) as ptch:
        #    try:
        #        _ = ora.OracleConnection(data)
        #    except cxo.InterfaceError:
        #        pass

if __name__ == '__main__':
    unittest.main()
