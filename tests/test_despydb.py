#!/usr/bin/env python2
# pylint: skip-file

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

    @classmethod
    def setUpClass(cls):
        conData = {'user': 'non-user',
                   'passwd': 'non-passwd',
                   'server': 'non-server',
                   'port': 0,
                   'name': 'myDB'}
        cls.con = ora.OracleConnection(conData, True)

    def test_init(self):
        # Note that cx_Oracle.Connection cannot be mocked, so not all paths
        # of init can be tested properly
        data = {'user': 'non-user',
                'passwd': 'non-passwd',
                'server': 'non-server',
                'port': 0}

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

        self.assertRaises(ora.cx_Oracle.DatabaseError, ora.OracleConnection, data)

    def test_get_expr_exec_format(self):
        self.assertTrue('DUAL' in self.con.get_expr_exec_format())

    def test_get_named_bind_string(self):
        name = 'blah'
        self.assertEqual(':' + name, self.con.get_named_bind_string(name))

    def test_get_positional_bind_string(self):
        self.assertEqual(':2', self.con.get_positional_bind_string(2))

    def test_get_regex_format(self):
        self.assertTrue('c' in self.con.get_regex_format(True))
        self.assertTrue('i' in self.con.get_regex_format(False))
        self.assertTrue(self.con.get_regex_format(None).endswith('s)'))
        self.assertRaises(errors.UnknownCaseSensitiveError, self.con.get_regex_format, '')

    def test_get_seq_next_clause(self):
        self.assertTrue(self.con.get_seq_next_clause('name').upper().endswith('NEXTVAL'))

    def test_from_dual(self):
        self.assertTrue('dual' in self.con.from_dual().lower())

    def test_get_current_timestamp_str(self):
        self.assertEqual('SYSTIMESTAMP', self.con.get_current_timestamp_str())

    def test_ping(self):
        self.assertFalse(self.con.ping())

if __name__ == '__main__':
    unittest.main()
