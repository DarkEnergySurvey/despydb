#!/usr/bin/env python2
# pylint: skip-file

import unittest
import sys
import os
import stat
import datetime
from contextlib import contextmanager
from StringIO import StringIO
from mock import patch, MagicMock

from despydb.oracon import OracleConnection, _ORA_NO_TABLE_VIEW, _ORA_NO_SEQUENCE, _TYPE_MAP
import despydb.pgcon as pgcon
import despydb.errors as errors
import despydb.desdbi as desdbi
import cx_Oracle as cxo
import psycopg2
from despydb.pgcon import PostgresConnection
from MockDBI import MockConnection

@contextmanager
def capture_output():
    new_out, new_err = StringIO(), StringIO()
    old_out, old_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = new_out, new_err
        yield sys.stdout, sys.stderr
    finally:
        sys.stdout, sys.stderr = old_out, old_err

class MockOracle(object):
    def __init__(self, user, password, **kwargs):
        self.user = user
        self.passwd = password
        self.value = ()
        self.count = -2

    def setReturn(self, value):
        self.value = value

    class Cursor(object):
        def __init__(self, descr, count):
            self.count = count
            self.description = descr

        class Obj(object):
            def __init__(self, code):
                self.code = code

        def execute(self, sql):
            if self.count >= 0:
                if 'DROP SEQUENCE' in sql.upper():
                    raise cxo.DatabaseError(self.Obj(_ORA_NO_SEQUENCE + self.count))
                elif 'DROP TABLE' in sql.upper():
                    raise cxo.DatabaseError(self.Obj(_ORA_NO_TABLE_VIEW + self.count))
                else:
                    raise Exception('test')

        def close(self):
            pass

    def cursor(self):
        self.count += 1
        return self.Cursor(self.value, self.count)

class MockPostgres(object):
    def __init__(self, user, password, **kwargs):
        self.user = user
        self.passwd = password
        self.value = ()
        self.count = -2

    def setReturn(self, value):
        self.value = value

    class Cursor(object):
        def __init__(self, descr, count):
            self.count = count
            self.description = descr

        class Obj(Exception):
            def __init__(self, code):
                self.pgcode = code
                Exception.__init__(self, "")

        def execute(self, sql):
            if self.count >= 0:
                if 'DROP SEQUENCE' in sql.upper():
                    if self.count == 0:
                        raise psycopg2.InternalError(self.Obj(psycopg2.errorcodes.INSUFFICIENT_PRIVILEGE))
                elif 'DROP TABLE' in sql.upper():
                    raise cxo.DatabaseError(self.Obj(_ORA_NO_TABLE_VIEW + self.count))
                else:
                    raise Exception('test')

        def close(self):
            pass

    def cursor(self):
        self.count += 1
        return self.Cursor(self.value, self.count)

def modifyOracle():
    global OracleConnection
    OracleConnection = type('OracleConnection', (MockOracle, ), dict(OracleConnection.__dict__))

def modifyPostgres():
    global PostgresConnection
    PostgresConnection = type('PostgresConnection', (MockPostgres, ), dict(PostgresConnection.__dict__))

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
    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def setUpClass(cls):
        modifyOracle()
        cls.conData = {'user': 'non-user',
                       'passwd': 'non-passwd',
                       'server': 'non-server',
                       'port': 0,
                       'name': 'myDB'}
        cls.con = OracleConnection(cls.conData)

    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def test_init(self):
        data = {'user': 'non-user',
                'passwd': 'non-passwd',
                'server': 'non-server',
                'port': 0}

        with self.assertRaises(errors.MissingDBId):
            _ = OracleConnection(data)

        data['name'] = 'myDB'
        _ = OracleConnection(data)

        data['sid'] = 123456
        _ = OracleConnection(data)

        data['service'] = 'non-service'
        _ = OracleConnection(data)

        data['threaded'] = True
        _ = OracleConnection(data)

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

    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def test_ping(self):
        con = OracleConnection(self.conData)
        self.assertTrue(con.ping())
        self.assertFalse(con.ping())

    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def test_getColumn_types(self):
        con = OracleConnection(self.conData)
        data = (('DATE', cxo.DATETIME),
                ('NAME', cxo.STRING),
                ('COUNT', cxo.NUMBER))
        con.setReturn(data)
        rt = con.get_column_types('table')
        self.assertEqual(rt['date'], datetime.datetime)
        self.assertEqual(rt['name'], str)
        self.assertEqual(rt['count'], float)

    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def test_sequence_drop(self):
        con = OracleConnection(self.conData)
        con.sequence_drop('MYSEQ')
        con.sequence_drop('MYSEQ')
        self.assertRaises(cxo.DatabaseError, con.sequence_drop, 'MYSEQ')

    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def test_table_drop(self):
        con = OracleConnection(self.conData)
        con.table_drop('MYTABLE')
        con.table_drop('MYTABLE')
        self.assertRaises(cxo.DatabaseError, con.table_drop, 'MYTABLE')


class TestDesdbi(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        open(cls.sfile, 'w').write("""

[db-maximal]
PASSWD  =   maximal_passwd
name    =   maximal_name_1    ; if repeated last name wins
user    =   maximal_name      ; if repeated key, last one wins
Sid     =   maximal_sid       ;comment glued onto value not allowed
type    =   POSTgres
server  =   maximal_server

[db-minimal]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   oracle

[db-test]
USER    =   Minimal_user
PASSWD  =   Minimal_passwd
name    =   Minimal_name
sid     =   Minimal_sid
server  =   Minimal_server
type    =   test
port    =   0
""")
        os.chmod(cls.sfile, (0xffff & ~(stat.S_IROTH | stat.S_IWOTH | stat.S_IRGRP | stat.S_IWGRP )))
        cls.dbh = desdbi.DesDbi(cls.sfile, 'db-test')


    @classmethod
    def tearDownClass(cls):
    #    cls.dbh.teardown()
        os.unlink(cls.sfile)

    def test_init(self):
        dbh = desdbi.DesDbi(connection=self.dbh)

        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        with patch('despydb.oracon'):
            dbh = desdbi.DesDbi(self.sfile, 'db-minimal')

        self.assertRaises(errors.UnknownDBTypeError, desdbi.DesDbi, self.sfile, 'db-maximal')

        with patch('despydb.oracon.OracleConnection', side_effect=Exception()):
            self.assertRaises(Exception, desdbi.DesDbi, self.sfile, 'db-minimal', True)

        with patch('despydb.oracon.OracleConnection', side_effect=[Exception(), MagicMock]):
            dbh = desdbi.DesDbi(self.sfile, 'db-minimal', True)

    def test_with(self):
        with self.assertRaises(Exception):
            with patch('despydb.oracon'):
                with desdbi.DesDbi(self.sfile, 'db-minimal') as dbh:
                    raise Exception()

        with patch('despydb.oracon'):
            dbh = desdbi.DesDbi(self.sfile, 'db-minimal')
            with desdbi.DesDbi(connection=dbh) as dbh2:
                pass
"""
class TestPgcon(unittest.TestCase):

    @classmethod
    @patch('despydb.pgcon.psycopg2.extensions.connection', MockPostgres)
    def setUpClass(cls):
        modifyPostgres()
        conData = {'user': 'non-user',
                   'passwd': 'non-passwd',
                   'server': 'non-server',
                   'port': 0,
                   'name': 'myDB'}
        cls.con = pgcon.PostgresConnection(conData)

    def test_get_expr_exec_format(self):
        self.assertTrue('SELECT' in self.con.get_expr_exec_format())

    def test_get_named_bind_string(self):
        name = 'blah'
        self.assertTrue('%%' in self.con.get_named_bind_string(name))

    def test_get_positional_bind_string(self):
        self.assertEqual('2', self.con.get_positional_bind_string(2))

    def test_get_regex_format(self):
        self.assertTrue('~' in self.con.get_regex_format(True))
        self.assertTrue('~*' in self.con.get_regex_format(False))
        self.assertTrue('~' in self.con.get_regex_format(None))
        self.assertRaises(errors.UnknownCaseSensitiveError, self.con.get_regex_format, '')

    def test_get_seq_next_clause(self):
        self.assertTrue(self.con.get_seq_next_clause('name').upper().startswith('NEXTVAL'))

    def test_from_dual(self):
        self.assertTrue('dual' in self.con.from_dual().lower())

    def test_get_current_timestamp_str(self):
        self.assertEqual('SYSTIMESTAMP', self.con.get_current_timestamp_str())

    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def test_ping(self):
        con = OracleConnection(self.conData)
        self.assertTrue(con.ping())
        self.assertFalse(con.ping())

    @patch('despydb.pgcon.psycopg2.extensions.connection', MockPostgres)
    def test_getColumn_types(self):
        con = OracleConnection(self.conData)
        data = (('DATE', psycopg2.DATETIME),
                ('NAME', psycopg2.STRING),
                ('COUNT', psycopg2.extensions.FLOAT))
        con.setReturn(data)
        rt = con.get_column_types('table')
        self.assertEqual(rt['date'], datetime.datetime)
        self.assertEqual(rt['name'], str)
        self.assertEqual(rt['count'], float)

    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def test_sequence_drop(self):
        con = OracleConnection(self.conData)
        con.sequence_drop('MYSEQ')
        con.sequence_drop('MYSEQ')
        self.assertRaises(cxo.DatabaseError, con.sequence_drop, 'MYSEQ')

    @patch('despydb.oracon.cx_Oracle.Connection', MockOracle)
    def test_table_drop(self):
        con = OracleConnection(self.conData)
        con.table_drop('MYTABLE')
        con.table_drop('MYTABLE')
        self.assertRaises(cxo.DatabaseError, con.table_drop, 'MYTABLE')
"""


if __name__ == '__main__':
    unittest.main()
