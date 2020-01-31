#!/usr/bin/env python3
# pylint: skip-file

import unittest
from copy import deepcopy
import sys
import os
import stat
import datetime
import sqlite3
from contextlib import contextmanager
from io import StringIO
from mock import patch, MagicMock
from subprocess import Popen, PIPE, STDOUT

from despydb.oracon import OracleConnection, _ORA_NO_TABLE_VIEW, _ORA_NO_SEQUENCE, _TYPE_MAP
import despydb.errors as errors
import despydb.desdbi as desdbi
import cx_Oracle as cxo
import query
from MockDBI import MockConnection, convert_timestamp

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

"""
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
"""
def modifyOracle():
    global OracleConnection
    OracleConnection = type('OracleConnection', (MockOracle, ), dict(OracleConnection.__dict__))

#def modifyPostgres():
#    global PostgresConnection
#    PostgresConnection = type('PostgresConnection', (MockPostgres, ), dict(PostgresConnection.__dict__))

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

class TestQuery(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sfile = 'services.ini'
        cls.qry = "select * from OPS_ARCHIVE_VAL"

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
        cls.dbh.con.teardown()
        os.unlink(cls.sfile)

    def test_query(self):
        argv = deepcopy(sys.argv)
        sys.argv = ['query.py', '--section', 'db-test', self.qry]
        with capture_output() as (out, _):
            query.main()
            output = out.getvalue().strip()
            self.assertTrue('query took' in output)
            self.assertTrue('desar2home' in output)
        sys.argv = deepcopy(argv)

    def test_query_stdin(self):
        #my_env = os.environ.copy()
        argv = ['query.py', '--section', 'db-test','-']
        prc = Popen(argv, stdin=PIPE, stderr=STDOUT, stdout=PIPE, text=True)
        output = prc.communicate('#\n' + self.qry + '\nEND\n', timeout=15)
        self.assertTrue('query took' in output[0])
        self.assertTrue('desar2home' in output[0])

        argv = ['query.py', '--section', 'db-test','+']
        prc = Popen(argv, stdin=PIPE, stderr=STDOUT, stdout=PIPE, text=True)
        output = prc.communicate(self.qry, timeout=15)
        self.assertTrue('query took' in output[0])
        self.assertTrue('desar2home' in output[0])


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
        cls.dbh.con.teardown()
        os.unlink(cls.sfile)

    def test_init(self):
        #dbh = desdbi.DesDbi(connection=self.dbh)

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

    def test_ping(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        self.assertTrue(dbh.ping())
        dbh.close()
        self.assertFalse(dbh.ping())

    def test_reconnect(self):
        dbh = desdbi.DesDbi(self.sfile, 'db-test')
        self.assertTrue(dbh.ping())
        with capture_output() as (out, _):
            dbh.reconnect()
            output = out.getvalue().strip()
            self.assertTrue('still good' in output)
        dbh.close()
        self.assertFalse(dbh.ping())
        dbh.reconnect()
        self.assertTrue(dbh.ping())
        dbh.close()

    def test_autocommit(self):
        self.assertFalse(self.dbh.autocommit(True))
        self.assertTrue(self.dbh.autocommit(False))
        self.assertFalse(self.dbh.autocommit())

    def test_cursor(self):
        self.assertTrue(isinstance(self.dbh.cursor(), sqlite3.Cursor))

    def test_exec_sql_expression(self):
        res = self.dbh.exec_sql_expression(['name','junk'])
        self.assertEqual(len(res), 2)
        res = self.dbh.exec_sql_expression('name,junk')
        self.assertEqual(len(res), 2)

    def test_get_column_metadata(self):
        res = self.dbh.get_column_metadata('exposure')
        self.assertTrue('expnum' in res.keys())
        self.assertTrue('nite' in res.keys())

    def test_get_column_lengths(self):
        res = self.dbh.get_column_lengths('exposure')
        for _, val in res.items():
            self.assertIsNone(val)  #sqlite databases do not have column lengths

    def test_get_column_names(self):
        res = self.dbh.get_column_names('exposure')
        self.assertEqual(len(res), 56)
        self.assertTrue('expnum' in res)
        self.assertTrue('nite' in res)

    def test_get_column_types(self):
        res = self.dbh.get_column_types('exposure')
        self.assertEqual(res['telescope'], str)
        self.assertEqual(res['mjd_obs'], float)

    def test_get_named_bind_string(self):
        self.assertTrue('blah' in self.dbh.get_named_bind_string('blah'))

    def test_get_positional_bind_string(self):
        self.assertTrue('?' in self.dbh.get_positional_bind_string(5))

    def test_get_regex_clause(self):
        res = self.dbh.get_regex_clause('col1', '*.fits')
        self.assertTrue('col1' in res)

    def test_get_seq_next_clause(self):
        self.assertTrue(isinstance(self.dbh.get_seq_next_clause('DESFILE_SEQ'), str))

    def test_get_seq_next_value(self):
        res1 = self.dbh.get_seq_next_value('DESFILE_SEQ')
        res2 = self.dbh.get_seq_next_value('DESFILE_SEQ')
        self.assertEqual(res2-res1, 1)

    def test_insert_many(self):
        self.dbh.autocommit(False)
        self.dbh.insert_many('none', 'none', [])
        self.dbh.insert_many('exposure', ['expnum','nite'], [(12345, '20190809'), (234567, '20190809')])
        self.dbh.insert_many('exposure', ['expnum','nite'], [{'expnum': 12345, 'nite': '20190810'}, {'expnum': 234567, 'nite': '20190810'}])
        self.dbh.commit()
        c = self.dbh.cursor()
        c.execute('select * from exposure where nite="20190809"')
        self.assertEqual(len(c.fetchall()), 2)
        c.execute('select * from exposure where nite="20190810"')
        self.assertEqual(len(c.fetchall()), 2)
        c.close()

    def test_insert_many_indiv(self):
        self.dbh.autocommit(False)
        self.dbh.insert_many_indiv('none', 'none', [])
        self.dbh.insert_many_indiv('exposure', ['expnum','nite'], [(12345, '20190811'), (234567, '20190811')])
        self.dbh.insert_many_indiv('exposure', ['expnum','nite'], [{'expnum': 12345, 'nite': '20190812'}, {'expnum': 234567, 'nite': '20190812'}])
        self.dbh.commit()
        c = self.dbh.cursor()
        c.execute('select * from exposure where nite="20190811"')
        self.assertEqual(len(c.fetchall()), 2)
        c.execute('select * from exposure where nite="20190812"')
        self.assertEqual(len(c.fetchall()), 2)
        c.close()
        self.assertRaises(Exception, self.dbh.insert_many_indiv, 'exposure2', ['expnum','nite'], [{'expnum': 12345, 'nite': '20190812'}, {'expnum': 234567, 'nite': '20190812'}])

    def test_query_simple(self):
        self.assertRaises(TypeError, self.dbh.query_simple, None)
        res = self.dbh.query_simple('OPS_ARCHIVE_VAL')
        self.assertEqual(len(res), 5)
        res = self.dbh.query_simple('OPS_ARCHIVE_VAL', ['name','val'], ["key='endpoint'", "val='desar2'"])
        self.assertEqual(len(res), 1)
        res = self.dbh.query_simple('OPS_ARCHIVE_VAL', "name,val", "key='endpoint'")
        self.assertEqual(len(res), 1)
        res = self.dbh.query_simple('OPS_ARCHIVE_VAL', orderby="key")
        self.assertEquals(len(res), 5)
        self.assertEquals(res[0]['key'], 'endpoint')
        res = self.dbh.query_simple('OPS_DATAFILE_METADATA', 'filetype,attribute_name', orderby=["filetype, attribute_name"])
        self.assertEqual(res[0]['attribute_name'], 'a_image')
        self.assertRaises(TypeError, self.dbh.query_simple, 'OPS_ARCHIVE_VAL', None)

        res = self.dbh.query_simple('OPS_ARCHIVE_VAL', ['name','val'], ["key=:1", "val=:2"], params=('endpoint','desar2'))
        self.assertEqual(len(res), 1)

        self.assertTrue(isinstance(self.dbh.query_simple('OPS_ARCHIVE_VAL', "name,val", "key='endpoint'", rowtype=tuple)[0], tuple))
        self.assertTrue(isinstance(self.dbh.query_simple('OPS_ARCHIVE_VAL', "name,val", "key='endpoint'", rowtype=list)[0], list))

    def test_sequence_drop(self):
        cur = self.dbh.cursor()
        cur.execute("select count(*) from dummy where name='PFW_EXEC_SEQ'")
        count = cur.fetchone()[0]
        self.dbh.sequence_drop('PFW_EXEC_SEQ')
        cur.execute("select count(*) from dummy where name='PFW_EXEC_SEQ'")
        newcount = cur.fetchone()[0]
        self.assertEqual(count - newcount, 1)
        cur.close()

    def test_table_drop(self):
        cur = self.dbh.cursor()
        cur.execute('select * from image')
        cur.close()
        self.dbh.table_drop('image')
        #cur.execute("commit")
        cur = self.dbh.cursor()
        self.assertRaises(sqlite3.OperationalError, cur.execute, 'select * from image')

    def test_is_oracle(self):
        self.assertFalse(self.dbh.is_oracle())

        with patch('despydb.oracon'):
            dbh = desdbi.DesDbi(self.sfile, 'db-minimal')
            self.assertTrue(dbh.is_oracle())

    def test_rollback(self):
        cur = self.dbh.cursor()
        cur.execute("select count(*) from proctag where tag='testtag'")
        self.assertEqual(cur.fetchone()[0], 0)
        self.dbh.basic_insert_row('proctag', {'tag': 'testtag', 'created_date': self.dbh.get_current_timestamp_str()})
        cur.execute("select count(*) from proctag where tag='testtag'")
        self.assertEqual(cur.fetchone()[0], 1)
        self.dbh.rollback()
        cur.execute("select count(*) from proctag where tag='testtag'")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_from_dual(self):
        self.assertTrue(isinstance(self.dbh.from_dual(), str))

    def test_which_services_file(self):
        self.assertEqual(self.dbh.which_services_file(), self.sfile)

    def test_which_services_section(self):
        self.assertEqual(self.dbh.which_services_section(), 'db-test')

    def test_quote(self):
        res = self.dbh.quote("'hello'")
        self.assertEqual(res.count("'"), 6)

    def test_query_results_dict(self):
        res = self.dbh.query_results_dict('select name, junk from dummy', 'name')
        self.assertTrue('desfile_seq', res.keys())

    def test_basic_insert_row(self):
        cur = self.dbh.cursor()
        cur.execute("select count(*) from proctag where tag='testtag'")
        self.assertEqual(cur.fetchone()[0], 0)
        self.dbh.basic_insert_row('proctag', {'tag': 'testtag', 'created_date': self.dbh.get_current_timestamp_str()})
        cur.execute("select count(*) from proctag where tag='testtag'")
        self.assertEqual(cur.fetchone()[0], 1)
        self.assertRaises(Exception, self.dbh.basic_insert_row, 'blah', {'tag': 'testtag', 'created_date': self.dbh.get_current_timestamp_str()})
        cur.close()
        self.dbh.rollback()

    def test_basic_update_row(self):
        cur = self.dbh.cursor()
        cur.execute("select junk from dummy where name='TASK_SEQ'")
        self.assertFalse(cur.fetchone()[0] == 86)
        self.dbh.basic_update_row('dummy', {'junk': 86}, {'name': 'TASK_SEQ'})
        cur.execute("select junk from dummy where name='TASK_SEQ'")
        self.assertTrue(cur.fetchone()[0] == 86)
        now = datetime.datetime.now()
        date_str = "TO_DATE('%i-%02i-%02i %02i:%02i:%.4f', 'YYYY-MM-DD HH24:MI:SS.S')" % (now.year, now.month,
                                                                                          now.day, now.hour,
                                                                                          now.minute, now.second
                                                                                          )
        self.dbh.basic_update_row('dummy', {'junk': date_str}, {'name': 'TASK_SEQ'})
        cur.execute("select junk from dummy where name='TASK_SEQ'")
        self.assertTrue(abs((convert_timestamp(cur.fetchone()[0])-now).total_seconds()) < 1.0)
        self.assertRaisesRegexp(Exception, '0 rows', self.dbh.basic_update_row, 'dummy', {'junk': 86}, {'name': None})

        self.assertRaises(Exception, self.dbh.basic_update_row, 'dummy2', {'junk': 89}, {'name': 'TASK_SEQ'})

if __name__ == '__main__':
    unittest.main()
