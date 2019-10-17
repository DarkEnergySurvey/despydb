"""
    Module for mocking a des db connection
"""
# pylint: skip-file

import sqlite3

import datetime
import time
import socket
import os
import inspect
import glob

import shutil

import despydb.errors as errors

# Construct a name for the v$session module column to allow database auditing.

import __main__
try:
    _MODULE_NAME = __main__.__file__
except AttributeError:
    _MODULE_NAME = "unavailable"

if _MODULE_NAME.rfind('/') > -1:
    _MODULE_NAME = _MODULE_NAME[_MODULE_NAME.rfind('/') + 1:]
_MODULE_NAME += '@' + socket.getfqdn()
_MODULE_NAME = _MODULE_NAME [:48]

DB_FILE = 'des_test_db.db'
# No need to rebuild this mapping every time it is used, so make it a global
# module object.
_TYPE_MAP = {'TEXT'      : str,
             'INTEGER'   : int,
             'NUMBER'    : float,
             'REAL'      : float,
             'BLOB'      : bytearray,
             'TIMESTAMP' : datetime.datetime,
             'DATE'      : datetime.datetime
            }

# Define some symbolic names for oracle error codes to make it clearer what
# the error codes mean.

_ORA_NO_TABLE_VIEW = 942    # table or view does not exist
_ORA_NO_SEQUENCE = 2289   # sequence does not exist

def adapt_timestamp(data):
    """ convert a datetime to a timestamp string """
    return str(time.mktime(data.timetuple()))

def convert_timestamp(data):
    """ convert a timestamp to a datetime object """
    return datetime.datetime.fromtimestamp(data)

class MockCursor(sqlite3.Cursor):
    def __init__(self, *args, **kwargs):
        self._stmt = None
        sqlite3.Cursor.__init__(self, *args, **kwargs)

    def prepare(self, stmt):
        self._stmt = stmt

    def execute(self, stmt, params=()):
        if stmt:
            return super(MockCursor, self).execute(stmt, params)
        return super(MockCursor, self).execute(self._stmt, params)


class MockConnection(sqlite3.Connection):
    """
    Provide cx_Oracle-specific implementations of canonical database methods

    Refer to desdbi.py for full method documentation.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize an OracleConnection object

        Connect the OracleConnection instance to the database identified in
        access_data.

        """
        self.haveExpr = False
        self.pingval = True
        self.module = _MODULE_NAME
        self.type = 'MOCKDB'
        self.configdict = {'user': 'non-user',
                           'passwd': 'non-passwd',
                           'meta_file': 'non-file',
                           'meta_section': 'non-section'}
        sqlite3.register_adapter(datetime.datetime, adapt_timestamp)
        sqlite3.register_converter('TIMESTAMP', convert_timestamp)
        sqlite3.register_converter('DATE', convert_timestamp)

        filename = inspect.getframeinfo(inspect.currentframe()).filename
        self.home_dir = os.path.dirname(os.path.abspath(filename))

        needSetup = False

        if not os.path.exists(os.path.join(self.home_dir, DB_FILE)):
            needSetup = True
        elif not os.path.isfile(os.path.join(self.home_dir, DB_FILE)):
            shutil.rmtree(os.path.join(self.home_dir, DB_FILE))
            needSetup = True
        sqlite3.Connection.__init__(self, database=os.path.join(self.home_dir, DB_FILE),
                                   detect_types=sqlite3.PARSE_DECLTYPES,
                                   check_same_thread=False)
        self._autocommit = False
        if needSetup:
            self.setup()


    @property
    def autocommit(self):
        if self.isolation_level is None:
            return True
        return False

    @autocommit.setter
    def autocommit(self, val):
        if val:
            self.isolation_level = None
        else:
            self.isolation_level = ''

    def setup(self):
        """ initialize the DB """
        print "Creating test database..."
        files = glob.glob(os.path.join(self.home_dir, 'sqlFiles', '*.sql'))
        for fls in files:
            loc = fls.rfind('/')
            print "   " + fls.replace('.sql', '')[loc + 1:]
            flh = open(fls, 'r')
            curs = self.cursor()
            curs.executescript(flh.read())
            self.commit()
            curs.close()
            flh.close()

    def teardown(self):
        self.close()
        os.unlink(os.path.join(self.home_dir, DB_FILE))

    def cursor(self, fetchsize=None):
        """
        Return a cx_Oracle Cursor object for operating on the connection.

        The fetchsize argument is ignored, but retained for compatibility
        with other connection types.
        """

        # cx_Oracle doesn't implement/need named cursors, so ignore fetchsize.

        return MockCursor(self)

    def get_column_types(self, table_name):
        """
        Return a dictionary of python types indexed by column name for a table.
        """

        curs = self.cursor()
        curs.execute('PRAGMA table_info(%s)' % table_name)

        types = {str(d[1].lower()): _TYPE_MAP[d[2].split()[0]] for d in curs.fetchall()}

        curs.close()

        return types

    def get_expr_exec_format(self):
        """Return a format string for a statement to execute SQL expressions."""
        if self.haveExpr:
            self.haveExpr = False
            return '%s'
        return 'SELECT %s FROM DUMMY'

    def get_named_bind_string(self, name):
        """Return a named bind (substitution) string for name with cx_Oracle."""

        return ":" + name

    def get_positional_bind_string(self, pos=1):
        "Return a positional bind (substitution) string for cx_Oracle."

        return "?"

    def get_regex_format(self, case_sensitive=True):
        """
        Return a format string for constructing a regular expression clause.

        See DesDbi class for detailed documentation.
        """

        if case_sensitive is True:
            c = self.cursor()
            c.execute('PRAGMA case_sensitive_like=true')
        elif case_sensitive is False:
            c = self.cursor()
            c.execute('PRAGMA case_sensitive_like=false')
        elif case_sensitive is None:
            pass
        else:
            raise errors.UnknownCaseSensitiveError(value=case_sensitive)

        return "%(target)s REGEXP %(pattern)s"

    def get_seq_next_clause(self, seqname):
        "Return an SQL expression that extracts the next value from a sequence."

        c = self.cursor()
        c.execute("select junk from dummy where name='%s'" % seqname)
        val = c.fetchone()[0]
        c.execute("update dummy set junk=%i where name='%s'" % (val + 1, seqname))
        self.commit()
        self.haveExpr = True
        return "select junk from dummy where name='%s'" % seqname

    def sequence_drop(self, seq_name):
        "Drop sequence; do not generate error if it doesn't exist."
        return

    def table_drop(self, table):
        "Drop table; do not generate error if it doesn't exist."

        stmt = 'DROP TABLE %s' % table

        curs = self.cursor()
        try:
            curs.execute(stmt)
        except sqlite3.Error:
            raise
        finally:
            curs.close()

    def from_dual(self):
        return ""

    def get_current_timestamp_str(self):
        return str(time.mktime(datetime.datetime.now().timetuple()))

    def ping(self):
        return self.pingval
