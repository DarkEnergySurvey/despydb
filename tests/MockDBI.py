"""
    Module for mocking a DES db connection.

    This module will create an sqlite database in order to mock the real DES oracle database for
    local testing purposes. It is instantiated and used in the same fashion as the oracle database
    via despydb.desdbi.DesDbi or one of its subclasses. Sqlite3 databases in python are effectively
    single threaded due to file locking etc in the sqlite3. This module attempts to mimic
    multi-thread access to the database by instantiating a singleton for the actual interface. But it
    is not perfect and may not always behave as expected.
"""
import sqlite3

import datetime
import time
import socket
import os
import inspect
import glob
import re

import shutil
from dateutil import parser

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

_MATERIALIZE = r'/\*\s*\+\s*materialize\s*\*/'
_COADD_IMAGE_QUERY = 'COADD_IMAGE_QUERY'
_COADD_TILE_QUERY = 'COADD_TILE_QUERY'

# Define some symbolic names for oracle error codes to make it clearer what
# the error codes mean.

_ORA_NO_TABLE_VIEW = 942    # table or view does not exist
_ORA_NO_SEQUENCE = 2289   # sequence does not exist

def adapt_timestamp(data):
    """ Convert a datetime to a timestamp string

        Parameters
        ----------
        data : datetime
            The datetime to be converted

        Returns
        -------
        str of the input datetime, in timestamp format
    """
    return str(time.mktime(data.timetuple()))

def convert_timestamp(data):
    """ Convert a timestamp to a datetime object

        Parameters
        ----------
        date : str
            The timestamp to be converted into a datetime

        Returns
        -------
        datetime object representing the input
    """
    return datetime.datetime.fromtimestamp(float(data))

def find_balance(stmt, start=0):
    """ Function to find the outermost set of balanced parentheses.

        Parameters
        ----------
        stmt: str
            The string to look for the balanced parentheses in.

        start: int
            The index to start at. Default is 0.

        Returns
        -------
        int indicating the index + 1 of the location of the closing parentheses. Useful in slicing
        an array.
    """
    open_list = ['(']
    close_list = [')']
    stack = []
    found = False
    for i, val in enumerate(stmt[start:]):
        if val in open_list:
            stack.append(val)
            found = True
        elif val in close_list:
            pos = close_list.index(val)
            if (stack and open_list[pos] == stack[len(stack) - 1]):
                stack.pop()
            else:
                raise Exception("Unbalanced parentheses found")
            if found and not stack:
                return i + start + 1
    raise Exception("Unbalanced parentheses found")

def determine_temp_table(stmt):
    """ Determine which special temp table is being used. This is to mimic the Oracle
        materialize hint.

    """
    if re.search('filename', stmt, re.IGNORECASE) is not None:
        return _COADD_IMAGE_QUERY
    if re.search('tilename', stmt, re.IGNORECASE) is not None:
        return _COADD_TILE_QUERY
    raise Exception('Unknown materialize table type')

def find_columns(stmt):
    """ Determine the column names from the query.
    """
    columns = []
    tstmt = stmt.upper()
    parse = tstmt[tstmt.find("SELECT") + 6:tstmt.find("FROM")].strip().split(',')
    for p in parse:
        if ' AS ' in p:
            temp = p.split()
            temp.reverse()
            for i, val in enumerate(temp):
                if val == 'AS':
                    columns.append(temp[i - 1].strip())
                    break
        elif re.search(r"\D+\.\D+", p) is not None:
            columns.append(p.split('.')[-1].strip())
        else:
            columns.append(p)
    return columns

def process_aggregates(stmt):
    tstmt = stmt.upper()
    loc = tstmt.find('LISTAGG')
    asloc = tstmt.find(' AS ', loc)
    if asloc <= loc:
        raise Exception('Syntax error')
    if loc < tstmt.find(' WITHIN GROUP', loc) < asloc:
        stmt = stmt[:tstmt.find(' WITHIN GROUP')] + stmt[asloc:]
    stmt = re.sub('listagg', 'GROUP_CONCAT', stmt, re.IGNORECASE)
    return stmt

def convert_PROCEDURES(stmt):
    """ Function to convert Oracle TO_DATE statement into a timestamp useable
        by sqlite3.

        Parameters
        ----------
        stmt : str
            The sql statement to work on

        Returns
        -------
        str
            The sql statement with the appropriate replacements made.
    """
    if not isinstance(stmt, (str, bytes)):
        return stmt
    if isinstance(stmt, bytes):
        stmt = stmt.decode()
    for item in ['TO_DATE', 'TO_TIMESTAMP']:
        loc = stmt.find(item)
        while loc > -1:
            end = find_balance(stmt, loc)
            repl = stmt[loc:end]
            orig = stmt[loc:end]
            repl = repl[repl.find('(') + 1: -1].strip()
            sp = repl.split(',')

            sp.reverse()
            dstr = sp.pop()
            if len(sp) != 2:
                try:
                    while dstr.count("'")%2 != 0:
                        dstr += sp.pop()
                except IndexError:
                    raise Exception('Unbalanced quotes in expresion.')
            dstr = dstr.strip()
            dval = ''
            if dstr.find("'") < 0 and dstr.find('"') < 0:
                if dstr.startswith(':'):
                    dval = dstr
            else:
                dstr = dstr.replace("'", '')
                dval = adapt_timestamp(parser.parse(dstr))
            stmt = stmt.replace(orig, dval)
            loc = stmt.find(item)

    for item in ['NULLCMP', 'nullcmp']:
        loc = stmt.find(item)
        while loc > -1:
            end = find_balance(stmt, loc)
            repl = stmt[loc:end]
            orig = stmt[loc:end]
            repl = repl[repl.find('(') + 1: -1].strip()
            sp = repl.split(',')
            dval = 'CASE\n    WHEN ' + sp[0] + ' is NULL and ' + sp[1] + ' is NULL\n        THEN 1 \n'
            dval += '    WHEN ' + sp[0] + ' = ' + sp[1] + '\n        THEN 1\n'
            dval += '    ELSE 0\n'
            dval += 'END '
            stmt = stmt.replace(orig, dval)
            loc = stmt.find(item)

    if '||' in stmt:
        temp = stmt.split('||')
        # may need something more complex
        t1 = temp[0].split(',')[-1]
        t1 = t1.split()[-1]
        t2 = temp[1].split(',')[0]
        t2 = t2.split()[0]
        nstmt = "COALESCE(" + t1 + ", '') || COALESCE(" + t2 + ", '')"
        loc1 = stmt.find(t1)
        loc2 = stmt.find(t2) + len(t2)
        orig = stmt[loc1: loc2]
        stmt = stmt.replace(orig, nstmt)
    if 'dual where exists' in stmt.lower():
        tmp = stmt.lower().find('exists')
        loc = stmt.lower().find('(', tmp)
        orig = stmt[loc:]
        orig = orig.replace('(', '', 1)
        orig = orig.replace(')', '', 1)
        stmt = orig
    #print(stmt)
    return stmt

class Slot(int):
    """ Class to store an integer value
    """
    def __init__(self):
        self.value = None

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return str(self.value)

class MockCursor(sqlite3.Cursor):
    """ Class to mock cursor interaction. Can be used just like an oracle cursor. Oracle specific
        commands are interpreted and converted to sqlite versions, although this is not guaranteed.
        DES oracle procedures are also converted.

        The fail argument is used to force a failure in certain proceedures. All other arguments are
        passed to the sqlite3.Cursor.__init__
    """
    repl = {'nvl(': 'ifnull(',
            'NVL(': 'ifnull('}

    def __init__(self, *args, **kwargs):
        self._stmt = None
        self.fail = False
        if 'fail' in kwargs.keys():
            self.fail = kwargs['fail']
            del kwargs['fail']
        if 'results' in kwargs.keys():
            self._results = kwargs['results']
            del kwargs['results']
        sqlite3.Cursor.__init__(self, *args, **kwargs)
        self._slot = -1
        self.num = None

    def fetchall(self):
        """ Get all results from the last query

            Returns
            -------
            tuple of tuples, one for each row.
        """
        if self._results is not None:
            return self._results
        if self.num is None:
            return super(MockCursor, self).fetchall()
        retv = []
        for i in range(1, self.num + 1):
            retv.append((i,))
        self.num = None
        return retv

    def clearResults(self):
        self._results = None

    def setResults(self, results):
        self._results = results

    def prepare(self, stmt):
        """ This mimics the typical prepare call of other database types.

            Parameters
            ----------
            stmt: str
                The statement to prepare for execution later.

        """
        exstmt = self.replacevals(stmt)
        self._stmt = convert_PROCEDURES(exstmt)

    def process_materialize(self, stmt):
        """ Convert materialize statements into temporary table statements
        """
        tstmt = re.sub(r'\s+', ' ', stmt)
        while re.search(_MATERIALIZE, tstmt, re.IGNORECASE) is not None:
            loc = find_balance(tstmt)
            mat = tstmt[:loc - 1]
            tstmt = tstmt[loc:]
            l2 = mat.find('(')
            pre = mat[:l2]
            mat = mat[l2 + 1:]
            pres = pre.split()
            mat = re.sub(_MATERIALIZE, '', mat)
            if pres[-1] != 'as':
                raise Exception('Unexpected materialize format')
            subname = pres[-2]
            tablename = determine_temp_table(mat)
            columns = find_columns(mat)
            sql = f"INSERT INTO {tablename} ({','.join(columns)}) {mat}"
            self.execute(f"delete from {tablename}")
            self.execute(sql)
            tstmt = re.sub(fr'{subname}(,|\s+)', fr'{tablename} {subname}\1', tstmt, re.IGNORECASE)
        return tstmt


    def replacevals(self, stmt):
        """ Replace specific values in a string with their sqlite3 equlvalents
            The following are currently handled:
            - some unions
            - getting the next sequence value
            - nvl and NVL

            Parameters
            ----------
            stmt : str
                The statement to do the replacement on

            Returns
            -------
            str containing the modified statement.
        """
        if 'materialize' in stmt:
            stmt = self.process_materialize(stmt)
        if 'listagg' in stmt:
            stmt = process_aggregates(stmt)
        if 'select USER, table_name' in stmt and stmt.count('UNION') == 3:
            return "select user,table_name,preference from ingest_test"
        if '.nextval from dual' in stmt and 'connect by' in stmt:
            self.num = int(stmt[stmt.rfind('<') + 1:])
            return None
        for k, v in self.repl.items():
            stmt = stmt.replace(k, v)
        return stmt

    def execute(self, stmt, params=(), **kwargs):
        """ Execute the given query, substituting in any parameters.

            Parameters
            ----------
            stmt : str
                The sql statement to execute, use None to execute the most recently prepared statement.

            params : iterable
                The paremters to substitue in to the query.
        """
        if kwargs:
            params = kwargs
        # do any substitutions
        if params:
            if isinstance(params, (tuple, list, set)):
                newpar = []
                for par in params:
                    newpar.append(convert_PROCEDURES(par))
                params = newpar
            elif isinstance(params, dict):
                for k, val in params.items():
                    params[k] = convert_PROCEDURES(val)
        # if the statement was given do any conversions
        if stmt:
            if stmt.startswith('commit'):
                return None
            exstmt = self.replacevals(stmt)
            if exstmt is None:
                return None

            return super(MockCursor, self).execute(convert_PROCEDURES(exstmt), params)
        return super(MockCursor, self).execute(self._stmt, params)

    def executemany(self, stmt, params):
        """ Execute the given query with a range of inputs.

            Parameters
            ----------
            stmt : str
                The sql statement to execute, use None to execute the most recently prepared statement.

            params : iterable
                The paremters to substitue in to the query.
        """
        if params:
            if isinstance(params, (tuple, list, set)):
                newpar = []
                for par in params:
                    newpar.append(convert_PROCEDURES(par))
                params = newpar
            elif isinstance(params, dict):
                for k, val in params.items():
                    params[k] = convert_PROCEDURES(val)

        if stmt:
            exstmt = self.replacevals(stmt)
            if exstmt is None:
                return None
            #print exstmt
            #print params
            return super(MockCursor, self).executemany(convert_PROCEDURES(exstmt), params)
        return super(MockCursor, self).executemany(self._stmt, params)

    def var(self, _type):
        """ Mimic the var call.
        """
        return Slot()

    def callproc(self, procname, procargs):
        """ Mock the calling of Oracle procedures. The following proceedures are handled:

            - SEM_WAIT
            - SEM_DEQUEUE
            - SEM_SIGNAL
            - createObjectsTable
            - pMergeObjects

            Parameters
            ----------
            procname: str
                The name of the procedure.

            procargs: multiple
                Any arguments for the procedure call.
        """
        # mimic the SEM_WAIT procedure
        if procname == 'SEM_WAIT':
            [semname, slot] = procargs
            while True:
                self.execute("select slot from SEMLOCK where NAME='%s' and IN_USE=0" % semname)
                res = self.fetchall()
                try:
                    slot.value = res[0][0]
                    self.execute("update SEMLOCK set IN_USE=1 where slot=%i and name='%s'" % (slot.value, semname))
                    break
                except IndexError:
                    if self.fail:
                        raise Exception('SEMAPHORE allocation failure')
                    time.sleep(5)
        # mimic the SEM_DEQUEUE procedure
        elif procname == 'SEM_DEQUEUE':
            [semname, slot] = procargs
            slot.value = None
        # mimic the SEM_SIGNAL prodecure
        elif procname == 'SEM_SIGNAL':
            [semname, slot] = procargs
            self.execute("update SEMLOCK set IN_USE=0 where slot=%i and name='%s'" % (slot.value, semname))
        # mimic the createObjectsTable procedure
        elif procname == 'createObjectsTable':
            [temptable, _, table] = procargs
            self.execute("create table " + temptable + " as select * from " + table + " where 0=1")
        # mimic the pMergeObjects procedure
        elif procname == 'pMergeObjects':
            pass
        elif procname.endswith('.pMergeObjects'):
            pass
        else:
            raise Exception("Unknown proc called")

class MockConnection:
    """ Class for mocking an oracle connection, using sqlite3. This class creates a (or uses an existing)
        singleton instance of a connection to the sqlite3 database. It mimics the behvior of the DES
        Oracle database, inculding procedures and global temp tables. Any given parameters are
        passed to the connection class.

    """
    __instance = None
    home_dir = None
    __refcount = 0
    temp_tables = {'OPM_FILENAME_GTT': {'FILENAME': 'TEXT',
                                        'COMPRESSION' :'TEXT'},
                   'GTT_FILENAME': {'FILENAME': 'TEXT',
                                    'COMPRESSION' :'TEXT'},
                   'GTT_ARTIFACT': {'FILENAME': 'TEXT',
                                    'COMPRESSION': 'TEXT',
                                    'MD5SUM': 'TEXT',
                                    'FILESIZE': 'INTEGER'},
                   'GTT_ATTEMPT': {'REQNUM': 'INTEGER',
                                   'UNITNAME': 'TEXT',
                                   'ATTNUM': 'INTEGER'},
                   'GTT_EXPNUM': {'EXPNUM': 'INTEGER',
                                  'CCDNUM': 'INTEGER',
                                  'BAND': 'TEXT'},
                   'GTT_ID': {'ID': 'INTEGER'},
                   'GTT_NUM': {'NUM': 'INTEGER'},
                   'GTT_STR': {'STR': 'TEXT'},
                   _COADD_IMAGE_QUERY: {'FILENAME': 'TEXT',
                                        'FILETYPE': 'TEXT',
                                        'CROSSRA0': 'TEXT',
                                        'PFW_ATTEMPT_ID': 'INTEGER',
                                        'BAND': 'TEXT',
                                        'CCDNUM': 'INTEGER',
                                        'RA_CENT': 'REAL',
                                        'DEC_CENT': 'REAL',
                                        'RA_SIZE_CCD': 'REAL',
                                        'DEC_SIZE_CCD': 'REAL'},
                   _COADD_TILE_QUERY: {'TILENAME': 'TEXT',
                                       'RA_SIZE': 'REAL',
                                       'DEC_SIZE': 'REAL',
                                       'DEC_CENT': 'REAL',
                                       'RA_CENT': 'REAL'}
                   }

    def __init__(self, *args, **kwargs):
        self.pingval = True
        # increment the reference counter
        MockConnection.__refcount += 1
        self.__closed = False
        if MockConnection.home_dir is None:
            filename = inspect.getframeinfo(inspect.currentframe()).filename
            MockConnection.home_dir = os.path.dirname(os.path.abspath(filename))
        if MockConnection.__instance is None:
            MockConnection.__instance = _MockConnection(MockConnection.home_dir,
                                                        MockConnection.temp_tables,
                                                        *args, **kwargs)

    def teardown(self):
        """ Close and remove the database from disk.

        """
        try:
            self._close(True)
        except:
            pass
        try:
            os.unlink(os.path.join(self.home_dir, DB_FILE))
        except FileNotFoundError as _:
            pass

    @classmethod
    def destroy(cls):
        """ Static method to close the database and remove it from disk.
        """
        try:
            MockConnection.__instance.close()
            MockConnection.__instance = None
            MockConnection.__refcount = 0
        except:
            pass
        try:
            os.unlink(os.path.join(cls.home_dir, DB_FILE))
        except:
            pass

    def close(self):
        """ Mimic the closing of a database connection.
        """
        self._close()

    def _close(self, force=False):
        """ Internal method to mimic the closing of a connection as we do not want to actually close
            the database if there are other connections through the singleton.

            Parameters
            ----------
            force: bool
                Whether to force the closing of the database connection. Useful when running tests.
                Default is False, do not force it to close.
        """
        if not force:
            self.__closed = True
            self.pingval = False
        # decrement the reference counter
        MockConnection.__refcount -= 1
        # if there are no mor active connections then close up
        if MockConnection.__refcount == 0 or force:
            MockConnection.destroy()

    def ping(self):
        """ Mimic the pinging of the database.

            Returns
            -------
            bool
        """
        return self.pingval

    def __getattr__(self, name):
        """ Pass all other method calls on to the connection.
        """
        # if this connection is closed then throw an error
        if self.__closed:
            raise Exception("Cannot operate on a closed database")
        return getattr(MockConnection.__instance, name)

    @classmethod
    def mock_fail(cls, val=None):
        """ Static method to set/get the failure method, used for testing.
        """
        if val is None:
            return _MockConnection.mock_fail
        _MockConnection.mock_fail = val

class _MockConnection(sqlite3.Connection):
    """
    Provide cx_Oracle-specific implementations of canonical database methods

    Refer to desdbi.py for full method documentation.
    """
    mock_fail = False
    def __init__(self, home_dir, temp_tables, *args, **kwargs):
        """
        Initialize a mimic OracleConnection object

        Connect the  instance to the database identified in access_data.

        """
        self.home_dir = home_dir
        self.temp_tables = temp_tables
        self._results = None
        self.haveExpr = False
        self.module = _MODULE_NAME
        self.type = 'MOCKDB'
        self.configdict = {'user': 'non-user',
                           'passwd': 'non-passwd',
                           'meta_file': 'non-file',
                           'meta_section': 'non-section'}
        # register data converters
        sqlite3.register_adapter(datetime.datetime, adapt_timestamp)
        sqlite3.register_converter('TIMESTAMP', convert_timestamp)
        sqlite3.register_converter('DATE', convert_timestamp)


        needSetup = False

        # see if the database needs to be set up
        if not os.path.exists(os.path.join(self.home_dir, DB_FILE)):
            needSetup = True
        elif not os.path.isfile(os.path.join(self.home_dir, DB_FILE)):
            shutil.rmtree(os.path.join(self.home_dir, DB_FILE))
            needSetup = True
        # initialize the connection
        sqlite3.Connection.__init__(self, database=os.path.join(self.home_dir, DB_FILE),
                                    detect_types=sqlite3.PARSE_DECLTYPES,
                                    check_same_thread=False)
        cur = self.cursor()
        cur.execute("PRAGMA synchronous = OFF")
        cur.close()
        self._autocommit = False
        self.setupTempTables()
        if needSetup:
            self.setup()

    @property
    def autocommit(self):
        """ Getter for the autocommit flag
        """
        if self.isolation_level is None:
            return True
        return False

    @autocommit.setter
    def autocommit(self, val):
        """ Setter for the autocommit flag
        """
        if val:
            self.isolation_level = None
        else:
            self.isolation_level = ''

    def fakeResults(self, results=None):
        self._results = results

    def clearResults(self):
        self.fakeResults()

    def setupTempTables(self):
        """ Create any temporary tables in memory
        """
        cur = self.cursor()
        cur.execute("PRAGMA temp_store = MEMORY")
        for table, columns in self.temp_tables.items():
            cur.execute("create temporary table if not exists %s (" % table + (',').join('"' + key + '" ' + val for key, val in columns.items()) + ')')

    def commit(self):
        """ Commit any changes to disk
        """
        curs = self.cursor()
        self.clearTempTables(curs)
        super(_MockConnection, self).commit()

    def clearTempTables(self, curs):
        """ Clear any temp tables
        """
        for table in self.temp_tables:
            curs.execute('delete from %s' % table)

    def close(self):
        """ Close the connection to the database
        """
        cur = self.cursor()
        for table in self.temp_tables:
            cur.execute('drop table %s' % table)

        super(_MockConnection, self).close()

    def setup(self):
        """ Initialize the DB, creating tables and populating them with initial data, and creating triggers. This is done
            by looking for all files ending with .sql in the sqlFiles directory and executing them.
            All initial data are in the INSERTS file and all triggers are in the TRIGGERS file

        """
        #print "Creating test database..."
        files = glob.glob(os.path.join(self.home_dir, 'sqlFiles', '*.sql'))
        for fls in files:
            loc = fls.rfind('/')
            #print("   " + fls.replace('.sql', '')[loc + 1:])
            flh = open(fls, 'r')
            curs = self.cursor()
            curs.executescript(flh.read())
            self.commit()
            curs.close()
            flh.close()
        for fls in ['INSERTS', 'TRIGGERS']:
            #print(fls)
            flh = open(os.path.join(self.home_dir, 'sqlFiles', fls), 'r')
            curs = self.cursor()
            curs.executescript(flh.read())
            self.commit()
            curs.close()
            flh.close()


    def cursor(self, fetchsize=None):
        """
        Return a Cursor object for operating on the connection.

        The fetchsize argument is ignored, but retained for compatibility
        with other connection types.
        """
        return MockCursor(self, fail=self.mock_fail, results=self._results)

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
            return '{}'
        return 'SELECT {} FROM DUMMY'

    def get_named_bind_string(self, name):
        """Return a named bind (substitution) string for name with cx_Oracle."""

        return ":" + name

    def get_positional_bind_string(self, pos=1):
        """Return a positional bind (substitution) string for cx_Oracle."""

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

        return "{target:s} REGEXP {pattern:s}"

    def get_seq_next_clause(self, seqname):
        """Return an SQL expression that extracts the next value from a sequence."""
        seq = seqname.upper()
        c = self.cursor()
        c.execute("update sequences set junk=0 where name='%s'" % seq)
        self.commit()
        self.haveExpr = True
        return "select seq_val from sequences where name='%s'" % seq

    def sequence_drop(self, seq_name):
        """ Drop sequence; do not generate error if it doesn't exist."""
        c = self.cursor()
        c.execute("delete from dummy where name='%s'" % seq_name.upper())
        c.execute("delete from sequences_data where name='%s'" % seq_name.upper())
        c.close()
        self.commit()

    def table_drop(self, table):
        """ Drop table; do not generate error if it doesn't exist. """

        stmt = 'DROP TABLE %s' % table

        curs = self.cursor()
        try:
            curs.execute(stmt)
        except sqlite3.OperationalError:
            pass
        finally:
            curs.close()

    def from_dual(self):
        """ Ignore calls to this as sqlite3 does not have dual.
        """
        return ""

    def get_current_timestamp_str(self):
        """ Get a timestamp of the current time.
        """
        return str(time.mktime(datetime.datetime.now().timetuple()))
