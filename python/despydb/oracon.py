# $Id: oracon.py 48541 2019-05-20 19:06:49Z friedel $
# $Rev:: 48541                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:06:49 #$:  # Date of last commit.

"""
    Define cx_Oracle-specific database access methods

    Classes:
        OracleConnection - Connects to an Oracle instance of a DES database
                           upon instantiation and the resulting object provides
                           an interface based on the cx_Oracle Connection class
                           with extensions to allow callers to interact with
                           the database in a dialect-neutral manner.

    Developed at:
    The National Center for Supercomputing Applications(NCSA).

    Copyright(C) 2012 Board of Trustees of the University of Illinois.
    All rights reserved.

"""
#pylint: disable=c-extension-no-member
__version__ = "$Rev: 48541 $"

import datetime
import socket

import cx_Oracle

import despydb.errors as errors

import despymisc.miscutils as miscutils

# Construct a name for the v$session module column to allow database auditing.

import __main__
try:
    _MODULE_NAME = __main__.__file__
except AttributeError:
    _MODULE_NAME = "unavailable"

if _MODULE_NAME.rfind('/') > -1:
    _MODULE_NAME = _MODULE_NAME[_MODULE_NAME.rfind('/') + 1:]
_MODULE_NAME += '@' + socket.getfqdn()
_MODULE_NAME = _MODULE_NAME[:48]

# No need to rebuild this mapping every time it is used, so make it a global
# module object.
_TYPE_MAP = {cx_Oracle.BINARY       : bytearray,
             cx_Oracle.BFILE        : cx_Oracle.BFILE,
             cx_Oracle.BLOB         : bytearray,
             cx_Oracle.CLOB         : unicode,
             cx_Oracle.CURSOR       : cx_Oracle.CURSOR,
             cx_Oracle.DATETIME     : datetime.datetime,
             cx_Oracle.FIXED_CHAR   : str,
             cx_Oracle.FIXED_NCHAR  : unicode,
             #cx_Oracle.FIXED_UNICODE: unicode,
             cx_Oracle.INTERVAL     : datetime.timedelta,
             cx_Oracle.LOB          : bytearray,
             cx_Oracle.LONG_BINARY  : bytearray,
             cx_Oracle.LONG_STRING  : str,
             cx_Oracle.NATIVE_FLOAT : float,
             cx_Oracle.NCLOB        : unicode,
             cx_Oracle.NUMBER       : float,
             cx_Oracle.OBJECT       : cx_Oracle.OBJECT,
             cx_Oracle.ROWID        : bytearray,
             cx_Oracle.STRING       : str,
             cx_Oracle.TIMESTAMP    : datetime.datetime,
             #cx_Oracle.UNICODE      : unicode
             cx_Oracle.NCHAR        : unicode
            }

# Define some symbolic names for oracle error codes to make it clearer what
# the error codes mean.

_ORA_NO_TABLE_VIEW = 942    # table or view does not exist
_ORA_NO_SEQUENCE = 2289   # sequence does not exist

class OracleConnection(cx_Oracle.Connection):
    """ Provide cx_Oracle-specific implementations of canonical database methods.
        Connect the OracleConnection instance to the database identified in
        access_data.

        Parameters
        ----------
        access_data : dict
            Dictionary of the parameters used to create the connection. Parameters
            are:

            * user - the user name
            * passwd - the user's password
            * server - the URL or IP of the Oracle server to use
            * port - the port to use
            * sid or name - the identity of the database to use
            * service - (optional) a special service to use
            * threaded - (optional) whether to create a thread safe connection

        runningTest : bool, optional
            Only used when running tests ans actual connections are not made

    """

    def __init__(self, access_data):
        cx_args = {}
        user = access_data['user']
        pswd = access_data['passwd']

        kwargs = {'host': access_data['server'], 'port': access_data['port']}

        # Take SID first as specified by DESDM-3.
        if access_data.get('sid', None):
            kwargs['sid'] = access_data['sid']
        elif access_data.get('name', None):
            kwargs['service_name'] = access_data['name']
        else:
            raise errors.MissingDBId()
        if access_data.get('service', None):
            kwargs['service'] = access_data['service']
        if 'sid' in kwargs:
            cdt = "(SID=%s)" % kwargs['sid']
        else:
            cdt = "(SERVICE_NAME=%s)" % kwargs['service_name']
        if 'service' in kwargs:
            cdt += '(SERVER=%s)' % kwargs['service']
            cx_args['cclass'] = 'DESDM'
            cx_args['purity'] = cx_Oracle.ATTR_PURITY_SELF
        dsn = ("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s))"
               "(CONNECT_DATA=%s))") %(kwargs['host'], kwargs['port'], cdt)
        if access_data.get('threaded', None):
            cx_args['threaded'] = True

        cx_args['dsn'] = dsn

        #miscutils.fwdebug(3, "CXORACLE_DEBUG", "dsn = %s" % dsn)
        miscutils.fwdebug(3, "CXORACLE_DEBUG", str(cx_args))
        cx_Oracle.Connection.__init__(self, user=user, password=pswd, **cx_args)

        self.module = _MODULE_NAME

    def cursor(self, fetchsize=None):
        """ Return a cx_Oracle Cursor object for operating on the connection.

            The fetchsize argument is ignored, but retained for compatibility
            with other connection types.

            Returns
            -------
            cx_Oracle.cursor
                Cursor for use with the database.
        """
        #pylint: disable=unused-argument
        # cx_Oracle doesn't implement/need named cursors, so ignore fetchsize.
        return cx_Oracle.Connection.cursor(self)

    def get_column_types(self, table_name):
        """ Return a dictionary of python types indexed by column name for a table.

            Parameters
            ----------
            table_name : str
                The name of the table for which the column data are retrieved.

            Returns
            -------
            dict
                Dictionary of column names and their data type as the key/value
                pairs.
        """

        curs = self.cursor()
        curs.execute('SELECT * FROM %s WHERE 0=1' % table_name)

        types = {d[0].lower(): _TYPE_MAP[d[1]] for d in curs.description}

        curs.close()

        return types

    def get_expr_exec_format(self):
        """ Return a format string for a statement to execute SQL expressions.

            Returns
            -------
            str
                The format string.
        """

        return 'SELECT %s FROM DUAL'

    def get_named_bind_string(self, name):
        """ Return a named bind(substitution) string for name with cx_Oracle.

            Parameters
            ----------
            name : str
                The name to use as the binding.

            Returns
            -------
            str
                The properly formatted binding string.
        """

        return ":" + name

    def get_positional_bind_string(self, pos=1):
        """ Return a positional bind(substitution) string for cx_Oracle.

            Parameters
            ----------
            pos : int, optional
                The position number to use in the binding, default is 1

            Returns
            -------
            str
                The properly formatted binding string.
        """

        return ":%d" % pos

    def get_regex_format(self, case_sensitive=True):
        """ Return a format string for constructing a regular expression clause.

            Parameters
            ----------
            case_sensitive : bool, optional
                Whenther or not the regex is to be case sensitive (True), or not (False).
                Default is True, None can be used to let the DB decide what to use.

            Returns
            -------
            str
                Format string for a regular expression

            Raises
            ------
            errors.UnknownCaseSensitiveError
                If an unknown value for case_sensitive is given.
        """

        if case_sensitive is True:
            param = ", 'c'"
        elif case_sensitive is False:
            param = ", 'i'"
        elif case_sensitive is None:
            param = '' # Leave it up to the database to decide
        else:
            raise errors.UnknownCaseSensitiveError(value=case_sensitive)

        return "REGEXP_LIKE(%%(target)s, %%(pattern)s%s)" % param

    def get_seq_next_clause(self, seqname):
        """ Return an SQL expression that extracts the next value from a sequence.

            Parameters
            ----------
            seqname : str
                The name of the sequence to get the next value for.

            Returns
            -------
            str
                Expression to obtain the next sequence value.
        """

        return seqname + '.NEXTVAL'

    def sequence_drop(self, seq_name):
        """ Drop sequence; do not generate error if it doesn't exist.

            Parameters
            ----------
            seq_name : str
                The name of the sequence to drop

        """

        stmt = 'DROP SEQUENCE %s' % seq_name

        curs = self.cursor()
        try:
            curs.execute(stmt)
        except cx_Oracle.DatabaseError as exc:
            if exc.args[0].code != _ORA_NO_SEQUENCE:
                raise
        finally:
            curs.close()

    def table_drop(self, table):
        """ Drop a table; do not generate error if it doesn't exist.

            Parameters
            ----------
            table : str
                The name of the table to drop.
        """

        stmt = 'DROP TABLE %s' % table

        curs = self.cursor()
        try:
            curs.execute(stmt)
        except cx_Oracle.DatabaseError as exc:
            if exc.args[0].code != _ORA_NO_TABLE_VIEW:
                raise
        finally:
            curs.close()

    def from_dual(self):
        """ Return the proper format for selecting something from DUAL

            Returns
            -------
            str
                The proper DUAL format.
        """
        return "from dual"

    def get_current_timestamp_str(self):
        """ Returns the Oracle specific method name for getting the current time stamp.

            Returns
            -------
            str
                The method name.
        """
        return "SYSTIMESTAMP"


    def ping(self):
        """ Ping the database to make sure the connection is still alive

            Returns
            -------
            bool
                True if the connection is still alive, False otherwise.
        """
        try:
            curs = self.cursor()
            curs.execute('select 1 from dual')
            return True
        except Exception:
            return False
