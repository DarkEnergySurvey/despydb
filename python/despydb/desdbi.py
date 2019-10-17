# $Id: desdbi.py 48541 2019-05-20 19:06:49Z friedel $
# $Rev:: 48541                            $:  # Revision of last commit.
# $LastChangedBy:: friedel                $:  # Author of last commit.
# $LastChangedDate:: 2019-05-20 14:06:49 #$:  # Date of last commit.

"""
    Provide a dialect-neutral interface to DES databases.

    Classes:
        DesDbi - Connects to a Postgresql or Oracle instance of a DES database
                 upon instantiation and the resulting object provides an
                 interface based on the Python DB API with extensions to allow
                 interaction with the database in a dialect-neutral manner.

    Developed at:
    The National Center for Supercomputing Applications(NCSA).

    Copyright(C) 2011 Board of Trustees of the University of Illinois.
    All rights reserved.

"""

__version__ = "2.0.0"

import sys
import copy
import time
import socket
from collections import OrderedDict
from despyserviceaccess import serviceaccess

# importing of DB specific modules done down inside code

import despydb.errors as errors
import despydb.desdbi_defs as defs

class DesDbi(object):
    """ Provide a dialect-neutral interface to a DES database.

        During Instantiation of this class, service access parameters are found and
        a connection opened to the database identified.  The resulting object
        exposes several methods from an implementation of a python DB API
        Connection class determined by the service access parameters.  In addition,
        the object provides several methods that allow callers to construct SQL
        statements and to interact with the database without dialect-specific code.

        This class may be used as a context manager whereupon it will automatically
        close the database connection after either commiting the transaction if the
        context is exited without an exception or rolling back the transaction
        otherwise.

        As an example of context manager usage, the following code will open a
        database connection, insert two rows into my_table, print the contents of
        my_table after the insert, commit the insert, and close the connection
        unless some sort of error happens:

            with coreutils.DesDbi() as dbh:
                dbh.insert_many('my_table', ['col1', 'col2'], [(1,1),(2,2)])
                print dbh.query_simple('my_table')

        If the insert fails, the transaction will be rolled back and the connection
        closed without an attempt query the table.

        The DES services file and/or section contained therein may be specified
        via the desfile and section arguments.  When omitted default values
        will be used as defined in DESDM-3.  A tag of "db" will be used in all
        cases.

        Parameters
        ----------
        desfile : str, optional
            The file to use for the services data, default is None.

        section : str, optional
            The section of the services file to use. Default is None

        retry : bool, optional
            Whether to retry when reading the services file and when connecting
            to the database, if errors are encountered. Default is False.

        connection : database handle, optional
            A database connection to use rather than creating a new one. Default
            is None.

        threaded : bool, optional
            Whether to use a thread safe connection or not. Default is False, no
            thread safety.
    """

    def __init__(self, desfile=None, section=None, retry=False, connection=None, threaded=False):
        #pylint: disable=import-error
        self.retry = retry
        if connection is None:
            self.inherit = False
            self.configdict = serviceaccess.parse(desfile, section, 'DB', retry)

            self.type = self.configdict['type']

            serviceaccess.check(self.configdict, 'DB')

            if self.type == 'oracle':
                self.configdict['threaded'] = threaded
                import despydb.oracon
                self.conClass = despydb.oracon.OracleConnection
            #elif self.type == 'postgres':
            #    import despydb.pgcon
            #    self.conClass = despydb.pgcon.PostgresConnection
            elif self.type == 'test':
                sys.path.append('../../tests')
                import MockDBI
                self.conClass = MockDBI.MockConnection
            else:
                raise errors.UnknownDBTypeError(self.type)

            self.connect()
        else:
            self.inherit = True
            self.configdict = connection.configdict
            self.type = connection.type
            self.con = connection.con

    def __enter__(self):
        """ Enable the use of this class as a context manager.
        """

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ Shutdown the connection to the database when context ends.

            Commit any pending transaction if no exception is raised; otherwise,
            rollback that transaction.  In either case, close the database
            connection.
        """
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        # don't close the connection if this is not the originator
        if self.inherit:
            return False
        self.close()

        return False

    def connect(self):
        """ Connect to the database, retrying if requested.
        """
        MAXTRIES = 1
        if self.retry:
            MAXTRIES = 5
        TRY_DELAY = 10 # seconds
        trycnt = 0
        done = False
        lasterr = ""
        while not done and trycnt < MAXTRIES:
            trycnt += 1
            try:
                self.con = self.conClass(self.configdict)
                done = True
            except Exception as e:
                lasterr = str(e).strip()
                print lasterr
                timestamp = time.strftime("%x %X", time.localtime())
                print "%s: Could not connect to database, try %i/%i" %(timestamp, trycnt, MAXTRIES)
                if trycnt < MAXTRIES:
                    print "\tRetrying...\n"
                    time.sleep(TRY_DELAY)
                else:
                    print "  Error, could not connect to the database after %i retries: %s" %(MAXTRIES, lasterr)

        if not done:
            print "Exechost:", socket.gethostname()
            print "Connection information:", str(self)
            print ""
            raise Exception("Aborting attempt to connect to database.  Last error message: %s" % lasterr)
        elif trycnt > 1: # only print success message if we've printed failure message
            print "Successfully connected to database after retrying."

    def reconnect(self):
        """ Reconnect to the database (but ony if the connection is no longer live).
        """
        if not self.ping():
            self.connect()
        else:
            print 'Connection still good, not reconnecting'

    def autocommit(self, state=None):
        """ Return and optionally set autocommit mode.

            Parameters
            ----------
            state : various, optional
                If state is a Boolean, then set connection's autocommit mode
                accordingly. Default is None.

            Returns
            -------
            bool
                The autocommit mode prior to any change.
        """
        a = self.con.autocommit

        if isinstance(state, bool):
            self.con.autocommit = state

        return a

    def close(self):
        """ Close the current connection, disabling any open cursors.
        """
        return self.con.close()

    def commit(self):
        """ Commit any pending transaction.
        """
        return self.con.commit()

    def cursor(self, fetchsize=None):
        """ Return a Cursor object for operating on the connection.

            The not yet implemented fetchsize argument would cause PostgreConnection
            to generate a psycopg2 named cursor configured to fetch the indicated
            number of rows from the database per request needed to fulfill the
            requirements of calls to fetchall(), fetchone(), and fetchmany().  It's
            default behavior is to fetch all rows from the database at once.
        """
        return self.con.cursor(fetchsize)

    def exec_sql_expression(self, expression):
        """ Execute an SQL expression or expressions.

            Parameters
            ----------
            expression : various
                Construct and execute an SQL statement from a string containing an SQL
                expression or a list of such strings.

            Returns
            -------
            tuple
                A tuple containing a single result for each column.
        """
        if hasattr(expression, '__iter__'):
            s = ','.join(expression)
        else:
            s = expression

        stmt = self.get_expr_exec_format() % s
        cursor = self.cursor()
        cursor.execute(stmt)
        res = cursor.fetchone()
        cursor.close()
        return res


    def get_expr_exec_format(self):
        """ Return a format string for a statement to execute SQL expressions.

            The returned format string contains a single unnamed python subsitution
            string that expects a string containing the expressions to be executed.
            Once the expressions have been substituted into the string, the
            resulting SQL statement may be executed.

            Returns
            -------
            str

            Examples:
                expression:      con.get_expr_exec_format()
                oracle result:   SELECT %s FROM DUAL
                postgres result: SELECT %s

                expression:      con.get_expr_exec_format() % 'func1(), func2()'
                oracle result:   SELECT func1(), func2() FROM DUAL
                postgres result: SELECT func1(), func2()
        """
        return self.con.get_expr_exec_format()

    def get_column_metadata(self, table_name):
        """ Return a dictionary of 7-item sequences, with lower case column name keys.
            The sequence values are:
            (name, type, display_size, internal_size, precision, scale, null_ok)
            Constants are defined for the sequence indexes in coreutils_defs.py

            Parameters
            ----------
            table_name : str
                The table whose information needs to be looked up.

            Returns
            -------
            dict
        """
        cursor = self.cursor()
        sqlstr = 'SELECT * FROM %s WHERE 0=1' % table_name
        if self.type == 'oracle':
            cursor.parse(sqlstr)
        #elif self.type == 'postgres':
        #    cursor.execute(sqlstr)
        elif self.type == 'test':
            cursor.execute(sqlstr)
        else:
            raise errors.UnknownDBTypeError(self.type)
        retval = {}
        for col in cursor.description:
            retval[col[defs.COL_NAME].lower()] = col
        cursor.close()
        return retval

    def get_column_lengths(self, table_name):
        """ Return a dictionary of column_name = column_length for the given table

            Parameters
            ----------
            table_name : str
                The table whose information needs to be looked up.

            Returns
            -------
            dict
        """
        meta = self.get_column_metadata(table_name)
        res = {}
        for col in meta.values():
            res[col[defs.COL_NAME].lower()] = col[defs.COL_LENGTH]
        return res


    def get_column_names(self, table_name):
        """ Return a sequence containing the column names of specified table.

            Parameters
            ----------
            table_name : str
                The table whose information needs to be looked up.

            Returns
            -------
            dict
        """
        meta = self.get_column_metadata(table_name)
        column_names = [d[0].lower() for d in meta.values()]
        return column_names

    def get_column_types(self, table_name):
        """ Return a dictionary of python types indexed by column name for a table.

            Parameters
            ----------
            table_name : str
                The table whose information needs to be looked up.

            Returns
            -------
            dict
        """
        return self.con.get_column_types(table_name)


    def get_named_bind_string(self, name):
        """ Return a named bind(substitution) string.

            Returns a dialect-specific bind string for use with SQL statement
            arguments specified by name.

            Parameters
            ----------
            name : str
                The name of the parameter to bind.

            Returns
            -------
            str

            Examples:
                expression:      get_named_bind_string('abc')
                oracle result:   :abc
                postgres result: %(abc)s
        """
        return self.con.get_named_bind_string(name)

    def get_positional_bind_string(self, pos=1):
        """ Return a positional bind(substitution) string.

            Returns a dialect-specific bind string for use with SQL statement
            arguments specified by position.

            Parameters
            ----------
            pos : int, optional
                The number of the position to bind.

            Returns
            -------
            str

            Examples:
                expression:      get_positional_bind_string()
                oracle result:   :1
                postgres result: %s
        """
        return self.con.get_positional_bind_string(pos)

    def get_regex_clause(self, target, pattern, case_sensitive=True):
        """ Return a dialect-specific regular expression matching clause.

            Construct a dialect-specific SQL Boolean expression that matches a
            provided target with a provided regular expression string.  The target
            is assumed to be a column name or bind expression so it is not quoted
            while the pattern is assumed to be a string, so it is quoted.

            Case sensitivity of matching can be controlled.  When case_sensitive is
            None, the Oracle implementation will defer to the database default
            settings.

            For a more flexible interface, refer to get_regex_format().

            Parameters
            ----------
            target : str
                the target of the binding

            pattern : str
                The regex pattern to use.

            case_sensitive : bool, optional
                Whether the expression should be case sensitive. Default is True,
                expression is case sensitive.

            Returns
            -------
            str
                The constructed regex

            Examples:
                expression:      get_regex_clause("col1", "pre.*suf")
                oracle result:   REGEXP_LIKE(col1, 'pre.*suf')
                postgres result:(col1 ~ 'pre.*suf')

                expression:      get_regex_clause(get_positional_bind_string(),
                                                   "prefix.*")
                oracle result:   REGEXP_LIKE(:1, 'prefix.*')
                postgres result:(%s ~ 'prefix.*')
        """
        d = {'target' : target,
             'pattern': "'" + pattern + "'"}

        return self.get_regex_format(case_sensitive) % d

    def get_regex_format(self, case_sensitive=True):
        """ Return a format string for constructing a regular expression clause.

            The returned format string contains two python named-substitution
            strings:
                target  -- expects string indicating value to compare to
                pattern -- expects string indicating the regular expression
            The value for both should be exactly what is desired in the SQL
            expression which means, for example, that if the regular expression is
            a explicit string rather than a bind string, it should contain the
            single quotes required for strings in SQL.

            When working with constant regular expressions, the get_regex_clause()
            is easier to use.

            Parameters
            ----------
            case_sensitive : bool, optional
                Whether the expression should be case sensitive. Default is True,
                expression is case sensitive.

            Returns
            -------
            str
                The constructed regex

            Examples:
                expression:      get_regex_format()
                oracle result:   REGEXP_LIKE(%(target)s, %(pattern)s)
                postgres result: %(target)s ~ %(pattern)s

                expression:      get_regex_format() % {"target": "col1",
                                                        "pattern": "'pre.*suf'"}
                oracle result:   REGEXP_LIKE(col1, 'pre.*suf', 'c')
                postgres result:(col1 ~ 'pre.*suf')

                expression:      get_regex_format() % {
                                    "target":  get_positional_bind_string(),
                                    "pattern": get_positional_bind_string()}
                oracle result:   REGEXP_LIKE(:1, :1, 'c')
                postgres result:(%s ~ %s)
        """
        return self.con.get_regex_format(case_sensitive)

    def get_seq_next_clause(self, seqname):
        """ Return an SQL expression that extracts the next value from a sequence.

            Construct and return a dialect-specific SQL expression that, when
            evaluated, will extract the next value from the specified sequence.

            Parameters
            ----------
            seqname : str
                The sequence to get the next value for

            Returns
            -------
            str

            Examples:
                expression:      get_seq_next_clause('seq1')
                oracle result:   seq1.NEXTVAL
                postgres result: nextval('seq1')
        """
        return self.con.get_seq_next_clause(seqname)

    def get_seq_next_value(self, seqname):
        """ Return the next value from the specified sequence.

            Execute a dialect-specific expression to extract the next value from
            the specified sequence and return that value.

            Parameters
            ----------
            seqname : str
                The sequence to get the next value for

            Returns
            -------
            int
                The next value of the sequence

            Examples:
                expression:           get_seq_next_value('seq1')
                oracle result from:   SELECT seq1.NEXTVAL FROM DUAL
                postgres result from: SELECT nextval('seq1')
        """
        expr = self.get_seq_next_clause(seqname)
        return self.exec_sql_expression(expr)[0]

    def insert_many(self, table, columns, rows):
        """ Insert a sequence of rows into the indicated database table.

            If each row in rows is a sequence, the values in each row must be in
            the same order as all other rows and columns must be a sequence
            identifying that order.  If each row is a dictionary or other mapping,
            columns can be any iterable that returns the column names and the set
            of keys for each row must match the set of column names.

            Parameters
            ----------
            table : str
                Name of the table into which data should be inserted.

            columns : list
                Names of the columns to be inserted.

            rows : array like
                A sequence of rows to insert.
        """

        if not rows:
            return
        if hasattr(rows[0], 'keys'):
            vals = ','.join([self.get_named_bind_string(c) for c in columns])
        else:
            bindStr = self.get_positional_bind_string()
            vals = ','.join([bindStr for c in columns])

        colStr = ','.join(columns)

        stmt = 'INSERT INTO %s(%s) VALUES(%s)' %(table, colStr, vals)

        curs = self.cursor()
        try:
            curs.executemany(stmt, rows)
        finally:
            curs.close()

    def insert_many_indiv(self, table, columns, rows):
        """ Insert a sequence of rows into the indicated database table.

            If each row in rows is a sequence, the values in each row must be in
            the same order as all other rows and columns must be a sequence
            identifying that order.  If each row is a dictionary or other mapping,
            columns can be any iterable that returns the column names and the set
            of keys for each row must match the set of column names.

            Parameters
            ----------
            table : str
                Name of the table into which data should be inserted.
            columns : list
                Names of the columns to be inserted.
            rows : array like
                A sequence of rows to insert.

        """

        if not rows:
            return
        if hasattr(rows[0], 'keys'):
            vals = ','.join([self.get_named_bind_string(c) for c in columns])
        else:
            bindStr = self.get_positional_bind_string()
            vals = ','.join([bindStr for c in columns])

        colStr = ','.join(columns)

        stmt = 'INSERT INTO %s(%s) VALUES(%s)' %(table, colStr, vals)

        curs = self.cursor()
        curs.prepare(stmt)
        for row in rows:
            try:
                curs.execute(None, row)
            except Exception as err:
                print "\n\nError: ", err
                print "sql>", stmt
                print "params:", row
                print "\n\n"
                raise

        curs.close()

    def query_simple(self, from_, cols='*', where=None, orderby=None,
                     params=None, rowtype=dict):
        """ Issue a simple query and return results.

            If positional bind strings are used in column expressions and/or the
            where clause, params should be a sequence of values in the same order.
            If named bind strings are used, params should be a dictionary indexed
            by bind string names.

            The resulting rows are returned as a list of the specified rowtype.

            Parameters
            ----------
            from_ : str
                a string containing the name of a table or view or some
                other from expression

            cols : various, optional
                The columns to retrieve; can be a sequence of column
                names or expressions or a string containing them; for
                rowtype = dict, unique aliases should be assigned to
                expressions so that the keys are reasonable. Default is '*'

            where : various, optional
                WHERE expression; can be a string containing
                the where clause minus "WHERE " or a sequence of
                expressions to be joined by AND. Default is None.

            orderby : various, optional
                ORDER BY expression; can be a string
                containing an ORDER BY expression or a sequence of such
                expressions. Default is None.

            params : array like, optional
                Bind parameters for the query. Default is None.

            rowtype : type
                The type of row to return; dict results in a dictionary
                indexed by the lowercase version of the column names
                provided by the query results; other types will be
                passed the retrieved row sequence to be coerced to the
                desired type. Default is dict.

            Returns
            -------
            various
                The results of the query.

            Example:
                Code:
                    dbh = coreutils.DesDbi()
                    cols  = ["col1", "col2"]
                    where = ["col1 > 5", "col2 < 'DEF'", "col3 = :1"]
                    ord   = cols
                    parms =("col3_value", )
                    rows = dbh.query_simple('tab1', cols, where, ord, parms)
                Possible Output:
                    [{"col1": 23, "col2": "ABC"}, {"col1": 45, "col2": "AAA"}]

        """

        if not from_:
            raise TypeError('A table name or other from expression is '
                            'required.')

        if hasattr(cols, '__iter__') and cols:
            colstr = ','.join(cols)
        elif cols:
            colstr = cols
        else:
            raise TypeError('A non-empty sequence of column names or '
                            'expressions or a string of such is required.')

        if hasattr(where, '__iter__') and where:
            where_str = ' WHERE ' + ' AND '.join(where)
        elif where:
            where_str = ' WHERE ' + where
        else:
            where_str = ''

        if hasattr(orderby, '__iter__') and orderby:
            ord_str = ' ORDER BY ' + ','.join(orderby)
        elif orderby:
            ord_str = ' ORDER BY ' + orderby
        else:
            ord_str = ''

        stmt = "SELECT %s FROM %s%s%s" %(colstr, from_, where_str, ord_str)

        curs = self.cursor()
        try:
            if params:
                curs.execute(stmt, params)
            else:
                curs.execute(stmt)

            rows = curs.fetchall()
            rcols = [desc[0].lower() for desc in curs.description]

        finally:
            curs.close()

        if rowtype == dict:
            res = [{col:val for col, val in zip(rcols, row)} for row in rows]
        elif rows and isinstance(rows[0], rowtype):
            res = rows
        else:
            res = [rowtype(row) for row in rows]

        return res

    def is_postgres(self):
        """ Returns whether or not the current connection is a PostgreSql

            Returns
            -------
            bool
        """
        return self.type == 'postgres'

    def is_oracle(self):
        """ Returns whether of not the current connection is Oracle

            Returns
            -------
            bool
        """
        return self.type == 'oracle'

    def rollback(self):
        """ Rollback the current transaction.
        """

        return self.con.rollback()

    def sequence_drop(self, seq_name):
        """ Drop sequence; do not generate error if it doesn't exist.

            Parameters
            ----------
            seq_name : str
                The name of the sequence to drop.
        """
        self.con.sequence_drop(seq_name)

    def __str__(self):
        copydict = copy.deepcopy(self.configdict)
        del copydict['passwd']
        return '%s' %(copydict)

    def table_drop(self, table):
        """ Drop table; do not generate error if it doesn't exist.

            Parameters
            ----------
            table : str
                The name of the table to drop.
        """
        self.con.table_drop(table)

    def from_dual(self):
        """ Get the appropriate dual expression.

            Returns
            -------
            str
        """
        return self.con.from_dual()

    def which_services_file(self):
        """ Returns which services file is being used.

            Returns
            -------
            str
                The name of the file
        """
        return self.configdict['meta_file']

    def which_services_section(self):
        """ Returns the section of the services file is being used.

            Returns
            -------
            str
                The name of the section
        """
        return self.configdict['meta_section']

    def quote(self, value):
        """ Replace single quotes with doubled ones so that they show up properly in the database.

            Parameters
            ----------
            value : str
                The string to operate on

            Returns
            -------
            str
                The updated string
        """
        return "'%s'" % str(value).replace("'", "''")

    def get_current_timestamp_str(self):
        """ Return a string for current timestamp

            Returns
            -------
            str
        """
        return self.con.get_current_timestamp_str()

    def query_results_dict(self, sql, tkey):
        """ Send a query to the database and convert the results of a query into a list of
            dictionaries, one for each line.
            The dictionary keys are the column names and the values are the associated values.

            Parameters
            ----------
            sql : str
                The query to perform

            tkey : list
                List of the expected columns

            Returns
            -------
            list
        """
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            result[d[tkey.lower()].lower()] = d

        curs.close()
        return result



    def basic_insert_row(self, table, row):
        """ Insert a row into the table

            Parameters
            ----------
            table : str
                The name of the table

            row : dict
                Dictionary of the columns and the data to insert.
        """
        ctstr = self.get_current_timestamp_str()

        cols = row.keys()
        namedbind = []
        params = {}
        for col in cols:
            if row[col] == ctstr:
                namedbind.append(row[col])
            else:
                namedbind.append(self.get_named_bind_string(col))
                params[col] = row[col]

        sql = "insert into %s(%s) values(%s)" %(table, ','.join(cols), ','.join(namedbind))


        curs = self.cursor()
        try:
            curs.execute(sql, params)
        except:
            (_type, value, _) = sys.exc_info()
            print "******************************"
            print "Error:", _type, value
            print "sql> %s\n" %(sql)
            print "params> %s\n" %(params)
            raise

    def basic_update_row(self, table, updatevals, wherevals):
        """ Update a row in a table

            Parameters
            ----------
            table : str
                The name of the table to update

            updatevals : dict
                Dictionary of the column names and values to update

            wherevals : dict
                Dictionary of column names and values to use in the where clause
        """

        ctstr = self.get_current_timestamp_str()

        params = {}
        whclause = []
        for c, v in wherevals.items():
            if v == ctstr:
                whclause.append("%s=%s" %(c, v))
            elif v is None:
                whclause.append("%s is NULL" %(c))
            else:
                whclause.append("%s=%s" %(c, self.get_named_bind_string('w_'+c)))
                params['w_'+c] = v

        upclause = []
        for c, v in updatevals.items():
            if v == ctstr:
                upclause.append("%s=%s" %(c, v))
            else:
                if isinstance(v, str) and 'TO_DATE' in v.upper():
                    upclause.append('%s=%s' % (c, v))
                else:
                    upclause.append("%s=%s" % (c, self.get_named_bind_string('u_'+c)))
                    params['u_'+c] = v


        sql = "update %s set %s where %s" %(table, ','.join(upclause), ' and '.join(whclause))

        curs = self.cursor()
        try:
            curs.execute(sql, params)
        except:
            (_type, value, _) = sys.exc_info()
            print "******************************"
            print "Error:", _type, value
            print "sql> %s\n" %(sql)
            print "params> %s\n" % params
            raise

        if curs.rowcount == 0:
            print "******************************"
            print "sql> %s\n" % sql
            print "params> %s\n" % params
            raise Exception("Error: 0 rows updated in table %s" % table)

        curs.close()

    def ping(self):
        """ Determine if the database connection is still live.

            Returns
            -------
            bool
                Whether the connection is still live.
        """
        return self.con.ping()
