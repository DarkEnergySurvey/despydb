# $Id: errors.py 33097 2015-01-20 15:14:29Z felipe $
# $Rev:: 33097                            $:  # Revision of last commit.
# $LastChangedBy:: felipe                 $:  # Author of last commit.
# $LastChangedDate:: 2015-01-20 09:14:29 #$:  # Date of last commit.

"""
    Define exceptions raised by coreutils.

    Classes:
        MissingDBId        - No database identification information found in
                             des services section.  Subclass of Exception.
        UnknownDBTypeError - An unknown database type found in des services
                             section.  Subclass of NotImplementedError.
        UnknownCaseSensitiveError
                             An unknown case sensitivity option was speciied.
                             Subclass of NotImplementedError.

    Developed at:
    The National Center for Supercomputing Applications(NCSA).

    Copyright(C) 2011 Board of Trustees of the University of Illinois.
    All rights reserved.

"""

class MissingDBId(Exception):
    """ Exception for when the service access configuration has missing
        database identification entries.

        Parameters
        ----------
        msg : str
            The error message to attach to the exception.
    """

    def __init__(self, msg=None):
        if not msg:
            msg = 'No database identifier found in service access config.'

        Exception.__init__(self, msg)

class UnknownDBTypeError(NotImplementedError):
    """ Exception for when the service access configuration identifies an
        unknown database type.

        Parameters
        ----------
        db_type : str
            The unknown database type

        msg : str
            The error message to attach to the exception.

    """

    def __init__(self, db_type, msg=None):
        self.db_type = db_type
        if not msg:
            msg = f'database type: "{self.db_type}"'

        NotImplementedError.__init__(self, msg)

class UnknownCaseSensitiveError(NotImplementedError):
    """ Exception for an invalid case sensitivity flag.

        Parameters
        ----------
        value : str
            The invalid flag value.

        msg : str
            The error message to attach to the exception.

    """

    def __init__(self, value, msg=None):
        self.value = value
        if not msg:
            msg = f'Unknown case sensitivity value: "{value}"'

        NotImplementedError.__init__(self, msg)
