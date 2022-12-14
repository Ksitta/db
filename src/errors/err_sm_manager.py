class NoUsingDatabaseError(Exception):
    ''' Raised when operate on using no database.
    '''


class DataBaseNotExistError(Exception):
    ''' Raised when open a non-existed data base.
    '''


class DataBaseExistError(Exception):
    ''' Raised when create an existed data base.
    '''


class TableExistsError(Exception):
    ''' Raised when create an existed table.
    '''


class TableNotExistsError(Exception):
    ''' Raised when operatr on an non-existed table.
    '''

class IndexAlreadyExistsError(Exception):
    ''' Raised when operatr on an non-existed table.
    '''

class ReferenceCountNotZeroError(Exception):
    ''' Raised when delete a table with reference count not zero.
    '''


class ForeignKeyNotExistsError(Exception):
    ''' Raised when insert a record with foreign key not exists.
    '''


class DuplicateColumnError(Exception):
    ''' Raised when create a table with duplicate columns.
    '''


class DuplicatePrimaryKeyError(Exception):
    ''' Raised when create a table with duplicate primary keys.
    '''


class DuplicateForeignKeyError(Exception):
    ''' Raised when create a table with duplicate foreign keys.
    '''


class ForeignKeyTypeError(Exception):
    ''' Raised when create a table with foreign keys with different types.
    '''


class NoSuchColumnError(Exception):
    ''' Raised when use a column not exists.
    '''


class AmbiguousColumnError(Exception):
    ''' Raised when use a column name that is not unique.
    '''
