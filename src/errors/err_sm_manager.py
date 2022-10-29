class NoUsingDatabaseError(Exception):
    ''' Raised when operate on using no database.
    '''

class NonExistDataBaseError(Exception):
    ''' Raised when open a non-existed data base.
    '''


class TableExistsError(Exception):
    ''' Raised when create an existed table.
    '''
    
class TableNotExistsError(Exception):
    ''' Raised when operatr on an non-existed table.
    '''