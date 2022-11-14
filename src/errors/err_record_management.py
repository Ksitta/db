class FileNotOpenedError(Exception):
    ''' Raised when trying to modify a closed file.
    '''


class InitMetaError(Exception):
    ''' Raised when failed to set meta info.
    '''
    

class ReadMetaError(Exception):
    ''' Raised when failed to read the meta info.
    '''


class InsertRecordError(Exception):
    ''' Raised when failed to insert a record.
    '''


class RemoveRecordError(Exception):
    ''' Raised when failed to remove a record.
    '''
    

class UpdateRecordError(Exception):
    ''' Raised when failed to update a record.
    '''
