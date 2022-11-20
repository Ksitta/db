class FileNotOpenedError(Exception):
    ''' Raised when trying to modify a closed file.
    '''


class InitMetaError(Exception):
    ''' Raised when failed to set meta info.
    '''
    

class ReadMetaError(Exception):
    ''' Raised when failed to read the meta info.
    '''
    

class PackRecordError(Exception):
    ''' Raised when failed to pack a record.
    '''
    

class UnpackRecordError(Exception):
    ''' Raised when failed to unpack a record.
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
