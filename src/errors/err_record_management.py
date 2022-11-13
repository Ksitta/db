class FileNotOpenedError(Exception):
    ''' Raised when trying to modify a closed file.
    '''


class InitMetaError(Exception):
    ''' Raised when failed to set meta info.
    '''
    

class ReadMetaError(Exception):
    ''' Raised when failed to read the meta info.
    '''
