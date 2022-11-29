class IndexNotOpenedError(Exception):
    ''' Raised when trying to modify a closed index file.
    '''


class InitIndexMetaError(Exception):
    ''' Raised when failed to init index meta.
    '''