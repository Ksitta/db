class IndexNotOpenedError(Exception):
    ''' Raised when trying to modify a closed index file.
    '''


class InitIndexMetaError(Exception):
    ''' Raised when failed to init index meta.
    '''
    

# IX_RidBucket
class BucketInsertRidError(Exception):
    ''' Raised when failed to insert a rid into a bucket.
    '''
    

class BucketRemoveRidError(Exception):
    ''' Raised when failed to remove a rid from a bucket.
    '''
    

# tree node
class IndexFieldTypeError(Exception):
    ''' Raised when triggering index field related error.
    '''
    

class NodeInsertEntryError(Exception):
    ''' Raised when failed to insert a node entry.
    '''
    

# index handle
class IndexInitMetaError(Exception):
    ''' Raised when failed to init index meta.
    '''
    

class IndexSearchError(Exception):
    ''' Raised when failed to search a field value in the index.
    '''


# index scan
class IndexOpenScanError(Exception):
    ''' Raised when failed to open an index scan.
    '''
    
class IndexScanNextError(Exception):
    ''' Raised when failed to scan the next rid in index.
    '''
    
