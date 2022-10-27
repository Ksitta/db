class ReadDiskError(Exception):
    ''' Raised when failed to read a page from disk.
    '''
    
    
class WriteDiskError(Exception):
    ''' Raised when failed to write a page to disk.
    '''


class CreateFileError(Exception):
    ''' Raised when failed to create a file.
    '''
    
    
class RemoveFileError(Exception):
    ''' Raised when failed to remove a file.
    '''


class OpenFileError(Exception):
    ''' Raised when failed to open a file.
    '''
    

class CloseFileError(Exception):
    ''' Raised when failed to close a file.
    '''
    

class AppendPageError(Exception):
    ''' Raised when failed to append a page.
    '''
    

class ReadPageError(Exception):
    ''' Raised when failed to read a page.
    '''
    
    
class WritePageError(Exception):
    ''' Raised when failed to write a page.
    '''
    
    