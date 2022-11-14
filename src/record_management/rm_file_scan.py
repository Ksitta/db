from rm_file_handle import RM_FileHandle


class RM_FileScan:
    ''' Scan all records within a file, possibly restricted with some conditions.
    '''
    
    def __init__(self, file_handle:RM_FileHandle):
        self.file_handle = file_handle
        

if __name__ == '__main__':
    pass
