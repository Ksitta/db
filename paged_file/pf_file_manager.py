import struct
from typing import NoReturn, List, Tuple, Dict

import global_vars
from paged_file.pf_buffer_manager import PF_BufferManager
from pf_file_handle import PF_FileHandle



class PF_FileManager:
    ''' The paged file manager.
    '''
    
    
    def __init__(self):
        ''' Init the paged file manager.
        '''
        self.buffer_manager = PF_BufferManager()
    
    
    def create_file(self, file_name: str) -> int:
        ''' Create a paged file named <file_name>.
        args:
            file_name: str, the file to be created.
        return: int, the return code.
        '''
        pass
        return 0
    
    
    def destroy_file(self, file_name: str) -> int:
        ''' Delete a paged file named <file_name>.
        args:
            file_name: str, the file to be deleted.
        return: int, the return code.
        '''
        pass
        return 0
    
    
    def open_file(self, file_name:str, file_handle: PF_FileHandle) -> int:
        ''' Open a created file.
        args:
            file_name: str, the file to be deleted.
        return: int, the return code.
        '''
        pass
        return 0
    
    
    def close_file(self, file_handle: PF_FileHandle) -> int:
        ''' Closed an opened file.
        args:
            file_name: str, the file to be closed.
        return int, the return code.
        '''
        pass
        return 0
    

if __name__ == '__main__':
    pass