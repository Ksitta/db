import os
import struct
from tkinter import WRITABLE
import numpy as np
from typing import NoReturn, List, Tuple, Dict

import global_vars as gv
from paged_file.pf_buffer_manager import PF_BufferManager
from errors.err_paged_file import *



class PF_FileManager:
    ''' The paged file manager.
    '''
    
    
    def __init__(self):
        ''' Init the paged file manager.
        '''
        self.buffer_manager = PF_BufferManager()
        self.file_name_to_id: Dict[str, int] = {}
        self.file_id_to_name: Dict[int, str] = {}
    
    
    def create_file(self, file_name:str):
        ''' Create a paged file named <file_name>.
        '''
        if os.path.exists(file_name):
            raise CreateFileError(f'File {file_name} has been created.')
        open(file_name, 'w').close()
    
    
    def remove_file(self, file_name:str):
        ''' Remove a paged file named <file_name>.
        '''
        if not os.path.exists(file_name):
            raise RemoveFileError(f'File {file_name} does not exist.')
        os.remove(file_name)
    
    
    def open_file(self, file_name:str) -> int:
        ''' Open a created file.
        return: int, the file id.
        '''
        if file_name in self.file_name_to_id:
            raise OpenFileError(f'File {file_name} has been opened.')
        file_id = os.open(file_name, gv.FILE_OPEN_MODE)
        self.file_name_to_id[file_name] = file_id
        self.file_id_to_name[file_id] = file_name
        return file_id
    
    
    def close_file(self, file_id:int):
        ''' Closed an opened file by its file id.
        '''
        if file_id not in self.file_id_to_name:
            raise CloseFileError(f'File {file_id} has not been opened.')
        os.close(file_id)
        file_name = self.file_id_to_name[file_id]
        self.file_id_to_name.pop(file_id)
        self.file_name_to_id.pop(file_name)
        
        
    def allocate_pages(self, file_id:int, page_cnt:int) -> int:
        ''' Allocate <page_cnt> empty pages continuously at the end of the file.
        args:
            file_id: int, the file to be alloacted.
            page_cnt: int, the number of pages to be allocated.
        return: int, the start page id of the allocated pages.
        '''
        if file_id not in self.file_id_to_name:
            raise AppendPageError(f'File {file_id} has not been opened.')
        data = np.zeros(gv.PAGE_SIZE * page_cnt, dtype=np.uint8).tobytes()
        file_size = os.lseek(file_id, 0, os.SEEK_END)
        os.write(file_id, data)
        return file_size // gv.PAGE_SIZE  
        
        
    def append_page(self, file_id:int, data:bytes=None) -> int:
        ''' Append a new page at the end of the file.
        args:
            file_id: int,
            data: bytes or None, the data to be appended.
                If None, an empty page will be appended.
                If not None, len(data) must >= gv.PAGE_SIZE.
        return: int, the appended page id.
        '''
        if file_id not in self.file_id_to_name:
            raise AppendPageError(f'File {file_id} has not been opened.')
        if data == None: data = np.zeros(gv.PAGE_SIZE, dtype=np.uint8).tobytes()
        if len(data) < gv.PAGE_SIZE:
            raise AppendPageError(f'Data size is not enough to append a page.')
        file_size = os.lseek(file_id, 0, os.SEEK_END)
        os.write(file_id, data)
        return file_size // gv.PAGE_SIZE        
        
    
    def read_page(self, file_id:int, page_id:int) -> bytes:
        ''' Read a page from a file.
        return: bytes, len == gv.PAGE_SIZE.
        '''
        if file_id not in self.file_id_to_name:
            raise ReadPageError(f'File {file_id} has not been opened.')
        os.lseek(file_id, page_id * gv.PAGE_SIZE, os.SEEK_SET)
        data = os.read(file_id, gv.PAGE_SIZE)
        if len(data) < gv.PAGE_SIZE:
            raise ReadPageError(f'Read page failed. Read bytes: {len(data)}.')
        return data
            
    
    def write_page(self, file_id:int, page_id:int, data:bytes):
        ''' Write a page to a file.
        args:
            data: bytes, len >= gv.PAGE_SIZE, the data to be written.
        '''
        if file_id not in self.file_id_to_name:
            raise WritePageError(f'File {file_id} has not been opened.')
        if len(data) < gv.PAGE_SIZE:
            raise WritePageError(f'Not enough data to write a page.')
        page_cnt = os.path.getsize(self.file_id_to_name[file_id]) // gv.PAGE_SIZE
        if page_id >= page_cnt:
            raise WritePageError(f'Page has not been allocated.')
        os.lseek(file_id, page_id * gv.PAGE_SIZE, os.SEEK_SET)
        size = os.write(file_id, data[:gv.PAGE_SIZE])
        if size != gv.PAGE_SIZE:
            raise WritePageError(f'Write page failed. Written bytes: {size}.')
    

if __name__ == '__main__':
    pass