import os
import struct
import numpy as np
from typing import NoReturn, List, Tuple, Dict, Set, Union

import config as cf
from errors.err_paged_file import *
from utils.lru_list import LRUList


class PF_FileManager:
    ''' The paged file manager.
    '''
    
    
    def __init__(self):
        ''' Init the paged file manager.
        '''
        # disk management
        self.page_cnt: Dict[int, int] = {}
        # file_name <=> file_id mapping
        self.file_name_to_id: Dict[str, int] = {}
        self.file_id_to_name: Dict[int, str] = {}
        # buffer management
        self.buffer: np.ndarray = np.zeros((cf.BUFFER_CAPACITY, cf.PAGE_SIZE), dtype=np.uint8)
        self.lru_list = LRUList(cf.BUFFER_CAPACITY)
        self.dirty: np.ndarray = np.zeros(cf.BUFFER_CAPACITY, dtype=np.bool)
        self.buffered_pages: Dict[int, Set[int]] = {}   # file_id to a set of buffer_ids
        # (file_id, page_id) <=> buffer_id mapping
        self.pair_to_buffer_id: Dict[Tuple[int, int], int] = {}
        self.buffer_to_file_id: np.ndarray = np.full(cf.BUFFER_CAPACITY, cf.INVALID, dtype=np.int64)
        self.buffer_to_page_id: np.ndarray = np.full(cf.BUFFER_CAPACITY, cf.INVALID, dtype=np.int64)
        
        
    def _read_disk(self, file_id:int, page_id:int) -> np.ndarray:
        ''' Read a page from a file on disk directly.
        return: np.ndarray[(PAGE_SIZE,), uint8]
        '''
        if file_id not in self.file_id_to_name:
            raise ReadDiskError(f'File {file_id} has not been opened.')
        os.lseek(file_id, page_id * cf.PAGE_SIZE, os.SEEK_SET)
        data = os.read(file_id, cf.PAGE_SIZE)
        if len(data) < cf.PAGE_SIZE:
            raise ReadDiskError(f'Read page failed. Read bytes: {len(data)}.')
        return np.frombuffer(data, dtype=np.uint8, count=cf.PAGE_SIZE).copy()
            
    
    def _write_disk(self, file_id:int, page_id:int, data:np.ndarray):
        ''' Write a page to a file on disk directly.
        args:
            data: np.ndarray[(>=PAGE_SIZE,), uint8], the data to be written.
        '''
        if file_id not in self.file_id_to_name:
            raise WriteDiskError(f'File {file_id} has not been opened.')
        if len(data) < cf.PAGE_SIZE:
            raise WriteDiskError(f'Not enough data to write a page.')
        os.lseek(file_id, page_id * cf.PAGE_SIZE, os.SEEK_SET)
        os.write(file_id, data[:cf.PAGE_SIZE].tobytes())
        
        
    def _alloc_buffer(self) -> int:
        ''' Allocate a buffer page.
            Find a buffer page using LRU strategy.
            If the page is pinned, unpin it first.
            If the page is dirty (only if it is pinned), write back to disk.
        return: int, the buffer id.
        '''
        buffer_id = self.lru_list.find()
        self._dealloc_buffer(buffer_id)
        self.lru_list.access(buffer_id)
        return buffer_id
        
    
    def _dealloc_buffer(self, buffer_id:int):
        ''' Dealloc a buffer page.
            If the page is dirty, write back to disk first.
        '''
        file_id = self.buffer_to_file_id[buffer_id]
        if file_id == cf.INVALID: return    # unpinned page, need not to deallocate
        page_id = self.buffer_to_page_id[buffer_id]
        if self.dirty[buffer_id]:
            self._write_disk(file_id, page_id, self.buffer[buffer_id])
        self.dirty[buffer_id] = False
        self.pair_to_buffer_id.pop((file_id, page_id), cf.INVALID)
        self.buffer_to_file_id[buffer_id] = cf.INVALID
        self.buffer_to_page_id[buffer_id] = cf.INVALID
        self.lru_list.free(buffer_id)
        if file_id in self.buffered_pages:
            if buffer_id in self.buffered_pages[file_id]:
                self.buffered_pages[file_id].remove(buffer_id)
            if len(self.buffered_pages[file_id]) == 0:
                self.buffered_pages.pop(file_id, {})
                
    
    def get_page_cnt(self, file_id:int) -> int:
        ''' Get the page cnt of a specific file.
        '''
        if file_id not in self.file_id_to_name:
            raise GetPageCntError(f'File {file_id} has not been opened.')
        return self.page_cnt[file_id]
    
    
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
        file_id = os.open(file_name, cf.FILE_OPEN_MODE)
        file_size = os.lseek(file_id, 0, os.SEEK_END)
        self.page_cnt[file_id] = file_size // cf.PAGE_SIZE
        self.file_name_to_id[file_name] = file_id
        self.file_id_to_name[file_id] = file_name
        return file_id
    
    
    def close_file(self, file:Union[int,str]):
        ''' Closed an opened file by its file id or file name.
        args:
            file: int or str, int for file_id, str for file_name.
        '''
        file_id = file
        if type(file) == str:
            file_id = self.file_name_to_id.get(file, cf.INVALID)
        if file_id not in self.file_id_to_name:
            raise CloseFileError(f'File {file_id} has not been opened.')
        self.flush_file(file_id)
        os.close(file_id)
        file_name = self.file_id_to_name[file_id]
        self.file_id_to_name.pop(file_id)
        self.file_name_to_id.pop(file_name)
        self.page_cnt.pop(file_id, cf.INVALID)
        
    
    def sync_file(self, file_id:int):
        ''' Sync the file from buffer to the disk.
            Mark all buffered pages as not dirty but do not change the buffer content.
        '''
        buffer_ids = self.buffered_pages.get(file_id, {})
        for buffer_id in buffer_ids:
            if not self.dirty[buffer_id]: continue
            page_id = self.buffer_to_page_id[buffer_id]
            self._write_disk(file_id, page_id, self.buffer[buffer_id])
            self.dirty[buffer_id] = False
        
        
    def flush_file(self, file_id:int):
        ''' Flush the file from buffer to the disk.
            Unpin all buffer pages from the buffer.
            After flushing, the buffer will not contain any pages of the file.
        '''
        buffer_ids = self.buffered_pages.get(file_id, {})
        for buffer_id in buffer_ids:
            page_id = self.buffer_to_page_id[buffer_id]
            if self.dirty[buffer_id]:
                self._write_disk(file_id, page_id, self.buffer[buffer_id])
            self.dirty[buffer_id] = False
            self.pair_to_buffer_id.pop((file_id, page_id), cf.INVALID)
            self.buffer_to_file_id[buffer_id] = cf.INVALID
            self.buffer_to_page_id[buffer_id] = cf.INVALID
            self.lru_list.free(buffer_id)
        self.buffered_pages.pop(file_id, {})
        

    def allocate_pages(self, file_id:int, page_cnt:int) -> int:
        ''' Allocate <page_cnt> empty pages continuously at the end of the file.
            Only write empty data to the buffer.
        args:
            file_id: int, the file to be alloacted.
            page_cnt: int, the number of pages to be allocated.
        return: int, the start page id of the allocated pages.
        '''
        if file_id not in self.file_id_to_name:
            raise AppendPageError(f'File {file_id} has not been opened.')
        page_id = self.page_cnt[file_id]
        self.page_cnt[file_id] = page_id + page_cnt
        if file_id not in self.buffered_pages:
            self.buffered_pages[file_id] = set()
        for i in range(page_cnt):
            buffer_id = self._alloc_buffer()
            self.buffer[buffer_id] = np.zeros(cf.PAGE_SIZE, dtype=np.uint8)
            self.dirty[buffer_id] = True
            self.pair_to_buffer_id[(file_id, page_id+i)] = buffer_id
            self.buffer_to_file_id[buffer_id] = file_id
            self.buffer_to_page_id[buffer_id] = page_id + i
            self.buffered_pages[file_id].add(buffer_id)
        return page_id
        
        
    def append_page(self, file_id:int, data:np.ndarray=None) -> int:
        ''' Append a new page at the end of the file.
        args:
            file_id: int,
            data: np.ndarray[(>=PAGE_SIZE,), uint8] or None, the data to be appended.
                If None, an empty page will be appended.
        return: int, the appended page id.
        '''
        if file_id not in self.file_id_to_name:
            raise AppendPageError(f'File {file_id} has not been opened.')
        if data is None: data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8)
        if len(data) < cf.PAGE_SIZE:
            raise AppendPageError(f'Data size is not enough to append a page.')
        page_id = self.page_cnt[file_id]
        self.page_cnt[file_id] = page_id + 1
        buffer_id = self._alloc_buffer()
        self.buffer[buffer_id] = data[:cf.PAGE_SIZE]
        self.dirty[buffer_id] = True
        self.pair_to_buffer_id[(file_id, page_id)] = buffer_id
        self.buffer_to_file_id[buffer_id] = file_id
        self.buffer_to_page_id[buffer_id] = page_id
        if file_id not in self.buffered_pages:
            self.buffered_pages[file_id] = set()
        self.buffered_pages[file_id].add(buffer_id)
        return page_id
        
    
    def read_page(self, file_id:int, page_id:int) -> np.ndarray:
        ''' Read a page from the file.
            If the page is buffered, read it from the buffer.
            If not, read it from the disk and buffer it.
        return: np.ndarray[(PAGE_SIZE,), uint8]
        '''
        if file_id not in self.file_id_to_name:
            raise ReadPageError(f'File {file_id} has not been opened.')
        if page_id >= self.page_cnt[file_id]:
            raise ReadPageError(f'Page {page_id} has not been allocated.')
        buffer_id = self.pair_to_buffer_id.get((file_id, page_id), cf.INVALID)
        if buffer_id == cf.INVALID: # not buffered
            os.lseek(file_id, page_id * cf.PAGE_SIZE, os.SEEK_SET)
            data = os.read(file_id, cf.PAGE_SIZE)
            if len(data) != cf.PAGE_SIZE:
                raise ReadPageError(f'Read page failed. Read bytes: {len(data)}.')
            buffer_id = self._alloc_buffer()
            self.buffer[buffer_id] = np.frombuffer(data, dtype=np.uint8, count=cf.PAGE_SIZE)
            self.pair_to_buffer_id[(file_id, page_id)] = buffer_id
            self.buffer_to_file_id[buffer_id] = file_id
            self.buffer_to_page_id[buffer_id] = page_id
            if file_id not in self.buffered_pages:
                self.buffered_pages[file_id] = set()
            self.buffered_pages[file_id].add(buffer_id)
            return np.frombuffer(data, dtype=np.uint8, count=cf.PAGE_SIZE).copy()
        self.lru_list.access(buffer_id)
        return self.buffer[buffer_id].copy()
            
    
    def write_page(self, file_id:int, page_id:int, data:np.ndarray):
        ''' Write a page to the file.
            Only write to the buffer.
        args:
            data: np.ndarray[(>=PAGE_SIZE,), uint8] the data to be written.
        '''
        if file_id not in self.file_id_to_name:
            raise WritePageError(f'File {file_id} has not been opened.')
        if len(data) < cf.PAGE_SIZE:
            raise WritePageError(f'Not enough data to write a page.')
        if page_id >= self.page_cnt[file_id]:
            raise WritePageError(f'Page {page_id} has not been allocated.')
        buffer_id = self.pair_to_buffer_id.get((file_id, page_id), cf.INVALID)
        if buffer_id == cf.INVALID:
            buffer_id = self._alloc_buffer()
            self.pair_to_buffer_id[(file_id, page_id)] = buffer_id
            self.buffer_to_file_id[buffer_id] = file_id
            self.buffer_to_page_id[buffer_id] = page_id
            if file_id not in self.buffered_pages:
                self.buffered_pages[file_id] = set()
            self.buffered_pages[file_id].add(buffer_id)
        self.buffer[buffer_id] = data[:cf.PAGE_SIZE]
        self.dirty[buffer_id] = True
    
    
pf_manager = PF_FileManager()


if __name__ == '__main__':
    pass