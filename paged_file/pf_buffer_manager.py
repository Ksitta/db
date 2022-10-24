import numpy as np

import global_vars as gv
from utils.lru_list import LRUList


class PF_BufferManager:
    
    
    def __init__(self):
        self.page_buffer = np.zeros((gv.BUFFER_CAPACITY, gv.PAGE_SIZE), dtype=np.uint8)
        self.dirty = np.zeros(gv.BUFFER_CAPACITY, dtype=np.bool)
        self.lru_list = LRUList(gv.BUFFER_CAPACITY)
        
        
    def write_page(self, page_no:int, data:np.ndarray):
        self.page_buffer[page_no] = data
    
    
    def read_page(self, page_no:int):
        return self.page_buffer[page_no]


if __name__ == '__main__':
    pass