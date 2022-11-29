import os
import sys
import time
import numpy as np
from typing import List, Dict, Tuple, Union

import config as cf
from paged_file.pf_manager import pf_manager
from index_management.ix_index_handle import IX_IndexHandle


class IX_Manager:
    
    
    def __init__(self) -> None:
        self.opened_indicies: Dict[str, IX_IndexHandle] = dict()
        
    
    def create_index(self, file_name:str, index_no:int) -> None:
        ''' Create index meta and data files with a specific index number.
        args:
            file_name: str, the index file name with out extension.
            index_no: int, the index number of the above file, must be unique for each file.
        '''
        pf_manager.create_file(f'{file_name}.{index_no}{cf.INDEX_META_SUFFIX}')
        pf_manager.create_file(f'{file_name}.{index_no}{cf.INDEX_DATA_SUFFIX}')
        
    
    def remove_index(self, file_name:str, index_no:int) -> None:
        ''' Remove the index <file_name>.<index_no>.
        '''
        pf_manager.remove_file(f'{file_name}.{index_no}{cf.INDEX_META_SUFFIX}')
        pf_manager.remove_file(f'{file_name}.{index_no}{cf.INDEX_META_SUFFIX}')
    
    
    def open_index(self, file_name:str, index_no:int) -> IX_IndexHandle:
        ''' Open the index <file_name>.<index_no> and return its index handle.
        return: IX_IndexHandle, the index handle for further index operations.
        '''
    
    
    def close_index(self, file_name:str, index_no:int) -> None:
        ''' Close the index <file_name>.<index_no>
        '''
        
        
    def query_index(self, file_name:str) -> List[int]:
        ''' Query all created index_no by a file name.
        args:
            file_name: str, the file name without extension.
        return: List[int], a list of index numbers created under the file_name.
            A returned list with len == 0 means no indicies have been created.
        '''


if __name__ == '__main__':
    pass
