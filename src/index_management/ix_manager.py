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
        
    
    def create_index(self, file_name:str, index_no:int,
        field_type:int, field_size:int) -> None:
        ''' Create an index on a field with specified field type and size.
            Will create an index file named <file_name>.<index_no>.<TABEL_INDEX_SUFFIX>.
        args:
            file_name: str, the index file name with out extension.
            index_no: int, the index number of the above file, must be unique for each file.
            field_type: int, in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
            field_size: int, the field size in bytes.
        '''
    
    
    def remove_index(self, file_name:str, index_no:int) -> None:
        ''' Remove the index <file_name>.<index_no>.
        '''
        
    
    
    def open_index(self, file_name:str, index_no:int) -> IX_IndexHandle:
        ''' Open the index <file_name>.<index_no> and return its index handle.
        '''
    
    
    def close_index(self, file_name:str, index_no:int) -> None:
        ''' Close the index <file_name>.<index_no>
        '''


if __name__ == '__main__':
    pass
