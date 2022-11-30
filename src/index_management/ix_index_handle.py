import os
import sys
import time
import numpy as np
from typing import Tuple, List, Dict, Union

import config as cf
from record_management.rm_rid import RM_Rid
from errors.err_index_management import *



class IX_IndexHandle:
    
    
    def __init__(self, file_name:str, index_no:int, meta_file_id:int, data_file_id:int) -> None:
        ''' Init the index handle for <file_name>.<index_no>.
        args:
            file_name: str, the name of the file to be indexed, without extension.
            index_no: int, the index number on the file.
            meta_file_id: int, the meta file id of the index.
            data_file_id: int, the data file id of the index.
        '''
        self.file_name = file_name
        self.index_no = index_no
        self.meta_file_id = meta_file_id
        self.data_file_id = data_file_id
        self.meta = dict()
        self.meta_modified = False
        self.is_opened = True
        
    
    def init_meta(self, meta:dict) -> None:
        ''' Init the index meta info. Use ONLY when the index was just created.
        args:
            meta: dict, data structure = {
                'field_type': int,                      # MUST, in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
                'field_size': int,                      # MUST, the field size in bytes.
            } TO BE CONTINUED ...
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        
    
    def read_meta(self) -> dict:
        ''' Read the meta info from .ixmeta file. Organized as a dict,
            update self.meta, and return it.
            The returned dict must be READ ONLY. Do not modify it out of this class.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
        
    
    def sync_meta(self) -> None:
        ''' Sync self.meta to the .ixmeta file. Use this interface since we do not
            want to write .ixmeta file each time we modify the index entries.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
    
    
    def insert_entry(self, value:Union[int, float, str], rid:RM_Rid) -> None:
        ''' Insert an entry to the index.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')
    
    
    def remove_entry(self, value:Union[int, float, str], rid:RM_Rid) -> None:
        ''' Remove an entry.
        '''
        if not self.is_opened:
            raise IndexNotOpenedError(f'Index {self.file_name}.{self.index_no} not opened.')


if __name__ == '__main__':
    pass
