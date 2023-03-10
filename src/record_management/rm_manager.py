import os
import sys
import time
import numpy as np
from typing import Dict

import config as cf
from paged_file.pf_manager import pf_manager
from record_management.rm_file_handle import RM_FileHandle


class RM_Manager:
    ''' The record manager, a higher-level client of PF_Manager,
    in charge of managing the files in record level.
    '''
    
    def __init__(self):
        self.opened_files: Dict[str, RM_FileHandle] = dict()
        
        
    def create_file(self, file_name:str):
        ''' Create a file with fixed record size.
        args:
            file_name: str, the full file path without ext, like 'dir/table',
                the same for other interfaces in RM_Manager.
        '''
        pf_manager.create_file(file_name + cf.TABLE_META_SUFFIX)
        pf_manager.create_file(file_name + cf.TABLE_DATA_SUFFIX)
        
        
    def remove_file(self, file_name:str):
        ''' Remove a file from the disk.
        '''
        pf_manager.remove_file(file_name + cf.TABLE_META_SUFFIX)
        pf_manager.remove_file(file_name + cf.TABLE_DATA_SUFFIX)
        
    
    def open_file(self, file_name:str) -> RM_FileHandle:
        ''' Open a file by file name and return the file handle.
        '''
        if file_name in self.opened_files:
            return self.opened_files[file_name]
        meta_file_id = pf_manager.open_file(file_name + cf.TABLE_META_SUFFIX)
        data_file_id = pf_manager.open_file(file_name + cf.TABLE_DATA_SUFFIX)
        file_handle = RM_FileHandle(file_name, meta_file_id, data_file_id)
        self.opened_files[file_name] = file_handle
        return file_handle
    
    
    def close_file(self, file_name:str):
        ''' Close a file by file name.
        '''
        pf_manager.close_file(file_name + cf.TABLE_META_SUFFIX)
        pf_manager.close_file(file_name + cf.TABLE_DATA_SUFFIX)
        handle:RM_FileHandle = self.opened_files.pop(file_name, None)
        if handle: handle.is_opened = False
        
        
rm_manager = RM_Manager()


if __name__ == '__main__':
    pass