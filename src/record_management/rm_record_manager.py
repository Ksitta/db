import os
import sys
import time
import numpy as np

import config as cf
from paged_file.pf_file_manager import pf_manager
from record_management.rm_file_handle import RM_FileHandle


class RM_RecordManager:
    ''' The record manager, a higher-level client of PF_FileManager,
    in charge of managing the files in record level.
    '''
    
    def __init__(self):
        self.opened_files = {}
        
        
    def create_file(self, file_name:str):
        ''' Create a file with fixed record size.
        args:
            file_name: str, the full file path without ext, like 'dir/table',
                the same for other interfaces in RM_RecordManager.
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
        self.opened_files.pop(file_name, None)
        
        
rm_manager = RM_RecordManager()


if __name__ == '__main__':
    pass