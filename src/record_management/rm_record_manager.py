import os
import sys
import time
import numpy as np

import config.pf_config as cf
from paged_file.pf_file_manager import PF_FileManager
from rm_file_handle import RM_FileHandle


class RM_RecordManager:
    ''' The record manager, a higher-level client of PF_FileManager,
    in charge of managing the files in record level.
    '''
    
    def __init__(self):
        self.file_manager = PF_FileManager()
        
        
    def create_file(self, file_name:str, record_size:int):
        ''' Create a file with fixed record size.
        '''
        
        
    def remove_file(self, file_name:str):
        ''' Remove a file from the disk.
        '''
        
    
    def open_file(self, file_name:str) -> RM_FileHandle:
        ''' Open a file by file name and return the file handle.
        '''
    
    
    def close_file(self, file_name:str):
        ''' Close a file by file name.
        '''
        


if __name__ == '__main__':
    pass