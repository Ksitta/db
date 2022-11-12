import numpy as np

import config as cf
from paged_file.pf_file_manager import PF_FileManager
from rm_rid import RM_Rid
from rm_record import RM_Record



class RM_FileHandle:
    
    
    def __init__(self, file_manager:PF_FileManager, file_name:str):
        self.file_manager = file_manager
        self.file_name = file_name
        self.meta_file_name = file_name + cf.TABLE_META_SUFFIX
        self.data_file_name = file_name + cf.TABLE_DATA_SUFFIX
        self.meta = {}
        
        
    def set_meta(self, meta:dict) -> None:
        ''' Set the file meta info, used when this file was just created.
            Store the meta info to .meta file after calculating.
        args:
            meta: dict, data structure = {
                'record_length': int,               # MUST, should be specified when creating the file
                'record_per_page': int,             # OPTIONAL, will be calculated from record_length
                'bitmap_length': int,               # OPTIONAL, will be calculated from record_length
                'page_number': int,                 # OPTIONAL, will be initialized with 0
                'record_number': int,               # OPTIONAL, will be initialized with 0
                'next_free_page': int,              # OPTIONAL, will be initialized with -1
                'column_number': int,               # MUST, should be specified when creating the file
                'columns': List[Dict] = [ {         # MUST, a list of dicts, the column meta info
                    'column_type': int,             # MUST, the column type enum
                    'column_size': int,             # MUST, the column data size
                    'column_name_length': int       # MUST, the column name length
                    'column_name': str              # MUST, the column name, len = column_name_length
                }, {...}, ... ]
            }
        '''
        # calc the meta info and save it
        
        
    def get_meta(self) -> dict:
        ''' Read the meta info from the .meta file. Organize it as a dict
            with the data structure as specified in set_meta() and return it. 
        '''
        
    
    def get_record(self, rid:RM_Rid) -> RM_Record:
        ''' Get a record by its rid.
        '''
        
    
    def insert_record(self, data:bytes) -> RM_Rid:
        ''' Insert a record, return the allocated rid.
        args:
            data: bytes, len >= record_size, will insert data[:record_size] as the record.
        return:
            RM_Rid, the inserted position.
        '''
        
    
    def remove_record(self, rid:RM_Rid) -> None:
        ''' Remove a record by its rid.
        '''
        
    
    def update_record(self, rid:RM_Rid, data:bytes) -> None:
        ''' Update a record with new data on a specific rid.
        args:
            rid: RM_Rid, the record identifier to be updated.
            data: bytes, len >= record_size, will interpret data[:record_size] as a record.
        '''
    

if __name__ == '__main__':
    pass
