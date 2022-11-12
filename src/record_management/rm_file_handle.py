import numpy as np

from paged_file.pf_file_manager import PF_FileManager
from rm_rid import RM_Rid
from rm_record import RM_Record


class RM_FileHandle:
    
    
    def __init__(self, file_manager:PF_FileManager, file_name:str, file_id:int):
        self.file_manager = file_manager
        self.file_name = file_name
        self.file_id = file_id
        
    
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
