import os
import sys
import time
import numpy as np
from typing import Tuple, List, Dict, Union

import config as cf
from record_management.rm_rid import RM_Rid



class IX_IndexHandle:
    
    
    def __init__(self, file_name:str, index_no:int,
        file_id:int, field_type:int, field_size:int) -> None:
        ''' Init the index handle for <file_name>.<index_no>.
        args:
            file_name: str, the name of the file to be indexed, without extension.
            index_no: int, the index number on the file.
            fild_id: int, the file id of the opened file returned by pf_manager.
            field_type: int, in {TYPE_INT, TYPE_FLOAT, TYPE_STR}.
            field_size: int, the field size in bytes.
        '''
    
    
    def insert_entry(self, value:Union[int, float, str], rid:RM_Rid) -> None:
        ''' Insert an entry to the index.
        '''
    
    
    def remove_entry(self, value:Union[int, float, str], rid:RM_Rid) -> None:
        ''' Remove an entry.
        '''
    
    
    def sync(self) -> None:
        ''' Sync the index to paged files.
        '''


if __name__ == '__main__':
    pass
