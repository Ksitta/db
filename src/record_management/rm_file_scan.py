from typing import Union, List, Tuple

import config as cf
from utils.enums import CompOp
from record_management.rm_record import RM_Record
from record_management.rm_file_handle import RM_FileHandle


class RM_FileScan:
    ''' Scan all records within a file, possibly restricted with some conditions.
    '''
    
    def __init__(self):
        ''' Init the file scan instance.
        '''
        self.is_opened = False
        self.file_handle = None
        
    
    def open_scan(self, file_handle:RM_FileHandle, value:Union[int,float,str,None]=None,
            field_type:int=cf.TYPE_INT, field_size:int=cf.SIZE_INT, field_off:int=0,
            comp_op:CompOp=CompOp.NO) -> None:
        ''' Open the file scan.
        args:
            file_handle: RM_FileHandle, the file to be scanned.
            value: Union[int,float,str,None], the field to be compared to,
                if value is None, the following parameters (field_type, field_size,
                field_off, and comp_op) will all be ignored, and all records will
                be returned by the scanning operation.
            field_type: int, in {cf.TYPE_INT, cf.TYPE_FLAOT, cf.TYPE_STR}.
            field_size: int, the size of the field to be compared.
            field_off: int, the field offset within the record.
            comp_op: CompOp, the filter condition.
        '''
        
    
    def next(self) -> Union[RM_Record, None]:
        ''' Return the next scanned record that satisfys the filtering condition.
        '''
        
    
    def close_scan(self) -> None:
        ''' Close the file scan.
        '''
    

if __name__ == '__main__':
    pass
