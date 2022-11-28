import struct
import numpy as np
from typing import Union, List, Tuple

import config as cf
from utils.enums import CompOp
from utils.bitmap import Bitmap
from paged_file.pf_manager import pf_manager
from record_management.rm_rid import RM_Rid
from record_management.rm_record import RM_Record
from record_management.rm_file_handle import RM_FileHandle
from record_management.rm_page_header import RM_PageHeader
from errors.err_record_management import * 


class RM_FileScan:
    ''' Scan all records within a file, possibly restricted with some conditions.
    '''
    
    def __init__(self):
        ''' Init the file scan instance.
        '''
        self.is_opened = False
        self.file_handle = None
        
    
    def open_scan(self, file_handle:RM_FileHandle, comp_op:CompOp=CompOp.NO,
            field_value:Union[int,float,str]=None, field_type:int=cf.TYPE_INT,
            field_size:int=cf.SIZE_INT, field_off:int=0) -> None:
        ''' Open the file scan.
        args:
            file_handle: RM_FileHandle, the file to be scanned. During scanning,
                the file should not be modified (i.e. do no insert, update or remove records).
            comp_op: CompOp, the filter condition. If comp_op == CompOp.NO, the following
                parameters (field_value, field_type, field_size, and field_off) will all be
                ignored, and all records will be returned by the scanning operation.
            field_value: Union[int,float,str,None], the field to be compared with.
            field_type: int, in {cf.TYPE_INT, cf.TYPE_FLAOT, cf.TYPE_STR}.
            field_size: int, the size of the field to be compared.
            field_off: int, the field offset within the record.
        '''
        self.is_opened = True
        self.file_handle = file_handle
        if not file_handle.is_opened:
            raise OpenScanError(f'The file to be scanned is not opened.')
        if field_type not in {cf.TYPE_INT, cf.TYPE_FLOAT, cf.TYPE_STR}:
            raise OpenScanError(f'Wrong field type: {field_type}.')
        (self.comp_op, self.field_value, self.field_type, self.field_size, self.field_off) \
            = comp_op, field_value, field_type, field_size, field_off
        
    
    def next(self) -> Union[RM_Record, None]:
        ''' Yield the next scanned record that satisfies the filtering condition.
        note:
            Should use the generator like this:
            ``` python
                for record in file_scan.next():
                    # do something with the record
            ```
        '''
        if not self.is_opened: return None
        file_handle = self.file_handle
        if not file_handle.is_opened:
            raise ScanNextError(f'The file to be scanned is not opened.')
        BYTE_ORDER = cf.BYTE_ORDER
        (comp_op, field_value, field_type, field_size, field_off) \
            = self.comp_op, self.field_value, self.field_type, self.field_size, self.field_off
        meta = file_handle.meta
        data_file_id = file_handle.data_file_id
        record_number = meta['record_number']
        record_per_page = meta['record_per_page']
        header_size = RM_PageHeader.size()
        bitmap_size = meta['bitmap_size']
        idx, page_no = 0, meta['next_free_page']
        while idx < record_number:
            if page_no == cf.INVALID:
                raise ScanNextError(f'Next page is invalid.')
            page_data = pf_manager.read_page(data_file_id, page_no)
            page_header:RM_PageHeader = RM_PageHeader.deserialize(page_data[:header_size])
            bitmap:Bitmap = Bitmap.deserialize(record_per_page,
                page_data[header_size:header_size+bitmap_size])
            for slot_no in range(record_per_page):
                if not bitmap.get_bit(slot_no): continue
                idx += 1
                record = file_handle.get_record(rid=RM_Rid(page_no, slot_no))
                if comp_op == CompOp.NO: # ignore comparison
                    yield record
                else:
                    if field_type == cf.TYPE_INT:
                        (field,) = struct.unpack(f'{BYTE_ORDER}i', record.data[field_off:field_off+4].tobytes())
                    elif field_type == cf.TYPE_FLOAT:
                        (field,) = struct.unpack(f'{BYTE_ORDER}d', record.data[field_off:field_off+8].tobytes())
                    elif field_type == cf.TYPE_STR:
                        (field,) = struct.unpack(f'{BYTE_ORDER}{field_size}s',
                            record.data[field_off:field_off+field_size].tobytes())
                        field = str(field, encoding='utf-8').strip('\0')
                    else: raise ScanNextError(f'Wrong field type: {field_type}.')
                    valid = True
                    if comp_op == CompOp.EQ: valid = (field == field_value)
                    elif comp_op == CompOp.LT: valid = (field < field_value)
                    elif comp_op == CompOp.GT: valid = (field > field_value)
                    elif comp_op == CompOp.LE: valid = (field <= field_value)
                    elif comp_op == CompOp.GE: valid = (field >= field_value)
                    elif comp_op == CompOp.NE: valid = (field != field_value)
                    else: raise ScanNextError(f'Wrong comp_op: {comp_op}.')
                    if valid: yield record
            page_no = page_header.next_free
        return None
        
    
    def close_scan(self) -> None:
        ''' Close the file scan.
        '''
        self.is_opened = False
    

if __name__ == '__main__':
    pass
