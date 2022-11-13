import struct
import numpy as np

import config as cf
from paged_file.pf_file_manager import pf_manager
from record_management.rm_rid import RM_Rid
from record_management.rm_record import RM_Record
from record_management.rm_page_header import RM_PageHeader
from errors.err_record_management import *


class RM_FileHandle:
    
    
    def __init__(self, file_name:str, meta_file_id:int, data_file_id:int):
        ''' Init the FileHandle.
        args:
            file_name: str the file name without extension.
            meta_file_id: int, the meta file id.
            data_file_id: int, the data_file_id.
        '''
        self.file_name = file_name
        self.meta_file_name = file_name + cf.TABLE_META_SUFFIX
        self.data_file_name = file_name + cf.TABLE_DATA_SUFFIX
        self.meta_file_id = meta_file_id
        self.data_file_id = data_file_id
        self.meta = {}
        self.is_opened = True
        
        
    def write_meta(self, meta:dict) -> None:
        ''' Set the file meta info, used ONLY when this file was just created.
            Store the meta info to .meta file after calculating.
        args:
            meta: dict, data structure = {
                'record_size': int,                 # MUST, should be specified when creating the file
                'record_per_page': int,             # OPTIONAL, will be calculated from record_length
                'bitmap_size': int,                 # OPTIONAL, will be calculated from record_length
                'meta_page_number': int,            # OPTIONAL, will be calculated from meta info 
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
        # check if meta has been set
        page_cnt = pf_manager.get_page_cnt(self.meta_file_id)
        if page_cnt != 0:
            raise WriteMetaError(f'File meta has already been written.')
        # calc the meta info
        record_size = meta['record_size']
        max_record_size = cf.PAGE_SIZE - RM_PageHeader.size() - 1
        if record_size > max_record_size:
            raise WriteMetaError(f'Record size {record_size} is too large, must <= {max_record_size}.')
        record_per_page = int((cf.PAGE_SIZE-RM_PageHeader.size()-1) / (record_size+1/8))
        bitmap_size = (record_per_page + 7) // 8
        page_number = 0
        record_number = 0
        next_free_page = -1
        column_number = meta['column_number']
        columns = meta['columns']
        total_size = 32     # 8 ints
        for column in columns:
            total_size += (12 + column['column_name_length'])
        meta_page_number = (total_size + cf.PAGE_SIZE - 1) // cf.PAGE_SIZE
        meta = {'record_size': record_size, 'record_per_page': record_per_page, 'bitmap_size': bitmap_size,
            'meta_page_number': meta_page_number, 'page_number': page_number, 'record_number': record_number,
            'next_free_page': next_free_page, 'column_number': column_number, 'columns': columns}
        self.meta = meta
        # store meta info in the pages
        offset = 0
        meta_pages = np.zeros((cf.PAGE_SIZE * meta_page_number,), dtype=np.uint8)
        data = struct.pack(f'{cf.BYTE_ORDER}iiiiiiii', record_size, record_per_page, bitmap_size,
            meta_page_number, page_number, record_number, next_free_page, column_number)
        meta_pages[offset:offset+len(data)] = np.frombuffer(data, dtype=np.uint8)
        offset += len(data)
        for column in columns:
            data = struct.pack(f'{cf.BYTE_ORDER}iii', column['column_type'],
                column['column_size'], column['column_name_length'])
            meta_pages[offset:offset+len(data)] = np.frombuffer(data, dtype=np.uint8)
            offset += len(data)
            data = bytes(column['column_name'], encoding='utf-8')[:column['column_name_length']]
            meta_pages[offset:offset+len(data)] = np.frombuffer(data, dtype=np.uint8)
            offset += len(data)
        meta_pages = meta_pages.reshape((meta_page_number, cf.PAGE_SIZE))
        for i in range(meta_page_number):
            pf_manager.append_page(self.meta_file_id, meta_pages[i,:])
        
        
    def read_meta(self) -> dict:
        ''' Read the meta info from the .meta file. Organize it as a dict
            with the data structure as specified in write_meta().
            Set self.meta with this dict and return it.
            The returned dict should be READ ONLY, do not modify it out of this class.
        '''
        # check if the meta info could be read
        page_cnt = pf_manager.get_page_cnt(self.meta_file_id)
        if page_cnt <= 0:
            raise ReadMetaError(f'No meta info has been stored on the file.')
        first_page = pf_manager.read_page(self.meta_file_id, 0)
        (meta_page_number,) = struct.unpack(f'{cf.BYTE_ORDER}i', first_page[12:16].tobytes())
        meta_pages = [first_page]
        for i in range(1, meta_page_number):
            meta_pages.append(pf_manager.read_page(self.meta_file_id, i))
        data = np.concatenate(meta_pages).tobytes()
        meta = dict()
        (record_size, record_per_page, bitmap_size, meta_page_number, page_number, record_number,
            next_free_page, column_number) = struct.unpack(f'{cf.BYTE_ORDER}iiiiiiii', data[:32])
        offset = 32
        columns = []
        for _ in range(column_number):
            (column_type, column_size, column_name_length) = struct.unpack(f'{cf.BYTE_ORDER}iii', data[offset:offset+12])
            offset += 12
            column_name = str(data[offset:offset+column_name_length], encoding='utf-8')
            offset += column_name_length
            columns.append({'column_type': column_type, 'column_size': column_size,
                'column_name_length': column_name_length, 'column_name': column_name})
        meta = {'record_size': record_size, 'record_per_page': record_per_page, 'bitmap_size': bitmap_size,
            'meta_page_number': meta_page_number, 'page_number': page_number, 'record_number': record_number,
            'next_free_page': next_free_page, 'column_number': column_number, 'columns': columns}
        self.meta = meta
        return meta
                
    
    def get_record(self, rid:RM_Rid) -> RM_Record:
        ''' Get a record by its rid.
        '''
        
    
    def insert_record(self, data:np.ndarray) -> RM_Rid:
        ''' Insert a record, return the allocated rid.
        args:
            data: np.ndarray[(>=record_size,), uint8], will interpret data[:record_size] as a record.
        return:
            RM_Rid, the inserted position.
        '''
        
    
    def remove_record(self, rid:RM_Rid) -> None:
        ''' Remove a record by its rid.
        '''
        
    
    def update_record(self, rid:RM_Rid, data:np.ndarray) -> None:
        ''' Update a record with new data on a specific rid.
        args:
            rid: RM_Rid, the record identifier to be updated.
            data: np.ndarray[(>=record_size,), uint8], will interpret data[:record_size] as a record.
        '''
    

if __name__ == '__main__':
    pass
