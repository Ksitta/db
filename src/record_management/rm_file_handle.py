import struct
import numpy as np
from typing import Union, List, Dict, Tuple, Any

import config as cf
from utils.bitmap import Bitmap
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
        self.meta_modified = False  # whether meta in mem is the same as it in disk
        self.page_headers = []
        self.bitmaps = []
        self.is_opened = True
        
        
    def _set_page_next_free(self, page_no:int, next_free:int):
        ''' Read a page, change its next_free in header, and write it back.
        '''
        page = pf_manager.read_page(self.data_file_id, page_no)
        header_size = RM_PageHeader.size()
        header = RM_PageHeader.deserialize(page[:header_size])
        header.next_free = next_free
        page[:header_size] = header.serialize()
        pf_manager.write_page(self.data_file_id, page_no, page)
        
        
    def init_meta(self, meta:dict) -> None:
        ''' Set the file meta info, used ONLY when this file was just created.
            Store the meta info to .meta file after calculating.
        args:
            meta: dict, data structure = {
                'record_size': int,                                 # MUST, should be specified when creating the file
                'record_per_page': int,                             # OPTIONAL, will be calculated from record_length
                'bitmap_size': int,                                 # OPTIONAL, will be calculated from record_length
                'meta_page_number': int,                            # OPTIONAL, will be calculated from meta info 
                'page_number': int,                                 # OPTIONAL, will be initialized with 0
                'record_number': int,                               # OPTIONAL, will be initialized with 0
                'next_free_page': int,                              # OPTIONAL, will be initialized with INVALID
                'column_number': int,                               # MUST, should be specified when creating the file
                'columns': List[Dict] = [ {                         # MUST, a list of dicts, the column meta info
                    'column_type': int,                             # MUST, the column type enum
                    'column_size': int,                             # MUST, the column data size
                    'column_name_length': int                       # MUST, the column name length
                    'column_name': str                              # MUST, the column name, len = column_name_length
                    'column_default_en': bool                       # MUST, whether to enable column default value
                    'column_default': np.ndarray                    # MUST, if en == False, store all 0 values
                }, {...}, ... ],
                'primary_key_size': int,                            # MUST, the number of columns controlled by the primary key
                'primary_keys': List[int],                          # MUST, a list of column indices
                'foreign_key_number': int,                          # MUST, the number of foreign keys
                'foreign_keys': List[Dict] = [ {                    # MUST, a list of dicts describing each foreign key
                    'foreign_key_name_length': int,                 # MUST, the length of the foreign key name
                    'foreign_key_name': str,                        # MUST, the foreign key name
                    'target_table_name_length': int,                # MUST, the length of the target table name
                    'target_table_name': str,                       # MUST, the target table name
                    'foreign_key_size': int,                        # MUST, the number of columns controlled by the foreign key
                    'foreign_key_pairs': List[Tuple[int,int]],      # MUST, a list of pairs, (column index of the current table, ~ the target table)
                }, {...}, ...],
            }
        '''
        if not self.is_opened:
            raise FileNotOpenedError(f'File {self.file_name} not opened.')
        # check if meta has been set
        page_cnt = pf_manager.get_page_cnt(self.meta_file_id)
        if page_cnt != 0:
            raise InitMetaError(f'File meta has already been written.')
        # calc the meta info
        record_size = meta['record_size']
        max_record_size = cf.PAGE_SIZE - RM_PageHeader.size() - 1
        if record_size > max_record_size:
            raise InitMetaError(f'Record size {record_size} is too large, must <= {max_record_size}.')
        record_per_page = int((cf.PAGE_SIZE-RM_PageHeader.size()-1) / (record_size+1/8))
        bitmap_size = (record_per_page + 7) // 8
        page_number = 0
        record_number = 0
        next_free_page = cf.INVALID
        column_number = meta['column_number']
        columns = meta['columns']
        primary_key_size = meta['primary_key_size']
        primary_keys = meta['primary_keys']
        foreign_key_number = meta['foreign_key_number']
        foreign_keys = meta['foreign_keys']
        total_size = 40     # 10 fixed ints
        for col in columns:
            total_size += (13 + col['column_name_length'] + col['column_size'])
        total_size += 4 * primary_key_size
        for fk in foreign_keys:
            total_size += (12 + fk['foreign_key_name_length'] + fk['target_table_name_length'] + 8 * fk['foreign_key_size'])
        meta_page_number = (total_size + cf.PAGE_SIZE - 1) // cf.PAGE_SIZE
        meta = {'record_size': record_size, 'record_per_page': record_per_page, 'bitmap_size': bitmap_size,
            'meta_page_number': meta_page_number, 'page_number': page_number, 'record_number': record_number,
            'next_free_page': next_free_page, 'column_number': column_number, 'columns': columns,
            'primary_key_size': primary_key_size, 'primary_keys': primary_keys, 'foreign_key_number': foreign_key_number,
            'foreign_keys': foreign_keys}
        self.meta = meta
        # store meta info in the pages
        BYTE_ORDER = cf.BYTE_ORDER
        off = 0
        meta_pages = np.zeros((cf.PAGE_SIZE * meta_page_number,), dtype=np.uint8)
        data = struct.pack(f'{BYTE_ORDER}iiiiiiii', record_size, record_per_page, bitmap_size,
            meta_page_number, page_number, record_number, next_free_page, column_number)
        meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
        for col in columns:
            data = struct.pack(f'{BYTE_ORDER}iii', col['column_type'],
                col['column_size'], col['column_name_length'])
            meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
            data = bytes(col['column_name'], encoding='utf-8')[:col['column_name_length']]
            meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
            data = (struct.pack(f'{BYTE_ORDER}b',bool(col['column_default_en']))
                + col['column_default'][:col['column_size']].tobytes())
            meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
        data = struct.pack(f'{BYTE_ORDER}i{primary_key_size}ii', primary_key_size,
            *(primary_keys[:primary_key_size]), foreign_key_number)
        meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
        for fk in foreign_keys:
            len1 = fk['foreign_key_name_length']
            len2 = fk['target_table_name_length']
            len3 = fk['foreign_key_size']
            pairs = list(np.array(fk['foreign_key_pairs'][:len3]).reshape(-1))
            data = struct.pack(f'{BYTE_ORDER}i{len1}si{len2}si{2*len3}i', len1,
                bytes(fk['foreign_key_name'], encoding='utf-8')[:len1], len2,
                bytes(fk['target_table_name'], encoding='utf-8')[:len2], len3, *pairs)
            meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
        meta_pages = meta_pages.reshape((meta_page_number, cf.PAGE_SIZE))
        for i in range(meta_page_number):
            pf_manager.append_page(self.meta_file_id, meta_pages[i,:])
        self.meta_modified = False
        
        
    def read_meta(self) -> dict:
        ''' Read the meta info from the .meta file. Organize it as a dict
            with the data structure as specified in init_meta().
            Set self.meta with this dict and return it.
            The returned dict should be READ ONLY, do not modify it out of this class.
        '''
        if not self.is_opened:
            raise FileNotOpenedError(f'File {self.file_name} not opened.')
        # check if the meta info could be read
        BYTE_ORDER = cf.BYTE_ORDER
        page_cnt = pf_manager.get_page_cnt(self.meta_file_id)
        if page_cnt <= 0:
            raise ReadMetaError(f'No meta info has been stored on the file.')
        first_page = pf_manager.read_page(self.meta_file_id, 0)
        (meta_page_number,) = struct.unpack(f'{BYTE_ORDER}i', first_page[12:16].tobytes())
        meta_pages = [first_page]
        for i in range(1, meta_page_number):
            meta_pages.append(pf_manager.read_page(self.meta_file_id, i))
        data = np.concatenate(meta_pages).tobytes()
        (record_size, record_per_page, bitmap_size, meta_page_number, page_number, record_number,
            next_free_page, column_number) = struct.unpack(f'{BYTE_ORDER}iiiiiiii', data[:32])
        off = 32
        columns = []
        for _ in range(column_number):
            (column_type, column_size, column_name_length) = struct.unpack(f'{BYTE_ORDER}iii', data[off:off+12])
            off += 12
            column_name = str(data[off:off+column_name_length], encoding='utf-8')
            off += column_name_length
            (column_default_en,) = struct.unpack(f'{BYTE_ORDER}b', data[off:off+1])
            off += 1
            column_default = np.frombuffer(data[off:off+column_size], dtype=np.uint8)
            off += column_size
            columns.append({'column_type': column_type, 'column_size': column_size, 'column_name_length': column_name_length,
                'column_name': column_name, 'column_default_en': column_default_en, 'column_default': column_default})
        (primary_key_size,) = struct.unpack(f'{BYTE_ORDER}i', data[off:off+4])
        off += 4
        primary_keys = []
        if primary_key_size > 0:
            primary_keys = list(struct.unpack(f'{BYTE_ORDER}{primary_key_size}i', data[off:off+primary_key_size*4]))
        off += primary_key_size * 4
        (foreign_key_number,) = struct.unpack(f'{BYTE_ORDER}i', data[off:off+4])
        off += 4
        foreign_keys = []
        for _ in range(foreign_key_number):
            (foreign_key_name_length,) = struct.unpack(f'{BYTE_ORDER}i', data[off:off+4]); off += 4
            (foreign_key_name,) = struct.unpack(f'{BYTE_ORDER}{foreign_key_name_length}s',
                data[off:off+foreign_key_name_length]); off += foreign_key_name_length
            (target_table_name_length,) = struct.unpack(f'{BYTE_ORDER}i', data[off:off+4]); off += 4
            (target_table_name,) = struct.unpack(f'{BYTE_ORDER}{target_table_name_length}s',
                data[off:off+target_table_name_length]); off += target_table_name_length
            (foreign_key_size,) = struct.unpack(f'{BYTE_ORDER}i', data[off:off+4]); off += 4
            pairs = list(struct.unpack(f'{BYTE_ORDER}{2*foreign_key_size}i',
                data[off:off+8*foreign_key_size])); off += 8*foreign_key_size
            foreign_key_pairs = [(pairs[i], pairs[i+1]) for i in range(0, foreign_key_size*2, 2)]
            foreign_keys.append({'foreign_key_name_length': foreign_key_name_length, 'foreign_key_name': foreign_key_name,
                'target_table_name_length': target_table_name_length, 'target_table_name': target_table_name,
                'foreign_key_size': foreign_key_size, 'foreign_key_pairs': foreign_key_pairs})    
        self.meta = {'record_size': record_size, 'record_per_page': record_per_page, 'bitmap_size': bitmap_size,
            'meta_page_number': meta_page_number, 'page_number': page_number, 'record_number': record_number,
            'next_free_page': next_free_page, 'column_number': column_number, 'columns': columns,
            'primary_key_size': primary_key_size, 'primary_keys': primary_keys,
            'foreign_key_number': foreign_key_number, 'foreign_keys': foreign_keys}
        self.meta_modified = False
        return self.meta
    

    def sync_meta(self) -> None:
        ''' Sync self.meta to the .meta file. Note that read or write records may change
            the file meta, and we don't want to wtite meta to disk each time when reading or
            writing the records. We only need to modify self.meta each time, and use this
            interface to sync meta info just once. Only page_number, record_number, and
            next_free_page are changable, so we only need to sync the first page to .meta file.
        '''
        if not self.is_opened:
            raise FileNotOpenedError(f'File {self.file_name} not opened.')
        if not self.meta_modified: return
        BYTE_ORDER = cf.BYTE_ORDER
        meta, off = self.meta, 0
        meta_pages = np.zeros((cf.PAGE_SIZE * meta['meta_page_number'],), dtype=np.uint8)
        data = struct.pack(f'{BYTE_ORDER}iiiiiiii', meta['record_size'], meta['record_per_page'],
            meta['bitmap_size'], meta['meta_page_number'], meta['page_number'], meta['record_number'],
            meta['next_free_page'], meta['column_number'])
        meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8)
        off += len(data)
        for col in meta['columns']:
            data = struct.pack(f'{BYTE_ORDER}iii', col['column_type'],
                col['column_size'], col['column_name_length'])
            meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
            data = bytes(col['column_name'], encoding='utf-8')[:col['column_name_length']]
            meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
            data = (struct.pack(f'{BYTE_ORDER}b', col['column_default_en'])
                + col['column_default'][:col['column_size']].tobytes())
            meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
        primary_key_size = meta['primary_key_size']
        foreign_key_number = meta['foreign_key_number']
        data = struct.pack(f'{BYTE_ORDER}i{primary_key_size}ii', primary_key_size,
            *(meta['primary_keys'][:primary_key_size]), foreign_key_number)
        meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
        for fk in meta['foreign_keys']:
            len1 = fk['foreign_key_name_length']
            len2 = fk['target_table_name_length']
            len3 = fk['foreign_key_size']
            pairs = list(np.array(fk['foreign_key_pairs'][:len3]).reshape(-1))
            data = struct.pack(f'{BYTE_ORDER}i{len1}si{len2}si{2*len3}i', len1,
                bytes(fk['foreign_key_name'], encoding='utf-8')[:len1], len2,
                bytes(fk['target_table_name'], encoding='utf-8')[:len2], len3, *pairs)
            meta_pages[off:off+len(data)] = np.frombuffer(data, dtype=np.uint8); off += len(data)
        meta_pages = meta_pages.reshape((meta['meta_page_number'], cf.PAGE_SIZE))
        pf_manager.write_page(self.meta_file_id, 0, meta_pages[0,:])
        self.meta_modified = False
        
    
    def get_first_free(self) -> Union[RM_Rid, None]:
        ''' Get the first free rid in pages. If all pages are full, return None.
        '''
        meta = self.meta
        next_free_page = meta['next_free_page']
        if next_free_page == cf.INVALID: return None
        page = pf_manager.read_page(self.data_file_id, next_free_page)
        header_size = RM_PageHeader.size()
        bitmap = Bitmap.deserialize(capacity=meta['record_per_page'],
            data=page[header_size:header_size+meta['bitmap_size']])
        slot_no = bitmap.first_free()
        return RM_Rid(page_no=next_free_page, slot_no=slot_no)
                
    
    def get_record(self, rid:RM_Rid) -> RM_Record:
        ''' Get a record by its rid.
        '''
        if not self.is_opened:
            raise FileNotOpenedError(f'File {self.file_name} not opened.')
        meta = self.meta
        record_size = meta['record_size']
        off = RM_PageHeader.size() + meta['bitmap_size'] + rid.slot_no*record_size
        page = pf_manager.read_page(self.data_file_id, rid.page_no)
        record_data = page[off:off+record_size]
        return RM_Record(rid=rid, data=record_data)
    

    def pack_record(self, fields:List[Union[int, float, str]]) -> np.ndarray:
        ''' Pack the fields to record data by columns info in meta.
        args:
            fields: List[Union[int, float, str]], len >= column_number.
        '''
        meta = self.meta
        column_number = meta['column_number']
        if len(fields) < column_number:
            raise PackRecordError(f'No enough fields to pack, len must >= {column_number}.')
        record_size = meta['record_size']
        columns = meta['columns']
        data = np.zeros(record_size, dtype=np.uint8)
        off = 0
        BYTE_ORDER = cf.BYTE_ORDER
        for col, field in zip(columns[:column_number], fields[:column_number]):
            col_type = col['column_type']
            col_size = col['column_size']
            if col_type == cf.TYPE_INT:
                data[off:off+col_size] = np.frombuffer(struct.pack(f'{BYTE_ORDER}i', field), dtype=np.uint8)
            elif col_type == cf.TYPE_FLOAT:
                data[off:off+col_size] = np.frombuffer(struct.pack(f'{BYTE_ORDER}d', field), dtype=np.uint8)
            elif col_type == cf.TYPE_STR:
                s = bytes(field, encoding='utf-8')[:col_size]
                data[off:off+len(s)] = np.frombuffer(s, dtype=np.uint8)
            else: raise PackRecordError(f'Type {col_type} is invalid.')
            off += col_size
        return data
    
    
    def unpack_record(self, data:np.ndarray) -> List[Union[int, float, str]]:
        ''' Unpack the record data into fileds by columns info in meta.
        args:
            data: np.ndarray[>=record_size, uint8], the record data.
        '''
        meta = self.meta
        if len(data) < meta['record_size']:
            raise UnpackRecordError(f'No enough data to unpack, len must >= {meta["record_size"]}.')
        column_number = meta['column_number']
        columns = meta['columns']
        fields, off = [], 0
        BYTE_ORDER = cf.BYTE_ORDER
        for col in columns[:column_number]:
            col_type = col['column_type']
            col_size = col['column_size']
            if col_type == cf.TYPE_INT:
                fields.append(struct.unpack(f'{BYTE_ORDER}i', data[off:off+col_size].tobytes())[0])
            elif col_type == cf.TYPE_FLOAT:
                fields.append(struct.unpack(f'{BYTE_ORDER}d', data[off:off+col_size].tobytes())[0])
            elif col_type == cf.TYPE_STR:
                (s,) = struct.unpack(f'{BYTE_ORDER}{col_size}s', data[off:off+col_size].tobytes())
                fields.append(str(s, encoding='utf-8').strip('\0'))
            else: raise UnpackRecordError(f'Type {col_type} is invalid.')
            off += col_size
        return fields
        
    
    def insert_record(self, data:np.ndarray) -> RM_Rid:
        ''' Insert a record, return the allocated rid.
        args:
            data: np.ndarray[(>=record_size,), uint8], will interpret data[:record_size] as a record.
        return:
            RM_Rid, the inserted position.
        '''
        if not self.is_opened:
            raise FileNotOpenedError(f'File {self.file_name} not opened.')
        meta = self.meta
        header_size = RM_PageHeader.size()
        record_size = meta['record_size']
        bitmap_size = meta['bitmap_size']
        first_free_page = meta['next_free_page']
        record_per_page = meta['record_per_page']
        data = data[:record_size]
        if first_free_page == cf.INVALID:
            header = RM_PageHeader(1, cf.INVALID)
            bitmap = Bitmap(capacity=record_per_page)
            bitmap.set_bit(0, True)
            page_data = np.zeros(cf.PAGE_SIZE, dtype=np.uint8)
            data_size = header_size + bitmap_size + record_size
            page_data[:data_size] = np.concatenate([header.serialize(), bitmap.serialize(), data])
            page_no = pf_manager.append_page(self.data_file_id, page_data)
            meta['page_number'] += 1
            meta['record_number'] += 1
            meta['next_free_page'] = page_no
            self.meta = meta
            self.meta_modified = True
            return RM_Rid(page_no=page_no, slot_no=0)
        page_data = pf_manager.read_page(self.data_file_id, first_free_page)
        header:RM_PageHeader = RM_PageHeader.deserialize(page_data[:header_size])
        header.record_cnt += 1
        bitmap:Bitmap = Bitmap.deserialize(page_data[header_size:header_size+bitmap_size])
        slot_no = bitmap.first_free()
        if slot_no == cf.INVALID:
            raise InsertRecordError(f'Can not find free slot on page {first_free_page} to insert.')
        bitmap.set_bit(slot_no, True)
        off = header_size + bitmap_size + slot_no * record_size
        page_data[:header_size] = header.serialize()
        page_data[header_size:header_size+bitmap_size] = bitmap.serialize()
        page_data[off:off+record_size] = data
        pf_manager.write_page(self.data_file_id, first_free_page, page_data)
        # modify meta
        meta['record_number'] += 1
        if header.record_cnt >= record_per_page:
            meta['next_free_page'] = header.next_free
        self.meta = meta
        self.meta_modified = True
        return RM_Rid(page_no=first_free_page, slot_no=slot_no)

    
    def remove_record(self, rid:RM_Rid) -> None:
        ''' Remove a record by its rid.
        '''
        if not self.is_opened:
            raise FileNotOpenedError(f'File {self.file_name} not opened.')
        meta = self.meta
        header_size = RM_PageHeader.size()
        bitmap_size = meta['bitmap_size']
        first_free_page = meta['next_free_page']
        record_per_page = meta['record_per_page']
        page_data = pf_manager.read_page(self.data_file_id, rid.page_no)
        header:RM_PageHeader = RM_PageHeader.deserialize(page_data[:header_size])
        bitmap:Bitmap = Bitmap.deserialize(page_data[header_size:header_size+bitmap_size])
        if not bitmap.get_bit(rid.slot_no):
            raise RemoveRecordError(f'Record {rid} does not exist.')
        header.record_cnt -= 1
        if header.record_cnt == record_per_page - 1:
            header.next_free = first_free_page
            meta['next_free_page'] = rid.page_no
        bitmap.set_bit(rid.slot_no, False)
        page_data[:header_size] = header.serialize()
        page_data[header_size:header_size+bitmap_size] = bitmap.serialize()
        pf_manager.write_page(self.data_file_id, rid.page_no, page_data)
        meta['record_number'] -= 1
        self.meta = meta
        self.meta_modified = True
        
    
    def update_record(self, rid:RM_Rid, data:np.ndarray) -> None:
        ''' Update a record with new data on a specific rid.
        args:
            rid: RM_Rid, the record identifier to be updated.
            data: np.ndarray[(>=record_size,), uint8], will interpret data[:record_size] as a record.
        '''
        if not self.is_opened:
            raise FileNotOpenedError(f'File {self.file_name} not opened.')
        meta = self.meta
        header_size = RM_PageHeader.size()
        record_size = meta['record_size']
        bitmap_size = meta['bitmap_size']
        data = data[:record_size]
        page_data = pf_manager.read_page(self.data_file_id, rid.page_no)
        bitmap:Bitmap = Bitmap.deserialize(page_data[header_size:header_size+bitmap_size])
        if not bitmap.get_bit(rid.slot_no):
            raise UpdateRecordError(f'Record {rid} does not exist.')
        off = header_size + bitmap_size + rid.slot_no * record_size
        page_data[off:off+record_size] = data
        pf_manager.write_page(self.data_file_id, rid.page_no, page_data)


if __name__ == '__main__':
    pass
