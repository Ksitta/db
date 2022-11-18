from record_management.rm_record_manager import rm_manager
from record_management.rm_file_handle import RM_FileHandle
from table.record import Record, Column
import numpy as np
class Table():
    def __init__(self, name: str, columns: list, pk: list, fk: dict) -> None:
        self._name: str = name
        if columns == None:
            self._file_handle: RM_FileHandle = rm_manager.open_file(name)
            meta: dict = self._file_handle.read_meta()
            self._columns: list = list()
            for each in meta['columns']:
                column = Column(each)
                self._columns.append(column)
        else:
            rm_manager.create_file(name)
            self._file_handle: RM_FileHandle = rm_manager.open_file(name)
            self._columns = columns
            
            meta: dict = dict()
            meta['column_number'] = len(columns)
            
            record_size: int = 0
            columns: list = list()
            
            for i in columns:
                each: Column = i
                column: dict = dict()
                column['column_type'] = each.type
                column['column_size'] = each.size
                column['column_name_length'] = len(each.name)
                column['column_name'] = each.name
                columns.append(column)
                record_size += len(each.name)
            meta['columns'] = columns
            meta['record_size'] = record_size
            meta['primary_key_size'] = len(pk)
            meta['primary_keys'] = []
            # meta['foreign_key_number'] = len(fk)
            
            self._file_handle.init_meta(meta)
   

    def __del__(self):
        if self._file_handle:
            self._file_handle.sync_meta()
        
    def drop(self):
        rm_manager.remove_file(self._name)
        self._file_handle = None

    def insert_record(self, record: Record):
        data: np.ndarray = record.to_nparray(self._columns)
        self._file_handle.insert_record(data)
    
    def delete_record(self, rid):
        self._file_handle.remove_record(rid)
    
    def update_record(self, rid, record):
        data: np.ndarray = record.to_nparray(self._columns)
        self._file_handle.update_record(rid, data)
        
    