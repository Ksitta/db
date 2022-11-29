from record_management.rm_manager import rm_manager
from record_management.rm_file_handle import RM_FileHandle
from record_management.rm_file_scan import RM_FileScan
from record_management.rm_rid import RM_Rid
import numpy as np
from typing import List, Union, Dict
from config import *
from common.common import *
import struct


class Table():
    def get_column_idx(self, col_name: str) -> int:
        for i in range(len(self._columns)):
            each = self._columns[i]
            if each.name == col_name:
                return i

    def describe(self):
        result = list()
        for each in self._columns:
            if each.type == TYPE_INT:
                tp = 'INT'
            elif each.type == TYPE_FLOAT:
                tp = 'FLOAT'
            elif each.type == TYPE_STR:
                tp = 'STRING'
            else:
                raise Exception("Unknown type")
            result.append([each.name, tp, each.size])
        res = Result(["name", "type","size"], result)
        return res

    def get_name(self) -> str:
        return self._name

    def __init__(self, name: str, columns: list = None, pk: List[int] = None, fk: List[Dict] = None) -> None:
        self._name: str = name
        if columns == None:
            self._file_handle: RM_FileHandle = rm_manager.open_file(name)
            meta: dict = self._file_handle.read_meta()
            columns: List[Column] = list()
            for each in meta['columns']:
                columns.append(Column(each['column_name'], each['column_type'], each['column_size'], False))
            
            # metas
            self._columns = columns
            self._pk: List[int]  = meta['primary_keys']
            self._fk: List[Dict] = meta['foreign_keys']

        else:
            rm_manager.create_file(name)

            # metas
            self._file_handle: RM_FileHandle = rm_manager.open_file(name)
            self._columns: List[Column] = columns
            self._pk: List[int] = pk
            self._fk: List[Dict] = fk

            meta: dict = dict()
            meta['column_number'] = len(columns)

            record_size: int = 0
            meta_columns: list = list()

            for i in columns:
                each: Column = i
                column: dict = dict()
                column['column_type'] = each.type
                column['column_size'] = each.size
                column['column_name_length'] = len(each.name)
                column['column_name'] = each.name
                column['column_default_en'] = each.default_val != None
                column['column_default'] = np.zeros(each.size, dtype=np.uint8)
                if(each.default_val != None):
                    if(each.type == TYPE_INT):
                        column['column_default'][0:each.size] = np.frombuffer(struct.pack(f'{BYTE_ORDER}i', each.default_val), dtype=np.uint8)
                    if(each.type == TYPE_FLOAT):
                        column['column_default'][0:each.size] = np.frombuffer(struct.pack(f'{BYTE_ORDER}d', each.default_val), dtype=np.uint8)
                    if(each.type == TYPE_STR):
                        s = bytes(each.default_val, encoding='utf-8')[:each.size]
                        column['column_default'][0:each.size] = np.frombuffer(s, dtype=np.uint8)
                meta_columns.append(column)
                record_size += each.size
            
            meta['record_size'] = record_size
            meta['column_number'] = len(meta_columns)
            meta['columns'] = meta_columns

            meta['primary_key_size'] = len(pk)
            meta['primary_keys'] = pk
            meta['foreign_key_number'] = len(fk)
            meta['foreign_keys'] = fk
            self._file_handle.init_meta(meta)

    def __del__(self) -> None:
        self._file_handle.sync_meta()
        rm_manager.close_file(self._name)

    def _check_fields(self, fields: List[Union[int, float, str, None]]):
        if(len(fields) != len(self._columns)):
            raise Exception("Field number not match")
        for i in range(len(fields)):
            if (type(fields[i]) is int):
                if(self._columns[i].type != TYPE_INT):
                    raise Exception("Field type not match expected int but got {}".format())
            if (type(fields[i]) is float):
                if(self._columns[i].type != TYPE_FLOAT):
                    raise Exception("Field type not match")
            if (type(fields[i]) is str):
                if(self._columns[i].type != TYPE_STR):
                    raise Exception("Field type not match")
            if (fields[i] is None):
                if(self._columns[i].nullable == False):
                    raise Exception("Field type not match")
                
    def insert_record(self, fields: List[Union[int, float, str, None]]):
        self._check_fields(fields)
        data: np.ndarray = self._file_handle.pack_record(fields)
        self._file_handle.insert_record(data)

    def delete_record(self, rid: RM_Rid):
        self._file_handle.remove_record(rid)

    def update_record(self, rid: RM_Rid, record: Record):
        data = self._file_handle.pack_record(record.data)
        self._file_handle.update_record(rid, data)

    def load_all_records(self) -> List[List[Union[int, float, str]]]:
        scaner = RM_FileScan()
        scaner.open_scan(self._file_handle)
        records: list = list()
        raw_record = scaner.next()
        for each in raw_record:
            rec = self._file_handle.unpack_record(each.data)
            records.append(Record(rec, each.rid))
        scaner.close_scan()
        return records

    def get_column_names(self) -> List[str]:
        return [i.name for i in self._columns]

    def get_columns(self) -> List[Column]:
        return self._columns