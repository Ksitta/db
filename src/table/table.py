from record_management.rm_manager import rm_manager
from record_management.rm_file_handle import RM_FileHandle
from record_management.rm_file_scan import RM_FileScan
from record_management.rm_rid import RM_Rid
import numpy as np
from typing import List, Union, Dict, Set
from config import *
from common.common import *
import struct
from index_management.ix_manager import ix_manager
from index_management.ix_index_scan import IX_IndexScan
from operators.conditions import Condition, AlgebraCondition

class Table():
    def index_exist(self, column_idx: int) -> bool:
        return column_idx in self._index_handles
    
    def sync_fk_pk(self):
        info = {}
        info["primary_key_size"] = len(self._pk)
        info["primary_key"] = self._pk
        info["foreign_key_size"] = len(self._fk)
        info["foreign_key"] = self._fk
        self._file_handle.set_primary_foreign_key()

    def add_pk(self, pk: List[int]):
        if(len(self._pk) != 0):
            raise Exception("Primary key already exists")
        for each in pk:
            self.create_index(each)
        self._pk = pk
        self.sync_fk_pk()

    def add_fk(self, fk: Dict):
        for each in self._fk:
            if each['foreign_key_name'] == fk['foreign_key_name']:
                raise Exception("Foreign key already exists")
        self._fk.append(fk)
        self.sync_fk_pk()

    def drop_pk(self):
        if(len(self._pk) == 0):
            raise Exception("Primary key not exists")
        self._pk = []
        self.sync_fk_pk()

    def drop_fk(self, fk_name: str):
        for each in self._fk:
            if each['foreign_key_name'] == fk_name:
                self._fk.remove(each)
                self.sync_fk_pk()
                return
        raise Exception("Foreign key not found")

    def get_fk(self):
        return self._fk

    def get_column_idx(self, col_name: str) -> int:
        for i in range(len(self._columns)):
            each = self._columns[i]
            if each.name == col_name:
                return i
    
    def check_primary_key(self, value: List[Union[int, float, str]]):
        if (len(self._pk) == 0):
            return
        scaner = IX_IndexScan()
        result: Dict[int, set] = {}
        for i in self._pk:
            val = value[i]
            handle = self._index_handles[i]
            scaner.open_scan(handle, val, val)
            result_i = set()
            while(True):
                res = scaner.next()
                if(res is None):
                    break
                result_i.add(res)
            scaner.close_scan()
            result[i] = result_i
        
        # intersection all result
        for each in result:
            result_i.intersection_update(result[each])

        if(len(result_i) != 0):
            raise Exception("Primary key conflict")
    
    def check_foreign_key(self, idx: List[int], value: List[Union[int, float, str]], is_insert: bool):
        scaner = IX_IndexScan()
        result: Dict[int, set] = {}
        i = 0
        for each in idx:
            val = value[i]
            handle = self._index_handles[each]
            scaner.open_scan(handle, val, val)
            result_each = set()
            while(True):
                res = scaner.next()
                if(res is None):
                    break
                result_each.add(res)
            scaner.close_scan()
            result[each] = result_each
            i = i + 1
        
        # intersection all result
        for each in result:
            result_each.intersection_update(result[each])
        if(is_insert):
            if(len(result_each) == 0):
                raise Exception("Foreign key not found")
        else:
            if(len(result_each) != 0):
                raise Exception("Foreign key exists")

    def check_foreign_key_on_delete(self, target_table: str, values: List[Union[int, float, str]]):
        for each in self._fk:
            if each['target_table'] == target_table:
                idx = [pair[0] for pair in each['foreign_key_pairs']]
                vals = [values[pair[1]] for pair in each['foreign_key_pairs']]
                self.check_foreign_key(idx, vals, False)

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

    @staticmethod
    def create_table(name: str, columns: list, pk: List[int], fk: List[Dict]):
        rm_manager.create_file(name)

        file_handle: RM_FileHandle = rm_manager.open_file(name)
        columns: List[Column] = columns
        pk: List[int] = pk
        fk: List[Dict] = fk

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
        file_handle.init_meta(meta)

        rm_manager.close_file(name)
        for each in pk:
            ix_manager.create_index(name)

    def create_index(self, column_idx: int):
        if(column_idx in self._pk):
            return
        ix_manager.create_index(self._name, column_idx)
        self._index_handles[column_idx] = ix_manager.open_index(self._name, column_idx)

    def drop_index(self, column_idx: int):
        if(self._index_handles[column_idx] == None):
            raise Exception("Index not exists")
        del self._index_handles[column_idx]
        ix_manager.destroy_index(self._name, column_idx)

    def __init__(self, name: str) -> None:
        self._name: str = name
        self._file_handle: RM_FileHandle = rm_manager.open_file(name)
        meta: dict = self._file_handle.read_meta()
        columns: List[Column] = list()
        for each in meta['columns']:
            columns.append(Column(each['column_name'], each['column_type'], each['column_size'], False))
        
        # metas
        self._columns = columns
        self._pk: List[int]  = meta['primary_keys']
        self._fk: List[Dict] = meta['foreign_keys']
        indexs: List[int] = ix_manager.query_index(".", name)
        self._index_handles = {i: ix_manager.open_index(name, i) for i in indexs}

    def __del__(self) -> None:
        for each in self._index_handles:
            ix_manager.close_index(self._name, each)
        self._file_handle.sync_meta()
        rm_manager.close_file(self._name)

    def check_fields(self, fields: List[Union[int, float, str, None]]):
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
    
    def load_records_with_cond(self, conds: List[AlgebraCondition]):
        scanner = IX_IndexScan()
        rid_sets = []
        for each in conds:
            scanner.open_scan(self._index_handles[each.get_col_idx()], each._operator, each._value)
            rid_gen = scanner.next()
            rids = set()
            for each in rid_gen:
                rids.append(each)
            rid_sets.append(rids)
            scanner.close_scan()
        
        res: Set[RM_Rid] = rid_sets[0]
        for each in rid_sets[1:]:
            res = res.intersection(each)
        
        records = [self._file_handle.get_record(each) for each in res]
        return records
        
    def get_column_names(self) -> List[str]:
        return [i.name for i in self._columns]

    def get_columns(self) -> List[Column]:
        return self._columns