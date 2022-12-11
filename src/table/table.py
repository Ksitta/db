from record_management.rm_manager import rm_manager
from record_management.rm_file_handle import RM_FileHandle
from record_management.rm_file_scan import RM_FileScan
from record_management.rm_rid import RM_Rid
import numpy as np
from typing import List, Union, Dict, Set, Tuple
from config import *
from common.common import *
import struct
from index_management.ix_manager import ix_manager
from index_management.ix_index_scan import IX_IndexScan
from operators.conditions import Condition, AlgebraCondition
from utils.bitwise import *


class Table():
    def check_foreign_key(self, val_list: np.ndarray):
        index_no = list_int_to_int(self._fk)
        handle = self._index_handles[index_no]
        scan = IX_IndexScan()
        for val in val_list:
            scan.open_scan(handle, CompOp.EQ, val)
            rid_gen = scan.next()
            exist = False
            for each in rid_gen:
                exist = True
                break
            if not exist:
                raise Exception("Foreign key not found")
            scan.close_scan()

    def modify_ref_cnt(self, val_list: np.ndarray, delta: int):
        if(len(self._pk) == 0):
            return
        index_no = list_int_to_int(self._pk)
        handle = self._index_handles[index_no]
        for each in val_list:
            handle.modify_verbose(each, delta)

    def get_ref_cnt(self, fields: np.ndarray):
        if(len(self._pk) == 0):
            return 0
        vals = fields[self._pk]
        index_no = list_int_to_int(self._pk)
        cnts = self._index_handles[index_no].modify_verbose(vals, 0)
        return cnts[0]

    def index_exist(self, column_idx: int) -> bool:
        return column_idx in self._index_handles

    def sync_fk_pk(self):
        info = {}
        info["primary_key_size"] = len(self._pk)
        info["primary_keys"] = self._pk
        info["foreign_key_number"] = len(self._fk)
        info["foreign_keys"] = self._fk
        self._file_handle.set_primary_foreign_key(info)

    def add_pk(self, pk: List[int]):
        if (len(self._pk) != 0):
            raise Exception("Primary key already exists")
        records = self.load_all_records()
        cols = []
        for col_idx in pk:
            cols.append([each.data[col_idx] for each in records])
        objects = list(zip(*cols))
        if(len(set(objects)) != len(objects)):
            raise Exception("Primary key conflict")

        self._pk = pk
        self.sync_fk_pk()

    def add_fk(self, fk: Dict):
        for each in self._fk:
            if each['foreign_key_name'] == fk['foreign_key_name']:
                raise Exception("Foreign key already exists")
        self._fk.append(fk)
        self.sync_fk_pk()

    def drop_pk(self):
        if (len(self._pk) == 0):
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

    def find_exist(self, idxes: List[int], value_idxes: List[int], values: np.ndarray) -> Set[RM_Rid]:
        scaner = IX_IndexScan()
        idx_no = list_int_to_int(idxes)
        handle = self._index_handles[idx_no]
        val = values[:,value_idxes].tolist()
        res = set()
        for each in val:
            scaner.open_scan(handle, CompOp.EQ, each)
            res_gen = scaner.next()
            for rid in res_gen:
                res.add(rid)
            scaner.close_scan()
        return res

    def check_primary_key(self, value: np.ndarray):
        if (len(self._pk) == 0):
            return
        pks = value[:,self._pk].tolist()
        pks = set([tuple(each) for each in pks])
        if (len(pks) != len(value)):
            raise Exception("Primary key conflict")

        result = self.find_exist(self._pk, self._pk, value)

        if (len(result) != 0):
            raise Exception("Primary key conflict")

    def describe(self):
        result = list()
        for i in range(len(self._columns)):
            each = self._columns[i]
            if each.type == TYPE_INT:
                tp = 'INT'
            elif each.type == TYPE_FLOAT:
                tp = 'FLOAT'
            elif each.type == TYPE_STR:
                tp = 'STRING'
            else:
                raise Exception("Unknown type")
            key_type = ""
            if i in self._pk:
                key_type = "PRI"
            result.append([each.name, tp, each.size, key_type])
        res = Result(["Name", "Type", "Size", "Key"], result)
        return res

    def get_name(self) -> str:
        return self._name

    @staticmethod
    def create_table(name: str, columns: List[Column], pk: List[int], fk: List[Dict]):
        rm_manager.create_file(name)

        file_handle: RM_FileHandle = rm_manager.open_file(name)

        meta: dict = dict()
        meta['column_number'] = len(columns) + 1

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
            if (each.default_val != None):
                if (each.type == TYPE_INT):
                    column['column_default'][0:each.size] = np.frombuffer(
                        struct.pack(f'{BYTE_ORDER}i', each.default_val), dtype=np.uint8)
                if (each.type == TYPE_FLOAT):
                    column['column_default'][0:each.size] = np.frombuffer(
                        struct.pack(f'{BYTE_ORDER}d', each.default_val), dtype=np.uint8)
                if (each.type == TYPE_STR):
                    s = bytes(each.default_val, encoding='utf-8')[:each.size]
                    column['column_default'][0:each.size] = np.frombuffer(
                        s, dtype=np.uint8)
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

        # create index for primary key
        index_no = list_int_to_int(pk)
        if index_no != 0:
            ix_manager.create_index(name, index_no)
            idx_handle = ix_manager.open_index(name, index_no)
            idx_meta = dict()
            idx_meta['field_number'] = len(pk)
            idx_meta['fields'] = [
                (columns[each].type, columns[each].size) for each in pk]
            idx_handle.init_meta(idx_meta)
            idx_handle.sync_meta()
            ix_manager.close_index(name, index_no)

    def create_index(self, column_idx: List[int], records: List[Record]):
        index_no = list_int_to_int(column_idx)
        if (index_no in self._index_handles):
            return
        ix_manager.create_index(self._name, index_no)
        idx_handle = ix_manager.open_index(self._name, index_no)
        idx_meta = dict()
        idx_meta['field_number'] = len(column_idx)
        idx_meta['fields'] = [
            (self._columns[each].type, self._columns[each].size) for each in column_idx]
        idx_handle.init_meta(idx_meta)
        idx_handle.sync_meta()
        self._index_handles[index_no] = idx_handle
        for each in records:
            val = [each.data[i] for i in column_idx]
            idx_handle.insert_entry(val, each.rid)

    def drop_index(self, column_idx: int):
        if (column_idx not in self._index_handles):
            raise Exception("Index not exists")
        ix_manager.close_index(self._name, column_idx)
        del self._index_handles[column_idx]
        ix_manager.remove_index(self._name, column_idx)

    def __init__(self, name: str) -> None:
        self._name: str = name
        self._index_handles = {}
        self._file_handle: RM_FileHandle = rm_manager.open_file(name)
        meta: dict = self._file_handle.read_meta()
        columns: List[Column] = list()
        for each in meta['columns']:
            columns.append(
                Column(each['column_name'], each['column_type'], each['column_size'], False))

        # metas
        self._columns = columns
        self._pk: List[int] = meta['primary_keys']
        self._fk: List[Dict] = meta['foreign_keys']
        indexs: List[int] = ix_manager.query_index(".", name)
        self._index_handles = {
            i: ix_manager.open_index(name, i) for i in indexs}
        for each in self._index_handles.values():
            each.read_meta()

    def __del__(self) -> None:
        for each in self._index_handles:
            ix_manager.close_index(self._name, each)
        self._file_handle.sync_meta()
        rm_manager.close_file(self._name)

    def check_fields(self, fields: np.ndarray):
        length = len(self._columns)
        if (len(fields.shape) == 1 or fields.shape[1] != length):
            raise Exception("Field number not match")
        types = []
        for each in self._columns:
            if each.type == TYPE_INT:
                types.append(int)
            if each.type == TYPE_FLOAT:
                types.append(float)
            if each.type == TYPE_STR:
                types.append(str)
        for i in range(length):
            for each in fields[:, i]:
                if (type(each) != types[i]):
                    raise Exception("Field type not match")
        

    def insert_records(self, fields_list: np.ndarray):
        rids = []
        for each in fields_list:
            data: np.ndarray = self._file_handle.pack_record(each)
            rid = self._file_handle.insert_record(data)
            rids.append(rid)
        for index_no in self._index_handles:
            column_idx = int_to_list_int(index_no)
            handle = self._index_handles[index_no]
            vals = fields_list[:, column_idx]
            for i in range(len(fields_list)):
                handle.insert_entry(vals[i], rids[i])

    def delete_record(self, record: Record):
        self._file_handle.remove_record(record.rid)
        for each in self._index_handles:
            self._index_handles[each].remove_entry(
                record.data[each], record.rid)

    def update_record(self, rid: RM_Rid, record: Record):
        data = self._file_handle.pack_record(record.data)
        self._file_handle.update_record(rid, data)

    def load_all_records(self) -> List[Record]:
        scaner: RM_FileScan = RM_FileScan()
        scaner.open_scan(self._file_handle)
        records: List = list()
        raw_record = scaner.next()
        for each in raw_record:
            rec = self._file_handle.unpack_record(each.data)
            records.append(Record(rec, each.rid))
        scaner.close_scan()
        return records

    def load_records_with_cond(self, conds: List[AlgebraCondition]) -> List[Record]:
        scanner = IX_IndexScan()
        rid_sets = []
        for each in conds:
            scanner.open_scan(
                self._index_handles[each.get_col_idx()], each._operator, each._value)
            rid_gen = scanner.next()
            rids = set()
            for each in rid_gen:
                rids.add(each)
            rid_sets.append(rids)
            scanner.close_scan()

        res: Set[RM_Rid] = rid_sets[0]
        for each in rid_sets[1:]:
            res = res.intersection(each)

        rm_records = [self._file_handle.get_record(each) for each in res]
        records = [Record(self._file_handle.unpack_record(
            each.data), each.rid) for each in rm_records]
        return records

    def get_column_names(self) -> List[str]:
        return [i.name for i in self._columns]

    def get_columns(self) -> List[Column]:
        return self._columns

    def drop(self):
        raw_keys = [each for each in self._index_handles]
        for each in raw_keys:
            self.drop_index(each)
