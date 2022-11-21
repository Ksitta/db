from record_management.rm_record_manager import rm_manager
from record_management.rm_file_handle import RM_FileHandle
from record_management.rm_file_scan import RM_FileScan
from record_management.rm_rid import RM_Rid
from records.record import Record, Column
import numpy as np
from typing import List, Union


class Table():
    def describe(self):
        result: List[List[str, list]]
        for each in self._columns:
            result.append([each.name, each.type, each.size])

    def __init__(self, name: str, columns: list = None, pk: list = None, fk: dict = None) -> None:
        self._name: str = name
        if columns == None:
            self._file_handle: RM_FileHandle = rm_manager.open_file(name)
            meta: dict = self._file_handle.read_meta()
            self._columns: List[Column] = list()
            for each in meta['columns']:
                pass  # TODO : read columns from meta
        else:
            rm_manager.create_file(name)
            self._file_handle: RM_FileHandle = rm_manager.open_file(name)
            self._columns: List[Column] = columns

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
            meta['foreign_key_number'] = 0  # TODO
            meta['foreign_keys'] = []
            self._file_handle.init_meta(meta)

    def __del__(self) -> None:
        self._file_handle.sync_meta()
        rm_manager.close_file(self._name)

    def insert_record(self, record: Record):
        data: np.ndarray = self._file_handle.pack_record(
            record.to_nparray(record._data))
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
        while True:
            raw_record = scaner.next()
            if raw_record == None:
                break
            rec = self._file_handle.unpack_record(raw_record.data)
            records.append(Record(rec, raw_record.rid))
        return records

    def get_column_names(self) -> List[str]:
        return [i.name for i in self._columns]
