from config import *
import numpy as np
from typing import List, Union
from record_management.rm_rid import RM_Rid
from common.common import *

class Column():
    def __init__(self, name: str, type: int, size: int, nullable: bool, default_val=None) -> None:
        self.name: str = name
        self.type: int = type
        self.size: int = size
        self.nullable: bool = nullable
        self.default_val = default_val


class Record():

    def __init__(self, columns: List[Union[int, float, str]] = list(), rid: RM_Rid = None) -> None:
        self.data: List[Union[int, float, str]] = columns
        self.rid: RM_Rid = rid

    def get_field(self, cnt: int) -> Union[int, float, str]:
        return self.data[cnt]

    def concat(rec1: 'Record', rec2: 'Record') -> 'Record':
        return Record(rec1.data + rec2.data, rec1.rid)

    def __radd__(self, other: 'Record') -> 'Record':
        return self.concat(other)

    def append(self, column: Union[int, float, str]):
        self.data.append(column)


class RecordList():
    def __init__(self, columns: List[Col] = list(), records: List[Record] = list()) -> None:
        self.records: List[Record] = records
        self.columns: List[Col] = columns

    def append(self, record: Record):
        self.records.append(record)

    def set_columns(self, columns: List[Col]):
        self.columns = columns

    def get_column_idx(self, col: Col) -> int:
        for i in range(len(self.columns)):
            each = self.columns[i]
            if each.col_name == col.col_name and each.table_name == col.table_name:
                return i