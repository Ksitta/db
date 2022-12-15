from enum import Enum
from config import *
import numpy as np
from typing import List, Union
from record_management.rm_rid import RM_Rid
from utils.enums import *

class Col:
    def __init__(self, col_name: str, table_name: str = None, aggregator: Aggregator = None):
        self.col_name = col_name
        self.table_name = table_name
        self.aggregator = aggregator
        

class Result():
    def __init__(self, header: list, results: list, addition: list) -> None:
        self.header = header
        self.results = results
        self.addition = addition

    def set_addition(self, addition: list):
        self.addition = addition

class Column():
    def __init__(self, name: str, type: int, size: int, nullable: bool, default_val=None) -> None:
        self.name: str = name
        self.type: int = type
        self.size: int = size
        self.nullable: bool = nullable
        self.default_val = default_val


class Record():

    def __init__(self, columns: np.ndarray, rid: RM_Rid = None) -> None:
        self.data: np.ndarray = columns
        self.rid: RM_Rid = rid

    def get_field(self, cnt: int) -> Union[int, float, str]:
        return self.data[cnt]

    def concat(rec1: 'Record', rec2: 'Record') -> 'Record':
        return Record(np.append(rec1.data, rec2.data), rec1.rid)

    def __radd__(self, other: 'Record') -> 'Record':
        return self.concat(other)

    def append(self, column: Union[int, float, str]):
        self.data.append(column)


class RecordList():
    def __init__(self, columns: List[Col], records: List[Record]) -> None:
        self.columns: List[Col] = columns
        self.records: List[Record] = records

    def append(self, record: Record):
        self.records.append(record)

    def set_columns(self, columns: List[Col]):
        self.columns = columns

    def get_column_idx(self, col: Col) -> int:
        if(col.table_name is None and col.col_name == "*"):
            return 0
        for i in range(len(self.columns)):
            each = self.columns[i]
            if each.col_name == col.col_name:
                if col.table_name:
                    if each.table_name == col.table_name:
                        return i
                else:
                    return i
        raise Exception("Column not found")