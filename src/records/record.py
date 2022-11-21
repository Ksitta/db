from config import *
import numpy as np
from typing import List, Union
from record_management.rm_rid import RM_Rid
class Column():
    def __init__(self, name: str, type: int, size: int, nullable: bool,default_val=None) -> None:
        self.name: str = name
        self.type: int = type
        self.size: int = size
        self.nullable: bool = nullable
        self.default_val = default_val
    
    # def __init__(self, column: dict) -> None:
    #     self.name: str = column['column_name']
    #     self.type: int = column['column_type']
    #     self.size: int = column['column_size']
    #     default_en = column['column_default_en']
    #     if default_en:
    #         default_data:np.ndarray = column['column_default']
    #         if self.type == TypeEnum.INT:
    #             self.default_val = int.from_bytes(default_data, byteorder='little')
    #         elif self.type == TypeEnum.FLOAT:
    #             self.default_val = float.from_bytes(default_data, byteorder='little')
    #         elif self.type == TypeEnum.VARCHAR:
    #             self.default_val = default_data.tobytes().decode('utf-8')
    #     else:
    #         self.default_val = None

class Record():
    
    def __init__(self, columns: List[Union[int, float, str]], rid: RM_Rid) -> None:
        self.data: List[Union[int, float, str]] = columns
        self.rid: RM_Rid = rid
        
    def get_field(self, cnt: int) -> Union[int, float, str]:
        return self.data[cnt]
    
    