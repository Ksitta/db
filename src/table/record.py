from type.type import TypeEnum
import numpy as np

class Column():
    def __init__(self, name: str, type: TypeEnum, size: int, default_val=None) -> None:
        self.name: str = name
        self.type: TypeEnum = type
        self.size: int = size
        self.default_val = default_val
    
    def __init__(self, column: dict) -> None:
        self.name: str = column['column_name']
        self.type: TypeEnum = column['column_type']
        self.size: int = column['column_size']
        default_en = column['column_default_en']
        if default_en:
            default_data:np.ndarray = column['column_default']
            if self.type == TypeEnum.INT:
                self.default_val = int.from_bytes(default_data, byteorder='little')
            elif self.type == TypeEnum.FLOAT:
                self.default_val = float.from_bytes(default_data, byteorder='little')
            elif self.type == TypeEnum.VARCHAR:
                self.default_val = default_data.tobytes().decode('utf-8')
        else:
            self.default_val = None

class Record():
    def __init__(self, data:np.ndarray, columns: list) -> None:
        pos = 0
        self._data: list = list()
        for each in columns:
            column: Column = each
            if column._type == TypeEnum.INT:
                res: int = int.from_bytes(data[pos:pos+column._size], 'little')
            if column._type == TypeEnum.FLOAT:
                res: float = float.from_bytes(data[pos:pos+column._size], 'little')
            if column._type == TypeEnum.VARCHAR:
                res: str = data[pos:pos+column._size].tobytes().decode('utf-8')    
            self._data.append(res)
            pos += column._size
    
    def to_nparray(self, columns: list) -> np.ndarray:
        data = np.ndarray((0,), dtype=np.uint8)
        for each in columns:
            column: Column = each
            if column._type == TypeEnum.INT:
                data = np.append(data, self._data[column._name].to_bytes(column._size, 'little'))
            if column._type == TypeEnum.FLOAT:
                data = np.append(data, self._data[column._name].to_bytes(column._size, 'little'))
            if column._type == TypeEnum.VARCHAR:
                data = np.append(data, self._data[column._name][:column.size:].encode('utf-8').ljust(column._size, b'\x00'))
        return data
    
        
    
    