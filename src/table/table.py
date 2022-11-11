from paged_file.pf_file_manager import pf_manager
from type.type import BaseType, FloatType, IntType, VarcharType


class Column():
    _name : str
    _type : BaseType
    _length : int
    
    def __init__(self, name : str, type : BaseType):
        self._name = name
        self._type = type
        self._length = type.get_length()


class Table():    
    _name : str
    _meta_fd : int
    _data_fd : int
    _columns : list
    _record_len : int
    _record_cnt : int
    
    def __init__(self, name : str, meta_fd : int, data_fd : int) -> None:
        self._name = name
        self._mata_fd = meta_fd
        self._data_fd = data_fd
        self._load()
        

    def _load(self) -> int:
        pos = 0
        metas : bytes = pf_manager.read_page(self._meta_fd, 0)
        self._record_len = int.from_bytes(metas[pos:pos + 4], byteorder='little')
        pos += 4
        self._record_cnt = int.from_bytes(metas[pos:pos + 4], byteorder='little')
        pos += 4
        col_cnt = int.from_bytes(metas[pos:pos + 4], byteorder='little')
        pos += 4
        for i in range(col_cnt):
            typenum = int.from_bytes(metas[pos:pos + 4], byteorder='little')
            pos += 4
            type_len = int.from_bytes(metas[pos:pos + 4], byteorder='little')
            pos += 4
            name_len = int.from_bytes(metas[pos:pos + 4], byteorder='little')
            pos += 4
            name = metas[pos:pos + name_len].decode('utf-8')
            pos += name_len
            if(typenum == 0):
                type = IntType()
            if(typenum == 1):
                type = VarcharType(type_len)
            if(typenum == 2):
                type = FloatType()
            self._columns.append(Column(name, type))
        return pos
        
        
    def _store(self):
        metas = bytes()
        metas += self._record_len.to_bytes(4, byteorder='little')
        metas += self._record_cnt.to_bytes(4, byteorder='little')
        metas += len(self._columns).to_bytes(4, byteorder='little')
        for col in self._columns:
            metas += col._type.get_type().value.to_bytes(4, byteorder='little')
            metas += col._type.get_length().to_bytes(4, byteorder='little')
            metas += len(col._name).to_bytes(4, byteorder='little')
            metas += col._name.encode('utf-8')
        pf_manager.write_page(self._meta_fd, 0, metas)
        return len(metas)